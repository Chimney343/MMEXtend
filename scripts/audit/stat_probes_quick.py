"""
Statistical audit probes against the real pipeline outputs (read-only).

Checks the expense-trend labels against noise series (false-positive rates)
and compares them with the labels on your local outputs; prints summaries
only.

Run from the repo root:
    PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe scripts/audit/stat_probes_quick.py
"""
import sys
import warnings

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.stats.multitest import multipletests
from statsmodels.stats.stattools import durbin_watson
from statsmodels.tsa.stattools import acf
from scipy.stats import spearmanr

warnings.filterwarnings("ignore")
sys.path.insert(0, ".")
from src.analysis.expense_trends import month_of_year_kruskal  # noqa: E402

INTERIM = "data/interim/"
P = "data/processed/"
pd.set_option("display.width", 200)

print("=" * 70, "\nA. EXPENSE TRENDS")
et = pd.read_parquet(P + "expense_trends.parquet")
ma = pd.read_parquet(INTERIM + "monthly_aggregates.parquet")
print("months in aggregates:", ma.year_month.min(), "->", ma.year_month.max(), "| n:", ma.year_month.nunique())
cs = et.drop_duplicates("category").copy()
tested = cs[cs.trend_pvalue.notna()].copy()
print("categories:", len(cs), "| tested (>=6 months):", len(tested))
print("trend_class counts:", cs.trend_class.value_counts().to_dict())
m = len(tested)
p = tested.trend_pvalue.values
q = tested["trend_pvalue_adj"].values if "trend_pvalue_adj" in tested.columns else np.array([])
print(f"unadjusted p<0.05: {(p < 0.05).sum()}  | Bonferroni(m={m}): {multipletests(p, 0.05, 'bonferroni')[0].sum()}"
      f"  | BH q=0.05: {multipletests(p, 0.05, 'fdr_bh')[0].sum()}  | BH q=0.10: {multipletests(p, 0.10, 'fdr_bh')[0].sum()}")
if len(q):
    print(f"pipeline q (BH, FDR 0.10): {(q < 0.10).sum()} significant"
          f" | rising {int((cs.trend_class == 'rising').sum())} | falling {int((cs.trend_class == 'falling').sum())}")
print("is_seasonal:", int(cs.is_seasonal.sum()), "/", len(cs),
      "| has_consecutive_rise:", int(cs.has_consecutive_rise.sum()) if "has_consecutive_rise" in cs else "n/a")

# gaps: expense_trends is now fitted on a zero-filled continuous grid
all_months = sorted(et.year_month.unique())
gap_rows = []
for cat, g in et.groupby("category"):
    grid = pd.period_range(g.year_month.min(), g.year_month.max(), freq="M").strftime("%Y-%m")
    gap_rows.append((cat, len(g), len(grid), int((g.total_amount == 0).sum())))
gaps = pd.DataFrame(gap_rows, columns=["category", "n_rows", "n_span", "n_zero_filled"])
print("categories whose fitted grid is discontinuous (should be 0):",
      int((gaps.n_rows != gaps.n_span).sum()), "/", len(gaps))
print("zero-filled rows per category: median",
      int(gaps.n_zero_filled.median()), "| max", int(gaps.n_zero_filled.max()))

# refit: zero-filled grid, complete months only, HAC SEs, autocorrelation diagnostics
last_complete = ma.year_month.max()  # check partial below
print("\nlast month in aggregates:", last_complete, "(pipeline as_of unknown here; see is_partial in cashflow split)")
cf = pd.read_parquet(INTERIM + "monthly_cashflow_split.parquet")
print("cashflow split partial months:", cf.loc[cf.is_partial_month, "year_month"].tolist())
partial = set(cf.loc[cf.is_partial_month, "year_month"])
exp = ma[ma.flow_type == "expense"]
exp = exp[~exp.year_month.isin(partial)]
last_exp_month = exp.year_month.max()

rows = []
for cat, g in exp.groupby("category"):
    s = g.set_index("year_month")["total_amount"]
    span = pd.period_range(g.year_month.min(), last_exp_month, freq="M").strftime("%Y-%m")
    y = s.reindex(span, fill_value=0.0).values.astype(float)
    if len(y) < 6:
        continue
    X = sm.add_constant(np.arange(len(y), dtype=float))
    ols = sm.OLS(y, X).fit()
    hac = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 3})
    dw = durbin_watson(ols.resid)
    lb = acorr_ljungbox(ols.resid, lags=[min(6, len(y) // 3)], return_df=True)["lb_pvalue"].iloc[0]
    orig = cs.loc[cs.category == cat]
    rows.append(dict(category=cat, n=len(y), p_orig=float(orig.trend_pvalue.iloc[0]) if len(orig) else np.nan,
                     class_orig=orig.trend_class.iloc[0] if len(orig) else None,
                     p_zero_fill=ols.pvalues[1], p_hac=hac.pvalues[1], dw=dw, lb_p=lb,
                     slope=ols.params[1]))
rf = pd.DataFrame(rows)
rf["bh_hac_q10"] = multipletests(rf.p_hac.values, 0.10, "fdr_bh")[0]
print(f"\nrefit on zero-filled complete-month grid ({len(rf)} cats, maxlags=3 for comparability):")
print(f"  p<0.05 naive OLS: {(rf.p_zero_fill < 0.05).sum()} | p<0.05 HAC: {(rf.p_hac < 0.05).sum()}"
      f" | BH(HAC) q=0.10: {rf.bh_hac_q10.sum()}")
print(f"  residual Ljung-Box p<0.05 (autocorrelated): {(rf.lb_p < 0.05).sum()} / {len(rf)};"
      f" Durbin-Watson median {rf.dw.median():.2f} (2 = none)")
print(rf.sort_values("p_orig")[["category", "n", "class_orig", "p_orig", "p_zero_fill", "p_hac", "dw", "lb_p"]]
      .head(15).round(4).to_string(index=False))
print("HAC/OLS slope-SE ratio (pipeline maxlags), rising categories:")
ratios = []
for cat in cs.loc[cs.trend_class == "rising", "category"]:
    g = exp[exp.category == cat]
    span = pd.period_range(g.year_month.min(), last_exp_month, freq="M").strftime("%Y-%m")
    y = g.set_index("year_month")["total_amount"].reindex(span, fill_value=0.0).values.astype(float)
    if len(y) < 6:
        continue
    from src.analysis.stats_utils import newey_west_maxlags
    Xc = sm.add_constant(np.arange(len(y), dtype=float))
    o = sm.OLS(y, Xc).fit()
    h = sm.OLS(y, Xc).fit(cov_type="HAC", cov_kwds={"maxlags": newey_west_maxlags(len(y))})
    ratios.append(h.bse[1] / o.bse[1])
if ratios:
    print(f"  over {len(ratios)} rising categories: median {np.median(ratios):.2f}, max {np.max(ratios):.2f}")

print("\n--- Null simulation: noise-only series, same n and CV as each category ---")
rng = np.random.default_rng(0)
specs = []
for cat, g in exp.groupby("category"):
    s = g.sort_values("year_month")["total_amount"].values.astype(float)
    if len(s) < 6 or s.mean() <= 0:
        continue
    specs.append((np.sqrt(np.log(1 + (s.std(ddof=1) / s.mean()) ** 2)), s.mean(), len(s)))
seas_rate = []
for _ in range(300):
    pvals = []
    for sig, mu, n in specs:
        y = rng.lognormal(np.log(mu) - sig ** 2 / 2, sig, n)  # stationary, no season, no trend
        idx = pd.date_range("2018-01-01", periods=n, freq="MS")
        pvals.append(month_of_year_kruskal(pd.Series(y, index=idx)))
    seas_rate.append(float((multipletests(pvals, 0.10, "fdr_bh")[1] < 0.10).mean()))
print(f"  is_seasonal (KW on residuals + BH q=0.10) on pure noise: median {np.median(seas_rate):.0%},"
      f" range {min(seas_rate):.0%}-{max(seas_rate):.0%}")
print("  has_consecutive_rise: removed in Phase 3 (fired on ~100% of noise series)")

print("=" * 70, "\nB. CATEGORY PROPHET DIRECTION")
ct = pd.read_parquet(P + "category_trends_summary.parquet")
print("rows:", len(ct), "| by level/flow:", ct.groupby(["flow_type", "level"]).size().to_dict())
print("direction counts:", ct.direction.value_counts().to_dict())
if "direction_pvalue_adj" in ct.columns:
    sig = ct["direction_pvalue_adj"] < 0.10
    print(f"direction q (BH, FDR 0.10) < 0.10: {int(sig.sum())} / {int(ct.direction_pvalue_adj.notna().sum())}")
fit = ct[ct.direction != "insufficient_data"].copy()
fit["abs_g"] = fit.growth_pct.abs()
r = spearmanr(fit.mean_monthly, fit.abs_g)
print(f"Spearman(mean_monthly, |growth_pct|) = {r.statistic:.2f} (p={r.pvalue:.3g})  -> small series show bigger swings")
q = pd.qcut(fit.mean_monthly, 4, labels=["smallest", "q2", "q3", "largest"])
print(fit.groupby(q, observed=True).apply(lambda d: pd.Series({
    "n": len(d), "share_non_flat": (d.direction != "flat").mean(), "median_|growth|": d.abs_g.median()})).round(2))
print("last month in series:", ct.last_month.max())

print("=" * 70, "\nC. NET CASHFLOW SERIES (forecast + MC input)")
comp = cf[~cf.is_partial_month]
y = comp.disposable_income.values.astype(float)
print("complete months:", len(y), comp.year_month.min(), "->", comp.year_month.max())
a = acf(y, nlags=3)
print("ACF lags 1-3:", np.round(a[1:], 3), "| Ljung-Box(6) p:",
      round(acorr_ljungbox(y, lags=[6], return_df=True).lb_pvalue.iloc[0], 4))
X = sm.add_constant(np.arange(len(y), dtype=float))
o = sm.OLS(y, X).fit()
oh = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 3})
print(f"OLS trend slope {o.params[1]:.1f}/mo, p={o.pvalues[1]:.3f} (HAC p={oh.pvalues[1]:.3f}),"
      f" 95% CI [{o.conf_int()[1][0]:.1f}, {o.conf_int()[1][1]:.1f}]")
print("month-group sizes for Kruskal-Wallis:", pd.Series(pd.to_datetime(comp.year_month + "-01").dt.month).value_counts().sort_index().to_dict())
part = cf[cf.is_partial_month]
if len(part):
    print("partial month row (dashboard MC includes it):", part[["year_month", "deposits", "withdrawals", "disposable_income"]].to_dict("records"))
    print("  complete-month disposable_income median:", round(float(np.median(y)), 0))

print("\n--- Sum of per-month quantiles vs quantile of cumulative sum (12 mo, residual bootstrap) ---")
res = o.resid
rng = np.random.default_rng(1)
sims = rng.choice(res, size=(20000, 12), replace=True)
per_month_p10 = np.percentile(sims, 10, axis=0)
cum = sims.cumsum(axis=1)
print(f"  sum of monthly P10 at m12: {per_month_p10.sum():,.0f}  vs  true P10 of 12-mo cumulative: {np.percentile(cum[:, -1], 10):,.0f}")
print(f"  coverage of the 'summed P10..P90' band at m12: "
      f"{((cum[:, -1] >= per_month_p10.sum()) & (cum[:, -1] <= np.percentile(sims, 90, axis=0).sum())).mean():.4f} (nominal 0.80)")

print("=" * 70, "\nD. ANNUAL CREEP")
bh = pd.read_parquet(P + "budget_health_monthly.parquet")
from src.analysis.budget_health import build_annual_creep  # noqa: E402
ac = build_annual_creep(bh)
print(ac.round(3).to_string(index=False))

print("=" * 70, "\nE. RECURRING COMMITMENTS vs CURRENCY")
tf = pd.read_parquet(INTERIM + "transactions_featured.parquet")
print("featured columns w/ currency:", [c for c in tf.columns if "curr" in c.lower() or c in ("amount", "amount_pln", "account")])
reg = pd.read_parquet(P + "recurring_commitments.parquet")
print("register rows:", len(reg), "| with price change 12m:", int(reg.has_price_change_12m.sum()))
ccol = next((c for c in tf.columns if "currency" in c.lower()), None)
if ccol:
    pay_ccy = tf[tf.payee.isin(reg.payee)].groupby("payee")[ccol].agg(lambda s: ",".join(sorted(set(map(str, s)))))
    reg2 = reg.merge(pay_ccy.rename("ccy"), left_on="payee", right_index=True, how="left")
    reg2["non_pln"] = ~reg2.ccy.fillna("PLN").str.fullmatch("PLN")
    print(reg2.groupby("non_pln").agg(n=("payee", "size"), with_price_change=("has_price_change_12m", "sum"),
                                       n_changes=("n_price_changes", "sum")).to_string())
