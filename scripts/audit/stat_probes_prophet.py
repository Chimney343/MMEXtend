"""
Heavier probes: seasonality battery, Prophet holidays, backtest, permutation null.

Checks that the statistical labels and forecast bands stay calibrated: noise
and time-shuffled series should rarely be labelled, and the backtest reports
how often bands cover outcomes. Reads your local pipeline outputs; prints
summaries only.
Takes several minutes (about 250 Prophet fits).

Run from the repo root:
    PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe scripts/audit/stat_probes_prophet.py
"""
import logging
import sys
import warnings

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import acf

warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)
sys.path.insert(0, ".")
from src.analysis.cashflow_forecast import build_prophet_model, run_expenditure_mc  # noqa: E402
from src.analysis.category_prophet import (  # noqa: E402
    _continuous_month_series, _iter_series, build_category_monthly_series)
from src.analysis.feature_engineering import complete_cashflow_series  # noqa: E402
from src.analysis.seasonality_tests import run_seasonality_battery  # noqa: E402

logging.disable(logging.CRITICAL)
INTERIM, P = "data/interim/", "data/processed/"
cf = pd.read_parquet(INTERIM + "monthly_cashflow_split.parquet")
comp = cf[~cf.is_partial_month]
y = pd.Series(comp.disposable_income.values.astype(float), index=pd.to_datetime(comp.year_month + "-01"))

print("=" * 70, "\n1. SEASONALITY BATTERY on current complete months (n=%d)" % len(y))
b = run_seasonality_battery(y)
for t in b["tests"]:
    print("  ", t["test_name"], "|", t.get("has_seasonality", t.get("recommended_mode")), "|", t["note"][:110])
print("  VERDICT:", b["verdict"])

print("=" * 70, "\n2. PROPHET HOLIDAYS ON MONTH-START ds (Phase 4a: removed)")
m = build_prophet_model(False)
m.fit(pd.DataFrame({"ds": y.index, "y": y.values}))
print("  model.holidays:", m.holidays, "| country_holidays:",
      getattr(m, "country_holidays", None))
fut = m.make_future_dataframe(periods=24, freq="MS")
fc = m.predict(fut)
fc_f = fc.iloc[len(y):]
holiday_col = fc_f["holidays"] if "holidays" in fc_f.columns else pd.Series(0.0, index=fc_f.index)
eff = fc_f.loc[holiday_col.abs() > 1e-6, ["ds"]].copy()
print("  future months carrying a holiday effect:", len(eff), "of 24 (expected 0)")
print("  residual sd (in-sample):", round(float(np.std(y.values - fc.yhat.iloc[:len(y)].values)), 0))

print("=" * 70, "\n3. ROLLING-ORIGIN BACKTEST (12-month horizon, shipped code path)")
from src.analysis.forecast_backtest import (  # noqa: E402
    empirical_bands, naive_mean_forecaster, rolling_origin_backtest, summarise_backtest)
from src.analysis.cashflow_forecast import select_and_run_forecast  # noqa: E402

prophet_bt = rolling_origin_backtest(y, select_and_run_forecast)
naive_bt = rolling_origin_backtest(y, naive_mean_forecaster)
for label, bt in (("Prophet (shipped)", prophet_bt), ("naive trailing-12 mean", naive_bt)):
    s = summarise_backtest(bt)
    print(f"  {label:24s} {s['n_origins']:2d} origins | MAE/month {s['mae_month']:8,.0f}"
          f" | |12-mo cum err| {s['abs_cum_err_12']:9,.0f}"
          f" | monthly 80% cov {s['model_ci80_coverage_month']:.2f}"
          f" | holdout cum 80% band {s['holdout_cum80_k']}/{s['holdout_cum80_m']}")
shipped = empirical_bands(prophet_bt, select_and_run_forecast(y, periods=24))
print("  shipped band:", shipped["interval_method"].iloc[0], "| ci95 all NaN:",
      bool(shipped["ci95_lower"].isna().all()))

print("=" * 70, "\n4. DASHBOARD MC: partial month included vs excluded")
dashboard_series = complete_cashflow_series(cf)
for name, s in (("as dashboard (complete months)", dashboard_series),
                ("complete months only", y)):
    r = run_expenditure_mc(s, 0.0, n_simulations=4000, horizon_months=12, detrend=True)
    print(f"  {name:30s} 12-mo cumulative: P10 {r['p10'][-1]:9,.0f} | P50 {r['p50'][-1]:9,.0f} | P90 {r['p90'][-1]:9,.0f}")
print("  dashboard series == complete series:", dashboard_series.equals(y))

print("=" * 70, "\n5. IID vs BLOCK BOOTSTRAP of detrended residuals (12-mo cumulative)")
X = sm.add_constant(np.arange(len(y), dtype=float))
resid = sm.OLS(y.values, X).fit().resid
print("  residual ACF lags 1-3:", np.round(acf(resid, nlags=3)[1:], 3))
rng = np.random.default_rng(2)
iid = rng.choice(resid, size=(20000, 12)).sum(axis=1)
blk = []
for _ in range(20000):
    starts = rng.integers(0, len(resid) - 3, size=4)
    blk.append(np.concatenate([resid[s:s + 3] for s in starts]).sum())
blk = np.array(blk)
print(f"  12-mo cumulative residual: iid P10 {np.percentile(iid, 10):,.0f} / sd {iid.std():,.0f}"
      f"  vs block(3) P10 {np.percentile(blk, 10):,.0f} / sd {blk.std():,.0f}")

print("=" * 70, "\n6. EXPENSE TRENDS: naive OLS vs HAC slope SE, zero-filled complete-month grid")
ma = pd.read_parquet(INTERIM + "monthly_aggregates.parquet")
et = pd.read_parquet(P + "expense_trends.parquet").drop_duplicates("category")
cf6 = pd.read_parquet(INTERIM + "monthly_cashflow_split.parquet")
partial_months = set(cf6.loc[cf6.is_partial_month, "year_month"])
exp = ma[(ma.flow_type == "expense") & (~ma.year_month.isin(partial_months))]
from src.analysis.stats_utils import newey_west_maxlags  # noqa: E402
last_exp_month = exp.year_month.max()
ratios = []
for cat in et.loc[et.trend_class == "rising", "category"]:
    g = exp[exp.category == cat]
    span = pd.period_range(g.year_month.min(), last_exp_month, freq="M").strftime("%Y-%m")
    s = g.set_index("year_month")["total_amount"].reindex(span, fill_value=0.0).values.astype(float)
    Xc = sm.add_constant(np.arange(len(s), dtype=float))
    o = sm.OLS(s, Xc).fit()
    h = sm.OLS(s, Xc).fit(cov_type="HAC", cov_kwds={"maxlags": newey_west_maxlags(len(s))})
    ratios.append(h.bse[1] / o.bse[1])
print(f"  HAC/OLS slope-SE ratio over {len(ratios)} rising categories: median {np.median(ratios):.2f}, max {np.max(ratios):.2f}")
nov = exp[exp.year_month == "2026-11"].set_index("category").total_amount
print(f"  categories with a partial-month point in the fit: {len(nov)} (expected 0)")

print("=" * 70, "\n7. FX: non-PLN recurring payees, price changes in PLN vs original currency")
tf = pd.read_parquet(INTERIM + "transactions_featured.parquet")
reg = pd.read_parquet(P + "recurring_commitments.parquet")
for payee in reg.payee:
    t = tf[(tf.payee == payee) & (tf.currency != "PLN") & tf.type.str.lower().isin(["withdrawal", "expense"])]
    if t.empty:
        continue
    d = t.groupby("date").agg(orig=("amount", lambda v: v.abs().sum()), pln=("amount_pln", lambda v: v.abs().sum()))
    def flags(v):
        p = v.shift(1)
        return int((((v - p).abs() / p > 0.05) | ((v - p).abs() > 5.0)).iloc[1:].sum())
    # original-currency check uses the 5% rule only (5 PLN rule is PLN-specific)
    orig_flags = int((((d.orig - d.orig.shift(1)).abs() / d.orig.shift(1)) > 0.05).iloc[1:].sum())
    print(f"  payee#{abs(hash(payee)) % 1000:03d} ({t.currency.iloc[0]}, {len(d)} charges): "
          f"flags on PLN amounts {flags(d.pln)} | on original-currency amounts (5% rule) {orig_flags}")

print("=" * 70, "\n8. DIRECTION: permutation null (expense, category level)")
from src.analysis.stats_utils import FDR_Q, bh_adjust, fit_linear_trend  # noqa: E402

df_feat = tf
monthly = build_category_monthly_series(df_feat)
series_list = [
    _continuous_month_series(s)
    for flow, level, cat, sub, s in _iter_series(monthly)
    if flow == "expense" and level == "category" and len(_continuous_month_series(s)) >= 24
]
rng = np.random.default_rng(3)
non_flat_rates, all_labels = [], []
for _ in range(20):
    pvals, slopes = [], []
    for s in series_list:
        perm = rng.permutation(s.values)
        fit = fit_linear_trend(perm, np.arange(len(perm), dtype=float))
        pvals.append(fit["p_value"])
        slopes.append(fit["slope"])
    q = bh_adjust(pd.Series(pvals))
    labels = [
        ("rising" if sl > 0 else "falling") if qq < FDR_Q else "flat"
        for qq, sl in zip(q, slopes)
    ]
    non_flat_rates.append(float(np.mean([label != "flat" for label in labels])))
    all_labels.extend(labels)
print(f"  {len(series_list)} shuffled series x 20 draws = {len(all_labels)} HAC OLS fits")
print("  direction on shuffled data:",
      pd.Series(all_labels).value_counts(normalize=True).round(3).to_dict())
print(f"  non-flat share (BH within family, FDR {FDR_Q:.0%}): median {np.median(non_flat_rates):.3f},"
      f" max {np.max(non_flat_rates):.3f}")
