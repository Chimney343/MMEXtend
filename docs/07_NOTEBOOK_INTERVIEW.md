# Interviewing Data at Intermediate Stages

## Overview

Every pipeline step saves its output as a named parquet file. JupyterLab notebooks are the primary interface for inspecting, validating, and exploring data between steps. This document defines the inspection protocol.

---

## Principle: Every Stage is Inspectable

```
raw CSV/SQLite
     │
     ▼
[00_data_quality.ipynb] → transactions_validated.parquet  ← INSPECT HERE
     │
     ▼
[01_wrangle.ipynb]      → transactions_featured.parquet   ← INSPECT HERE
                        → monthly_aggregates.parquet      ← INSPECT HERE
     │
     ▼
[02_expense_trends.ipynb] → expense_trends.parquet        ← INSPECT HERE
     │
     ▼
[03_cashflow_forecast.ipynb] → cashflow_forecast.parquet  ← INSPECT HERE
     │
     ▼
[04_networth_trajectory.ipynb] → networth_trajectory.parquet ← INSPECT HERE
     │
     ▼
[05_advisory_summary.ipynb] → final narrative output
```

Every `.parquet` file is a checkpoint. You can open a blank notebook at any time, load any checkpoint, and explore.

---

## Standard Inspection Cells

Paste these into any notebook to inspect an intermediate DataFrame. They are designed as a copy-paste protocol — every inspection follows the same structure.

### Cell Template: Load and Profile

```python
"""
INSPECTION CELL — paste into any notebook.
Change `TABLE_NAME` and `STAGE` to match the file you want to inspect.
"""

import sys
sys.path.insert(0, "..")
import pandas as pd
from IPython.display import display, HTML

# ──────────────────────────────────────────────
# CONFIGURE: which file to inspect
TABLE_NAME = "transactions_validated"  # file stem (no .parquet)
STAGE = "interim"                      # "interim" or "processed"
# ──────────────────────────────────────────────

filepath = f"../data/{STAGE}/{TABLE_NAME}.parquet"
df = pd.read_parquet(filepath)
print(f"Loaded: {filepath}")
print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
print(f"Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
```

### Cell Template: Column Summary

```python
# Column types and null counts
info_df = pd.DataFrame({
    "dtype": df.dtypes,
    "non_null": df.count(),
    "null_count": df.isna().sum(),
    "null_pct": (df.isna().mean() * 100).round(2),
    "nunique": df.nunique(),
    "sample": df.iloc[0] if len(df) > 0 else None,
})
display(info_df)
```

### Cell Template: Descriptive Statistics

```python
# Numeric columns
display(df.describe().T.round(2))

# Date columns
for col in df.select_dtypes(include=["datetime64"]).columns:
    print(f"\n{col}: {df[col].min()} → {df[col].max()} ({df[col].nunique()} unique dates)")

# Categorical columns
for col in df.select_dtypes(include=["object", "category"]).columns:
    print(f"\n{col} — top 10 values:")
    display(df[col].value_counts().head(10))
```

### Cell Template: Spot-Check Rows

```python
# Random sample for manual review
display(df.sample(min(10, len(df)), random_state=42))
```

### Cell Template: Check for Known Issues

```python
# Duplicate check
dupes = df.duplicated().sum()
print(f"Duplicate rows: {dupes}")

# Date continuity check (if date column exists)
if "date" in df.columns:
    date_series = df["date"].sort_values()
    gaps = date_series.diff()
    big_gaps = gaps[gaps > pd.Timedelta(days=14)]
    if len(big_gaps) > 0:
        print(f"\n⚠ Date gaps > 14 days:")
        for idx in big_gaps.index:
            print(f"  {date_series.iloc[idx-1].date()} → {date_series.iloc[idx].date()}")
    else:
        print("✓ No date gaps > 14 days")

# Amount sanity check
if "amount" in df.columns:
    print(f"\nAmount range: {df['amount'].min():.2f} → {df['amount'].max():.2f}")
    extremes = df.loc[df["amount"].abs() > df["amount"].abs().quantile(0.99)]
    if len(extremes) > 0:
        print(f"⚠ {len(extremes)} extreme values (>99th percentile):")
        display(extremes[["date", "account", "category", "amount", "payee"]].head(5))
```

---

## Stage-Specific Inspection Guides

### After Step 0: `transactions_validated.parquet`

**Key questions to answer:**
1. Did all rows load? Compare against MMEX's own transaction count.
2. Are dates in the expected range?
3. What percentage is uncategorised? (> 10% = ask user to categorise before proceeding)
4. Are there multi-currency rows? (halt if so, pending user decision)
5. Any null accounts?

```python
# Quick validation dashboard
print(f"Total transactions: {len(df)}")
print(f"Date range: {df['date'].min().date()} → {df['date'].max().date()}")
print(f"Accounts: {df['account'].nunique()}")
print(f"Categories: {df['category'].nunique()}")
print(f"Uncategorised: {df['category'].isna().sum()} ({df['category'].isna().mean():.1%})")
if "currency" in df.columns:
    print(f"Currencies: {df['currency'].unique()}")
```

### After Step 1: `monthly_aggregates.parquet`

**Key questions:**
1. Do monthly totals look reasonable? (No months with 0 income if you have salary)
2. Is the savings rate within expected bounds?
3. Are rolling averages smooth or jumpy?

```python
# Monthly time series quick view
import matplotlib.pyplot as plt
fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)

axes[0].bar(df["month"], df["income"], color="green", alpha=0.7, label="Income")
axes[0].bar(df["month"], df["expenditure"], color="red", alpha=0.7, label="Expenditure")
axes[0].set_title("Monthly Income vs Expenditure")
axes[0].legend()

axes[1].plot(df["month"], df["net_cashflow"], marker="o")
axes[1].axhline(0, color="gray", linestyle="--")
axes[1].set_title("Net Cash Flow")

axes[2].plot(df["month"], df["savings_rate"], marker="o")
axes[2].set_title("Savings Rate")
axes[2].set_ylabel("Ratio")

plt.tight_layout()
fig_monthly = fig  # persist for re-use
plt.show()
```

### After Step 2: `expense_trends.parquet`

**Key questions:**
1. Which categories are flagged as "rising"?
2. Are the R² values high enough to trust the trend? (> 0.3 is informative, > 0.6 is strong)
3. Any categories flagged as highly seasonal?

```python
# Trend summary table
trend_summary = df.groupby("category").agg({
    "trend_slope": "first",
    "trend_r2": "first",
    "trend_pvalue": "first",
    "trend_class": "first",
    "share_pct": "mean",
}).sort_values("share_pct", ascending=False)

display(trend_summary.style.background_gradient(subset=["share_pct"], cmap="Reds"))
```

### After Step 3: `cashflow_forecast.parquet`

**Key questions:**
1. Is the point estimate trajectory realistic?
2. How wide are the CIs? (Wide = high uncertainty = don't trust)
3. What's the runway at each scenario?

```python
import plotly.graph_objects as go

fig = go.Figure()
fig.add_trace(go.Scatter(x=df["month"], y=df["point_estimate"], name="Forecast"))
fig.add_trace(go.Scatter(
    x=df["month"].tolist() + df["month"].tolist()[::-1],
    y=df["ci80_upper"].tolist() + df["ci80_lower"].tolist()[::-1],
    fill="toself", name="80% CI", opacity=0.3
))
fig.update_layout(title="Cash Flow Forecast", xaxis_title="Month", yaxis_title="Net Cash Flow")
fig_forecast = fig
fig.show()
```

---

## Comparison Across Runs

When you re-run the pipeline after updating categories or fixing data:

```python
# Compare two versions of monthly aggregates
df_old = pd.read_parquet("../data/interim/monthly_aggregates_backup.parquet")
df_new = pd.read_parquet("../data/interim/monthly_aggregates.parquet")

comparison = df_old.merge(df_new, on="month", suffixes=("_old", "_new"))
comparison["net_cf_delta"] = comparison["net_cashflow_new"] - comparison["net_cashflow_old"]

display(comparison[["month", "net_cashflow_old", "net_cashflow_new", "net_cf_delta"]])
```

---

## Tips

- **Use `display()` not `print()` for DataFrames** — proper HTML table rendering in JupyterLab
- **Assign every figure to a variable** (`fig = plt.figure(...)`) so it persists in memory
- **Name your DataFrames descriptively** — `df_monthly`, `df_trends`, not `df`, `df2`
- **Add markdown cells above every code cell** explaining what you're checking and why
- **If something looks wrong, stop.** Don't proceed to the next step with bad data.
