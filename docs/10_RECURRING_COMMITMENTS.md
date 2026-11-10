# Recurring Commitments: Detection, Register, and Renewal Calendar

## Overview

The recurring-commitments stage turns free-text payees into a factual register of
payments that recur on a stable cadence: rent, utilities, taxes, installments,
gym memberships, and subscriptions. It answers three questions without
prescribing any action:

1. **What do I pay regularly?** — one register row per recurring payee.
2. **When is the next charge expected?** — a renewal calendar derived from the
   observed median gap.
3. **Has the price changed?** — per-occurrence events flag material jumps.

Implementation: [`src/analysis/recurring_commitments.py`](../src/analysis/recurring_commitments.py).
Outputs are written to `data/processed/recurring_commitments.parquet`,
`data/processed/recurring_commitment_events.parquet` and
`data/processed/recurring_categories.parquet` (the category roll-up), and
surfaced on the dashboard's **Recurring Costs** tab.

The register is descriptive. It carries flags (`has_price_change_12m`,
`is_lapsed`, `is_renewal_due_30d`) and never an action label such as "cancel" or
"renegotiate" — this project's design principles exclude financial advice.

---

## 1. Definitions

| Term | Definition |
|------|------------|
| **Series** | All non-transfer withdrawal rows sharing one exact payee string (whitespace-stripped, case-sensitive), with `date <= as_of`. |
| **Trailing window** | The `run_rate_window_months` (default 12) ending at `as_of`; if the series is younger, it runs from the first occurrence. |
| **Commitment** | A series with at least `min_distinct_months` distinct `year_month` values **and** a trailing-window amount CV at or below `amount_cv_threshold`. CV is measured on daily totals and tolerates one large price step (see Step 3). |
| **Cadence class** | `weekly`, `biweekly`, `monthly`, `quarterly`, `semiannual`, `annual`, or `irregular`, from the median gap between distinct payment dates. |
| **Active** | `last_seen` within `active_window_days` of `as_of`. |
| **Lapsed** | Not active, cadence known, and the next expected date is more than 1.5 median cycles overdue. |
| **Renewal due (30d)** | Not lapsed, known cadence, and `next_expected_date` within 30 days of `as_of` (overdue items included). Not gated on `is_active`, so annual/semiannual renewals can qualify. |
| **Monthly run-rate** | Sum of absolute amounts inside the trailing window ÷ months spanned by the window (capped at 12). |
| **Price-change event** | A payment date whose total differs from the previous payment date's total where `|Δ| / prev > price_change_pct` **or** `|Δ| > price_change_abs_pln`. Same-day split purchases are summed before comparison. |
| **`as_of`** | Analysis cutoff, default today. Future-dated scheduled rows are excluded inside this module only. |

Amounts are converted with the house convention: `amount_pln` when present,
otherwise `amount`. Withdrawals are negative in MMEX, so absolute values are
used throughout.

---

## 2. Algorithm

```python
from src.analysis.recurring_commitments import build_recurring_commitments

register, events = build_recurring_commitments(
    df_featured,              # output of engineer_features()
    as_of=None,               # default: today
    config={"min_distinct_months": 6, ...},  # optional overrides
)
```

### Step 1 — Row eligibility

Keep rows where:

- `date <= as_of` (drops future-dated scheduled rows),
- `type == "withdrawal"` (income timing is a Phase 1 concern),
- `is_transfer` is false,
- `payee` is not null/blank after stripping whitespace (dropped rows are counted
  and logged),
- `|amount_pln| >= min_amount_pln`.

### Step 2 — Series statistics (per payee)

- `n_transactions`, `n_distinct_months`, `first_seen`, `last_seen`.
- `median_gap_days` = median of day gaps between **distinct** payment dates.
  Same-day split purchases therefore do not distort the cadence.
- `cadence_class` via the windows below.

| Cadence | Median gap (days) |
|---------|-------------------|
| weekly | 5–10 |
| biweekly | 11–18 |
| monthly | 24–38 |
| quarterly | 80–105 |
| semiannual | 165–200 |
| annual | 340–390 |
| irregular | anything else (including NaN) |

### Step 3 — Commitment test

1. Require `n_distinct_months >= min_distinct_months` (default 6).
2. Require a trailing window with enough observations: at least 2, except for
   semiannual/annual series (median gap ≥ 150 days), where a single payment in
   12 months is normal. A frequent payee with one recent payment is a stale
   one-off, not a current commitment.
3. Sum amounts per payment date and compute
   `amount_cv_trailing = std(daily totals) / mean(daily totals)` over the window
   (0.0 when only one date is present). Exclude the series when
   `amount_cv_trailing > amount_cv_threshold`.

   Same-day split purchases are summed first, so a two-component installment
   (e.g. 500 + 600 PLN) is one 1,100 PLN charge, not alternating 500/600
   outliers. When the raw CV still exceeds the threshold, the window is split at
   its largest material price change and the **within-regime** CV (the larger of
   the two segments) is used instead. A single step change — including a large
   hike — therefore stays in the register, while continuously variable payees
   (supermarkets, transport) are still rejected.

This separates fixed or step-priced commitments (rent, a subscription that
changed price once) from habitual-but-variable payees.

### Step 4 — Run-rate, dates, flags

- `monthly_run_rate = sum(|amount| in window) / months_spanned`, where
  `months_spanned` is the inclusive calendar-month span of the window capped at
  `run_rate_window_months`.
- `next_expected_date = last_seen + median_gap_days`.
- `days_overdue = max(0, as_of − next_expected_date)`.
- `is_active`, `is_lapsed`, `is_renewal_due_30d`.
- Price changes are detected on the **full** series in date order, comparing
  daily totals (same-day rows are summed first). `n_price_changes` counts
  change **dates**, not rows. The register keeps `n_price_changes`,
  `last_price_change_date`, `last_price_change_pct` and `has_price_change_12m`.

### Step 5 — Outputs

Register columns (exact order):

```
payee, category, subcategory, n_transactions, n_distinct_months, first_seen,
last_seen, median_gap_days, cadence_class, is_active, amount_current,
amount_median, amount_cv_trailing, monthly_run_rate, run_rate_window_months,
next_expected_date, days_overdue, is_lapsed, n_price_changes,
last_price_change_date, last_price_change_pct, has_price_change_12m,
is_renewal_due_30d
```

`category` / `subcategory` are the most frequent values in the series;
`category` falls back to `"(uncategorised)"`. `amount_current` is the modal
amount in the trailing window (ties resolve to the most recent occurrence), so
the register shows today's price after a step change.

Events columns: `payee, date, year_month, amount, prev_amount,
is_price_change` — one row per occurrence of a register member, including the
full history (useful for the payee chart).

---

## 3. Configuration

Defaults live in code and can be overridden in `configs/pipeline.yaml`:

```yaml
recurring:
  min_distinct_months: 6
  active_window_days: 90
  amount_cv_threshold: 0.10
  price_change_pct: 0.05
  price_change_abs_pln: 5.0
  run_rate_window_months: 12
  min_amount_pln: 0.0
  category_cv_threshold: 0.25
```

Every key has a code default, so the pipeline also runs when the section is
absent.

---

## 4. Worked examples

### Rent — fixed monthly commitment

Twelve payments of 1,350 PLN on the 1st of each month, the last 27 days before
`as_of`:

- 12 distinct months ≥ 6 → passes the frequency test.
- All amounts are identical → `amount_cv_trailing = 0.0` → passes the CV test.
- Median gap = 31 days → `monthly` cadence.
- Run-rate = `(12 × 1,350) / 12 = 1,350 PLN/month`.
- `last_seen = 2026-10-01`, `next_expected_date = 2026-11-01`, `is_active = True`.

### A yearly step change (ZUS-style) — price-change event

Twelve monthly payments where the rate steps from 2,400 to 2,510 PLN:

- `Δ = 110 PLN`, which is `110 / 2,400 = 4.58%` — below the 5% relative
  threshold but above the 5 PLN absolute threshold, so the change is still
  flagged. The absolute rule catches small-rate, high-value commitments whose
  relative move is modest.
- `amount_current` = the modal amount in the window (2,510 when that rate has
  covered most of the window).
- `last_price_change_pct ≈ 0.0458`, `has_price_change_12m = True`.

---

## 5. Category summary

A category-level register is not a second payee register: a category aggregates
many payees, so its monthly total is inherently variable. In the current data
only 1 of 22 categories clears the payee-level 0.10 CV gate, so a strict
category register would be almost empty. Instead,
`build_recurring_category_summary(register, df_featured, as_of, config)` combines
two views:

- **Recurring amount** — the active payee commitments summed by category.
- **Category spend** — the category's average monthly withdrawals over the
  trailing window of complete months.

| Column | Meaning |
|---|---|
| `category` | Category name; blanks become `(uncategorised)`. |
| `n_months` | Distinct months with any spend in the category's history. |
| `active_months_ttm` | Months with spend inside the averaging window. |
| `last_month` | Most recent month with spend (`YYYY-MM`). |
| `cadence_class` | Cadence of months with spend (usually `monthly`). |
| `is_active` | Last spend within `active_window_days` of `as_of`. |
| `avg_monthly_spend` | Mean monthly total over up to 12 complete months, starting at the category's first spend; gaps count as zero. |
| `spend_cv` | Std / mean of those monthly totals. |
| `is_steady` | `active_months_ttm >= min_distinct_months`, category active, and `spend_cv <= category_cv_threshold` (default 0.25). |
| `n_commitments` | Payee commitments in the category (all). |
| `n_active_commitments` | Active payee commitments in the category. |
| `n_lapsed_commitments` | Lapsed payee commitments in the category. |
| `recurring_run_rate` | Sum of active commitments' monthly run-rates. |
| `recurring_coverage` | `min(1, recurring_run_rate / avg_monthly_spend)`. |
| `share_of_spend` | Category's share of total average monthly spend. |
| `n_price_changes_12m` | Commitments with a price change in the last 12 months. |
| `top_payee` | Highest-run-rate payee in the category, or `—`. |

**Read coverage with care.** It depends on payee detection: a category can be
steady with zero coverage when its payees are excluded by the CV gate. Taxes in
the current data is the example — ZUS carries extra annual contributions, so
the payee series is excluded even though the category's own spend is steady.
`spend_cv` and `is_steady` are payee-independent and should be read alongside
coverage.

Only categories with spend in the recent complete-month window are emitted;
dormant categories do not clutter the baseline.

---

## 6. Dashboard tab

`Recurring Costs` reads `recurring_commitments`, `recurring_commitment_events`,
`recurring_categories` and `monthly_cashflow_split`:

- KPI cards: active commitment count, active monthly run-rate, and the run-rate
  as a share of an average trailing-12-month month (annualised run-rate ÷ TTM
  withdrawals). The denominator uses complete months only (`year_month` before
  the current month) and takes the last 12, so neither the partial current month
  nor forecast months are counted.
- Category summary: stacked horizontal bars (recurring vs other spend per
  category) and a table with stability, average spend, coverage, and top payee.
  If `recurring_categories.parquet` is absent, an info alert is shown and the
  payee register still renders.
- Register table with active/lapsed badges.
- Top-15 commitments by monthly run-rate (horizontal bar).
- Renewal calendar: known-cadence, non-lapsed commitments due within 60 days,
  sorted by `next_expected_date` (overdue items appear first). It is deliberately
  not filtered on `is_active`, so annual and semiannual renewals appear when
  their last payment is more than 90 days old.
- Payee dropdown → amount history line, with price-change points marked.

When the register parquet is absent the tab shows the standard pipeline-data
warning instead of crashing.

---

## 7. Assumptions and limitations

- **Exact payee strings.** Payee renames split a series; canonical
  `merchant_id` matching is a separate Tier-2 item. Free-text payees make
  coverage partial, and null/blank payees cannot be keyed at all (counted
  and logged).
- **No usage data.** The register shows what recurs and what it costs; it cannot
  judge whether a subscription is worth its fee. That is a human decision.
- **One-off extra charges can hide a commitment.** A payee whose monthly charge
  is stable but occasionally carries an extra payment (annual corrections,
  partial months, multi-component invoices) has a high daily-total CV at the
  payee level and is excluded under the 0.10 threshold. In the current dataset
  this affects large variable contributors such as ZUS. Amount-cluster detection
  (one series per stable amount) is the follow-up that would recover them; it is
  deliberately out of scope here because it splits a series at every price
  change.
- **Category coverage is not a pure fixed-vs-variable split.** The category
  roll-up sums only detected payee commitments. A category whose payees are
  excluded (ZUS/installments) can show low coverage despite steady spend; use
  `spend_cv` and `is_steady`, which are computed from category totals and do not
  depend on payee matching, as the payee-independent signal.
- **Multiple step changes.** The change-tolerant CV splits the window at the
  single largest material step. A payee with several large steps plus noise can
  still exceed the threshold and be excluded (the within-regime CV remains high).
- **Split purchases and duplicates** (443 extra same-day rows in the current
  data) do not distort cadence, because gaps use distinct dates. Amounts are
  summed per payment date before the CV and price-change tests, so same-day
  installments produce no false price changes; they still add occurrence rows to
  the events output.
- **Variable recurring payees** (groceries, transport) are excluded by the CV
  rule. The excluded-variable count is logged for transparency.
- **Price changes may be data-entry errors.** They are flagged for human review;
  nothing is auto-actioned.
- **`as_of` is local to this module.** Phase 1's `is_future`/`--as-of` work is
  unimplemented, so this stage performs its own cutoff. It is a three-line guard,
  not a replacement for Phase 1.
- **Partial current month.** The run-rate denominator is the trailing window, so
  a partial month affects freshness (`is_active`, `days_overdue`) only.

---

## 8. IWTYTBR methodology mapping

The IWTYTBR skill (`.agents/skills/i-will-teach-you-to-be-rich/`) informs the
*ordering* of this feature, not its outputs. The register is the automated
equivalent of the book's first workflow step — a written audit of recurring
charges before any budget is set. The mapping below is methodology, not
financial advice.

| Register capability | IWTYTBR method | Reference |
|---|---|---|
| Renewal calendar (`next_expected_date`, `is_renewal_due_30d`) | Automatic Money Flow, step 2: **synchronise bill dates** to a single day and automate fixed payments | `references/spending-plan-automation.md` |
| Price-change flags (`n_price_changes`, `last_price_change_pct`, `has_price_change_12m`) | **Negotiation targets** — know the current fee/rate before calling to request a waiver or reduction (Credit Fee and APR Negotiation Scripts) | `references/banking-credit-debt.md` |
| Inactive / lapsed series (`is_lapsed`, dormant payees) | **A La Carte Method** — review auto-renewing subscriptions and keep only those whose usage justifies the fee | `references/spending-plan-automation.md` |
| Monthly run-rate and share of expense | CEO method, step 1: **Cut Costs** — quantify the recurring baseline before renegotiating or cancelling | `references/spending-plan-automation.md` |

The 85 Percent Solution applies: an automated register beats a hand-maintained
subscription spreadsheet, even though it is not a complete Conscious Spending
Plan. The skill's pitfalls also explain the scope boundary:

| Skill pitfall | How this stage responds |
|---|---|
| Arguing over minor purchases instead of pursuing big wins | The register ranks rent, taxes, installments, and subscriptions by monthly run-rate; discretionary line items are never itemised. |
| Paying active-fund management fees above 0.75% | Fund holdings and expense ratios are not in the transaction feed, so this is not observable here; it belongs to the index-portfolio rung. |
| Carrying credit-card debt while funding taxable investments | The feed has no balances or APRs, so the five-step debt plan is not computable from these records. |
| Irregular or freelance income | When income is booked irregularly and taxes are a large share of outflow, the 3–6-month buffer and the take-home definition must be settled before the four-bucket plan. |

The four-bucket formula (Fixed Costs 50–60%, Investments 10%, Savings Goals
5–10%, Guilt-Free 20–35%) and the Ladder of Personal Finance are the next
rungs. They remain out of scope until Phase 1's correctness fixes, an agreed
category-to-bucket mapping, and a take-home/buffer definition are in place.
Reference files for those later rungs:

- `references/spending-plan-automation.md` — four buckets, A La Carte,
  Automatic Money Flow, irregular-income buffer.
- `references/banking-credit-debt.md` — fee/APR negotiation, five-step debt plan.
- `references/investing-portfolio-selection.md` — Ladder, target-date vs Swensen
  portfolios, fee red flags.
- `references/negotiation-major-purchases.md` — salary and major-purchase
  negotiation.

---

## 9. Out of scope

- Conscious Spending Plan four-bucket targets and category mapping.
- Phase 1 correctness fixes (`is_future`, `as_of`, income timing).
- Payee canonicalisation / `merchant_id`.
- Contract metadata (start, end, notice period, signup price).
- xlsx/CSV exports of the register (exports only on explicit user request).
- Any recommendation to cancel, switch, or renegotiate a service.
