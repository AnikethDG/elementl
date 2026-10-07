"""fct_netsuite_budget_vs_actual: unpivot netsuite_budgets and compare with posted transaction lines."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from common import Context, field, latest_version, series_numeric, tf_to_bool, to_decimal

log = logging.getLogger("silver_transform.budget_vs_actual")

DEFAULT_TABLE = "fct_netsuite_budget_vs_actual"
ON_BUDGET_TOLERANCE = 0.05
KEY = ["account", "department", "year", "month"]

SCHEMA = [
    field("account", "STRING", "REQUIRED", "netsuite account id (part of key)"),
    field("department", "STRING", "REQUIRED", "netsuite department id (part of key)"),
    field("year", "INT64", "REQUIRED"),
    field("month", "INT64", "REQUIRED"),
    field("period_start_date", "DATE"),
    field("budget_amount", "NUMERIC", description="NULL when no budget row exists"),
    field("actual_amount", "NUMERIC", description="sum of posted detail-line netamount; 0 when none"),
    field("variance", "NUMERIC", description="actual - COALESCE(budget, 0)"),
    field("variance_pct", "FLOAT64", description="variance / budget; NULL when budget is 0 or NULL"),
    field("status", "STRING", description="'over' | 'under' | 'on_budget' (within 5%)"),
    field("has_budget", "BOOL"),
    field("has_actual", "BOOL"),
    field("actual_line_count", "INT64"),
]


def unpivot_budgets(budgets: pd.DataFrame) -> pd.DataFrame:
    """netsuite_budgets amount_1..amount_12 -> (account, department, year, month, budget_amount).

    Several budget rows (e.g. categories Operating/Capital) for the same key are summed.
    """
    if budgets.empty:
        return pd.DataFrame(columns=KEY + ["budget_amount"])
    b = latest_version(budgets, "id")
    amount_cols = [f"amount_{i}" for i in range(1, 13) if f"amount_{i}" in b.columns]
    long = b.melt(id_vars=["account", "department", "year"], value_vars=amount_cols, var_name="month",
                  value_name="budget_amount")
    long["month"] = long["month"].str.replace("amount_", "", regex=False).astype(int)
    long["year"] = pd.to_numeric(long["year"], errors="coerce").astype("Int64")
    long["budget_amount"] = series_numeric(long["budget_amount"])
    long = long.dropna(subset=["budget_amount", "year", "account", "department"])
    long["year"] = long["year"].astype(int)
    long["account"] = long["account"].astype(str)
    long["department"] = long["department"].astype(str)
    return long.groupby(KEY, as_index=False)["budget_amount"].sum()


def aggregate_actuals(transactions: pd.DataFrame, lines: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty or lines.empty:
        return pd.DataFrame(columns=KEY + ["actual_amount", "actual_line_count"])
    txn = latest_version(transactions, "id")
    txn = txn[txn["posting"].map(tf_to_bool) == True][["id", "trandate"]]  # noqa: E712
    txn["trandate_d"] = pd.to_datetime(txn["trandate"].map(lambda v: v), format="%m/%d/%Y", errors="coerce")
    txn = txn.dropna(subset=["trandate_d"]).rename(columns={"id": "transaction"})
    det = lines[lines["mainline"].map(tf_to_bool) == False].copy()  # noqa: E712
    det = latest_version(det, "id", tiebreak_col="linelastmodifieddate")
    det["netamount_f"] = series_numeric(det["netamount"])
    j = det.merge(txn[["transaction", "trandate_d"]], on="transaction", how="inner")
    j["year"] = j["trandate_d"].dt.year.astype(int)
    j["month"] = j["trandate_d"].dt.month.astype(int)
    j = j.dropna(subset=["account", "department"])
    j["account"] = j["account"].astype(str)
    j["department"] = j["department"].astype(str)
    agg = j.groupby(KEY, as_index=False).agg(actual_amount=("netamount_f", "sum"), actual_line_count=("netamount_f", "size"))
    return agg


def classify(variance_pct: float | None, actual: float, budget: float | None, tol: float = ON_BUDGET_TOLERANCE) -> str:
    if variance_pct is None or (isinstance(variance_pct, float) and np.isnan(variance_pct)):
        # no / zero budget: any spend is 'over', negative is 'under'
        if actual > 0:
            return "over"
        if actual < 0:
            return "under"
        return "on_budget"
    if variance_pct > tol:
        return "over"
    if variance_pct < -tol:
        return "under"
    return "on_budget"


def compare(budget_long: pd.DataFrame, actuals: pd.DataFrame) -> pd.DataFrame:
    m = budget_long.merge(actuals, on=KEY, how="outer", indicator=True)
    m["has_budget"] = m["_merge"].isin(["both", "left_only"]) & m["budget_amount"].notna()
    m["has_actual"] = m["_merge"].isin(["both", "right_only"])
    m["actual_amount"] = m["actual_amount"].fillna(0.0).astype(float)
    m["actual_line_count"] = m["actual_line_count"].fillna(0).astype(int)
    budget0 = m["budget_amount"].fillna(0.0).astype(float)
    m["variance"] = m["actual_amount"] - budget0
    m["variance_pct"] = np.where(m["budget_amount"].notna() & (budget0 != 0), m["variance"] / budget0.replace(0, np.nan), np.nan)
    m["status"] = [classify(vp, a, b) for vp, a, b in zip(m["variance_pct"], m["actual_amount"], m["budget_amount"])]
    m["year"] = m["year"].astype(int)
    m["month"] = m["month"].astype(int)
    m["period_start_date"] = [pd.Timestamp(year=int(y), month=int(mo), day=1).date() for y, mo in zip(m["year"], m["month"])]
    for c in ("budget_amount", "actual_amount", "variance"):
        m[c] = m[c].map(to_decimal)
    m["variance_pct"] = m["variance_pct"].astype(float).round(6)
    return m[[f.name for f in SCHEMA]].sort_values(KEY).reset_index(drop=True)


def run(ctx: Context) -> int:
    budgets = ctx.read_bronze("netsuite_budgets")
    txn = ctx.read_bronze("netsuite_transaction", ["id", "trandate", "posting", "lastmodifieddate", "ingestion_timestamp"])
    lines = ctx.read_bronze("netsuite_transactionline",
                            ["id", "transaction", "account", "department", "netamount", "mainline", "linelastmodifieddate",
                             "ingestion_timestamp"], where="mainline = 'F'")
    budget_long = unpivot_budgets(budgets)
    actuals = aggregate_actuals(txn, lines)
    out = compare(budget_long, actuals)
    log.info("budget keys=%d actual keys=%d -> %d rows; status=%s", len(budget_long), len(actuals), len(out),
             out["status"].value_counts().to_dict() if len(out) else {})
    return ctx.write_silver(out, SCHEMA)
