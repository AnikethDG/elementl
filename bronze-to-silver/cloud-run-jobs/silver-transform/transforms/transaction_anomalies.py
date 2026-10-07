"""fct_netsuite_transaction_anomalies: z-score of every detail line's netamount within its account."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from common import Context, field, latest_version, parse_ns_date, series_numeric, tf_to_bool, to_decimal

log = logging.getLogger("silver_transform.transaction_anomalies")

DEFAULT_TABLE = "fct_netsuite_transaction_anomalies"
Z_THRESHOLD = 2.5

SCHEMA = [
    field("line_id", "STRING", "REQUIRED", "netsuite_transactionline.id (primary key)"),
    field("transaction_id", "STRING", "REQUIRED"),
    field("tranid", "STRING"),
    field("transaction_type", "STRING"),
    field("trandate", "DATE"),
    field("posting", "BOOL"),
    field("linesequencenumber", "INT64"),
    field("account", "STRING"),
    field("department", "STRING"),
    field("subsidiary", "STRING"),
    field("netamount", "NUMERIC"),
    field("account_line_count", "INT64"),
    field("account_mean", "NUMERIC"),
    field("account_std", "NUMERIC", description="sample std (ddof=1); NULL when < 2 lines"),
    field("z_score", "FLOAT64", description="NULL when account_std is 0/NULL"),
    field("is_anomaly", "BOOL", description="|z_score| > 2.5"),
]


def detect(transactions: pd.DataFrame, lines: pd.DataFrame, threshold: float = Z_THRESHOLD) -> pd.DataFrame:
    if transactions.empty or lines.empty:
        return pd.DataFrame(columns=[f.name for f in SCHEMA])
    txn = latest_version(transactions, "id")[["id", "tranid", "type", "trandate", "posting"]].rename(
        columns={"id": "transaction_id", "type": "transaction_type"})
    txn["trandate"] = txn["trandate"].map(parse_ns_date)
    txn["posting"] = txn["posting"].map(tf_to_bool)

    det = lines[lines["mainline"].map(tf_to_bool) == False].copy()  # noqa: E712 (None-safe)
    det = latest_version(det, "id", tiebreak_col="linelastmodifieddate")
    det = det.rename(columns={"id": "line_id", "transaction": "transaction_id"})
    det["netamount_f"] = series_numeric(det["netamount"])

    df = det.merge(txn, on="transaction_id", how="inner")
    df["linesequencenumber"] = pd.to_numeric(df["linesequencenumber"], errors="coerce").astype("Int64")

    grp = df.groupby("account")["netamount_f"]
    df["account_line_count"] = grp.transform("count")
    df["account_mean"] = grp.transform("mean")
    df["account_std"] = grp.transform("std")  # ddof=1 -> NaN for single-line accounts

    std = df["account_std"]
    valid = std.notna() & (std > 0) & df["netamount_f"].notna()
    df["z_score"] = np.where(valid, (df["netamount_f"] - df["account_mean"]) / std.where(valid, 1.0), np.nan)
    df["is_anomaly"] = df["z_score"].abs().gt(threshold).fillna(False).astype(bool)

    df["netamount"] = df["netamount_f"].map(to_decimal)
    df["account_mean"] = df["account_mean"].map(to_decimal)
    df["account_std"] = df["account_std"].map(to_decimal)
    out = df[[f.name for f in SCHEMA]].sort_values(["account", "transaction_id", "line_id"]).reset_index(drop=True)
    return out


def run(ctx: Context) -> int:
    txn = ctx.read_bronze("netsuite_transaction", ["id", "tranid", "type", "trandate", "posting", "lastmodifieddate",
                                                  "ingestion_timestamp"])
    lines = ctx.read_bronze(
        "netsuite_transactionline",
        ["id", "transaction", "linesequencenumber", "account", "department", "subsidiary", "netamount", "mainline",
         "linelastmodifieddate", "ingestion_timestamp"],
        where="mainline = 'F'")
    out = detect(txn, lines)
    log.info("%d detail lines over %d accounts; %d anomalies (|z| > %s)", len(out), out["account"].nunique() if len(out) else 0,
             int(out["is_anomaly"].sum()) if len(out) else 0, Z_THRESHOLD)
    return ctx.write_silver(out, SCHEMA)
