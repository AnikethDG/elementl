"""Shared helpers for the silver-transform Cloud Run Job.

Every transform module receives a :class:`Context` and returns the number of rows written.
Conventions implemented here (see docs/CONVENTIONS.md):

* bronze is all-STRING; helpers convert 'T'/'F', M/D/YYYY, M/D/YYYY H:MM:SS AM and numeric strings
* ``latest_version`` dedupes bronze versions: latest ``ingestion_timestamp`` wins,
  tie-break on parsed ``lastmodifieddate``
* ``write_silver`` appends ``_silver_loaded_at`` / ``_silver_run_id`` and loads with an explicit
  schema using WRITE_TRUNCATE (full rebuild, idempotent) unless the caller asks otherwise
"""
from __future__ import annotations

import datetime as dt
import logging
import math
import re
from dataclasses import dataclass, field as dc_field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

try:  # bigquery is optional at import time so the pure helpers are testable offline
    from google.cloud import bigquery
except ImportError:  # pragma: no cover
    bigquery = None  # type: ignore

log = logging.getLogger("silver_transform")

SILVER_AUDIT_COLUMNS = ("_silver_loaded_at", "_silver_run_id")

_NS_DATE_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")
_NS_TS_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})\s*([AaPp][Mm])\s*$")


# ----------------------------------------------------------------------------------------------
# scalar converters
# ----------------------------------------------------------------------------------------------
def _is_null(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        return bool(pd.isna(v)) if not isinstance(v, (str, bytes, list, dict, tuple)) else False
    except (TypeError, ValueError):
        return False


def parse_ns_date(value: Any) -> dt.date | None:
    """``'M/D/YYYY'`` -> ``datetime.date`` (SAFE: returns None on anything unparsable)."""
    if _is_null(value):
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    m = _NS_DATE_RE.match(str(value))
    if not m:
        return None
    mo, d, y = (int(x) for x in m.groups())
    try:
        return dt.date(y, mo, d)
    except ValueError:
        return None


def parse_ns_timestamp(value: Any) -> dt.datetime | None:
    """``'M/D/YYYY H:MM:SS AM'`` -> tz-aware UTC ``datetime`` (None when unparsable)."""
    if _is_null(value):
        return None
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    m = _NS_TS_RE.match(str(value))
    if not m:
        d = parse_ns_date(value)
        return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc) if d else None
    mo, d, y, hh, mi, ss, ampm = m.groups()
    hour = int(hh) % 12
    if ampm.upper() == "PM":
        hour += 12
    try:
        return dt.datetime(int(y), int(mo), int(d), hour, int(mi), int(ss), tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def tf_to_bool(value: Any) -> bool | None:
    """NetSuite ``'T'``/``'F'`` -> bool; NULL stays NULL (``col = 'T'`` semantics otherwise)."""
    if _is_null(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return str(value).strip().upper() == "T"


def to_decimal(value: Any, scale: int = 9) -> Decimal | None:
    """numeric string / float -> ``Decimal`` suitable for a BigQuery NUMERIC column (SAFE_CAST)."""
    if _is_null(value):
        return None
    try:
        if isinstance(value, Decimal):
            d = value
        elif isinstance(value, (int, np.integer)):
            d = Decimal(int(value))
        elif isinstance(value, (float, np.floating)):
            if not math.isfinite(float(value)):
                return None
            d = Decimal(repr(float(value)))
        else:
            d = Decimal(str(value).strip().replace(",", ""))
        if not d.is_finite():
            return None
        return d.quantize(Decimal(1).scaleb(-scale))
    except (InvalidOperation, ValueError, TypeError):
        return None


def to_float(value: Any) -> float | None:
    if _is_null(value):
        return None
    try:
        f = float(str(value).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None
    return f if math.isfinite(f) else None


def to_int(value: Any) -> int | None:
    f = to_float(value)
    return None if f is None else int(f)


# ----------------------------------------------------------------------------------------------
# dataframe helpers
# ----------------------------------------------------------------------------------------------
def series_numeric(s: pd.Series) -> pd.Series:
    """Vectorised ``SAFE_CAST(col AS FLOAT64)`` for a STRING series (NaN when unparsable)."""
    return pd.to_numeric(s.astype("string").str.strip().str.replace(",", "", regex=False), errors="coerce").astype(float)


def series_bool(s: pd.Series) -> pd.Series:
    return s.map(tf_to_bool).astype(object)


def series_date(s: pd.Series) -> pd.Series:
    return s.map(parse_ns_date).astype(object)


def series_timestamp(s: pd.Series) -> pd.Series:
    return s.map(parse_ns_timestamp).astype(object)


def latest_version(
    df: pd.DataFrame,
    key_cols: Sequence[str] | str,
    ts_col: str = "ingestion_timestamp",
    tiebreak_col: str | None = "lastmodifieddate",
) -> pd.DataFrame:
    """Keep the latest bronze version per business key.

    Latest ``ingestion_timestamp`` wins; ties are broken on the parsed ``lastmodifieddate``
    (when the column exists). The result has a fresh RangeIndex and the original column set.
    """
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    key_cols = list(key_cols)
    if df.empty:
        return df.copy()
    work = df.copy()
    sort_cols = []
    if ts_col in work.columns:
        work["__ts"] = pd.to_datetime(work[ts_col], utc=True, errors="coerce")
        sort_cols.append("__ts")
    if tiebreak_col and tiebreak_col in work.columns:
        work["__lm"] = work[tiebreak_col].map(parse_ns_timestamp)
        work["__lm"] = pd.to_datetime(work["__lm"], utc=True, errors="coerce")
        sort_cols.append("__lm")
    if sort_cols:
        work = work.sort_values(sort_cols, kind="mergesort", na_position="first")
    work = work.drop_duplicates(subset=key_cols, keep="last")
    return work.drop(columns=[c for c in ("__ts", "__lm") if c in work.columns]).reset_index(drop=True)


# ----------------------------------------------------------------------------------------------
# BigQuery schema helpers
# ----------------------------------------------------------------------------------------------
def field(name: str, type_: str, mode: str = "NULLABLE", description: str | None = None):
    """Shorthand for ``bigquery.SchemaField`` (works without bigquery installed for tests)."""
    if bigquery is None:  # pragma: no cover
        return {"name": name, "type": type_, "mode": mode, "description": description}
    return bigquery.SchemaField(name, type_, mode=mode, description=description or None)


def audit_fields():
    return [
        field("_silver_loaded_at", "TIMESTAMP", "REQUIRED", "Silver load time (job start)"),
        field("_silver_run_id", "STRING", "REQUIRED", "Airflow run_id / job execution id"),
    ]


def _field_attr(f: Any, attr: str) -> str:
    return getattr(f, attr) if not isinstance(f, dict) else f[attr]


def _coerce_column(s: pd.Series, bq_type: str) -> pd.Series:
    """Coerce a pandas column to python objects pyarrow can map 1:1 onto the BigQuery type."""
    t = bq_type.upper()
    if t in ("NUMERIC", "BIGNUMERIC", "DECIMAL"):
        return s.map(to_decimal).astype(object)
    if t in ("FLOAT", "FLOAT64"):
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if t in ("INTEGER", "INT64"):
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if t in ("BOOLEAN", "BOOL"):
        return s.map(lambda v: None if _is_null(v) else bool(v)).astype(object)
    if t == "DATE":
        return s.map(parse_ns_date).astype(object)
    if t == "TIMESTAMP":
        return pd.to_datetime(s.map(parse_ns_timestamp), utc=True, errors="coerce")
    if t == "DATETIME":
        return pd.to_datetime(s, errors="coerce")
    # STRING / JSON / default
    return s.map(lambda v: None if _is_null(v) else str(v)).astype(object)


def prepare_for_load(df: pd.DataFrame, schema: Sequence[Any], loaded_at: dt.datetime, run_id: str) -> pd.DataFrame:
    """Return a copy of ``df`` restricted/ordered to ``schema`` with audit columns added.

    Columns in the schema that are missing from ``df`` are filled with NULL so the explicit
    BigQuery schema is always satisfied; extra df columns are dropped.
    """
    out = pd.DataFrame(index=df.index)
    names = [_field_attr(f, "name") for f in schema]
    if "_silver_loaded_at" not in names:
        schema = list(schema) + audit_fields()
        names = [_field_attr(f, "name") for f in schema]
    work = df.copy()
    work["_silver_loaded_at"] = loaded_at
    work["_silver_run_id"] = run_id
    for f in schema:
        name, type_ = _field_attr(f, "name"), _field_attr(f, "field_type" if not isinstance(f, dict) else "type")
        src = work[name] if name in work.columns else pd.Series([None] * len(work), index=work.index, dtype=object)
        out[name] = _coerce_column(src, type_)
    return out.reset_index(drop=True)


# ----------------------------------------------------------------------------------------------
# context (what every transform receives)
# ----------------------------------------------------------------------------------------------
@dataclass
class Context:
    project: str
    bronze_dataset: str
    silver_dataset: str
    output_table: str
    ds: str
    run_id: str
    params: dict = dc_field(default_factory=dict)
    client: Any = None
    transform: str = ""
    loaded_at: dt.datetime = dc_field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    # -- naming -------------------------------------------------------------------------------
    def bronze(self, table: str) -> str:
        return f"{self.project}.{self.bronze_dataset}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.project}.{self.silver_dataset}.{table}"

    @property
    def ds_date(self) -> dt.date:
        return dt.date.fromisoformat(self.ds)

    # -- io -----------------------------------------------------------------------------------
    def read_table(self, sql_or_table: str) -> pd.DataFrame:
        """Run a query (or ``SELECT *`` from a table given as ``project.dataset.table``)."""
        return read_table(self.client, sql_or_table, project=self.project)

    def read_bronze(self, table: str, columns: Iterable[str] | None = None, where: str | None = None) -> pd.DataFrame:
        cols = ", ".join(f"`{c}`" for c in columns) if columns else "*"
        sql = f"SELECT {cols} FROM `{self.bronze(table)}`"
        if where:
            sql += f" WHERE {where}"
        return self.read_table(sql)

    def write_silver(self, df: pd.DataFrame, schema: Sequence[Any], output_table: str | None = None,
                     write_disposition: str = "WRITE_TRUNCATE") -> int:
        return write_silver(self.client, df, output_table or self.output_table, schema,
                            write_disposition=write_disposition, run_id=self.run_id, loaded_at=self.loaded_at)


def read_table(client, sql_or_table: str, project: str | None = None) -> pd.DataFrame:
    """``SELECT`` text -> DataFrame; a bare ``project.dataset.table`` is wrapped in ``SELECT *``."""
    text = sql_or_table.strip()
    if re.match(r"^[\w\-]+\.[\w]+\.[\w]+$", text):
        text = f"SELECT * FROM `{text}`"
    log.info("bq query: %s", " ".join(text.split())[:300])
    job = client.query(text, project=project)
    # bqstorage client is deliberately disabled: data is small and it avoids needing
    # bigquery.readsessions.create on the job SA.
    df = job.result().to_dataframe(create_bqstorage_client=False)
    log.info("bq query returned %d rows x %d cols (job %s, %s bytes billed)", len(df), len(df.columns),
             job.job_id, job.total_bytes_billed)
    return df


def write_silver(client, df: pd.DataFrame, output_table: str, schema: Sequence[Any], *,
                 write_disposition: str = "WRITE_TRUNCATE", run_id: str = "", loaded_at: dt.datetime | None = None) -> int:
    """Load ``df`` into ``output_table`` with an explicit schema (never autodetect).

    WRITE_TRUNCATE gives the full-rebuild idempotency required by CONVENTIONS.md; pass
    ``write_disposition='WRITE_APPEND'`` for incremental transforms (they must MERGE/partition
    themselves).
    """
    loaded_at = loaded_at or dt.datetime.now(dt.timezone.utc)
    full_schema = list(schema)
    if "_silver_loaded_at" not in [_field_attr(f, "name") for f in full_schema]:
        full_schema += audit_fields()
    prepared = prepare_for_load(df, full_schema, loaded_at, run_id)
    job_config = bigquery.LoadJobConfig(
        schema=full_schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=False,
    )
    if write_disposition == "WRITE_TRUNCATE":
        # make sure a schema change in the transform is applied on rebuild
        job_config.schema_update_options = None
    log.info("loading %d rows into %s (%s)", len(prepared), output_table, write_disposition)
    load_job = client.load_table_from_dataframe(prepared, output_table, job_config=job_config)
    load_job.result()
    if load_job.errors:
        raise RuntimeError(f"load job {load_job.job_id} failed: {load_job.errors}")
    table = client.get_table(output_table)
    log.info("loaded %s: now %s rows (load job %s)", output_table, table.num_rows, load_job.job_id)
    return int(load_job.output_rows or len(prepared))
