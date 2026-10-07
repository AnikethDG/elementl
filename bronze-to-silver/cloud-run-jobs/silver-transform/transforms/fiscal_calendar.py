"""dim_netsuite_fiscal_calendar: one row per calendar day derived from netsuite_accountingperiod.

Months (isquarter='F', isyear='F') link to quarters (isquarter='T') via ``parent`` and quarters
link to years (isyear='T'). Pure-Python date arithmetic (the reason this lives in the Cloud Run
engine rather than SQL).
"""
from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

from common import Context, field, latest_version, parse_ns_date, tf_to_bool

log = logging.getLogger("silver_transform.fiscal_calendar")

DEFAULT_TABLE = "dim_netsuite_fiscal_calendar"

SCHEMA = [
    field("date", "DATE", "REQUIRED", "Calendar day (primary key)"),
    field("period_id", "STRING", "REQUIRED", "netsuite_accountingperiod.id of the month"),
    field("period_name", "STRING"),
    field("period_start_date", "DATE"),
    field("period_end_date", "DATE"),
    field("quarter_id", "STRING"),
    field("quarter_name", "STRING"),
    field("fiscal_year_id", "STRING"),
    field("fiscal_year_name", "STRING"),
    field("is_period_closed", "BOOL"),
    field("day_of_period", "INT64", description="1-based day index inside the month period"),
    field("days_in_period", "INT64"),
    field("is_month_end", "BOOL"),
    field("is_quarter_end", "BOOL"),
    field("is_year_end", "BOOL"),
]


def build_calendar(periods: pd.DataFrame) -> pd.DataFrame:
    """Expand accounting periods (bronze-shaped, STRING columns) into one row per day."""
    if periods.empty:
        return pd.DataFrame(columns=[f.name for f in SCHEMA])
    p = latest_version(periods, "id")
    p = p.assign(
        start=p["startdate"].map(parse_ns_date),
        end=p["enddate"].map(parse_ns_date),
        is_quarter=p["isquarter"].map(tf_to_bool).fillna(False).astype(bool),
        is_year=p["isyear"].map(tf_to_bool).fillna(False).astype(bool),
        is_closed=p["closed"].map(tf_to_bool),
        id=p["id"].astype(str),
        parent=p["parent"].where(p["parent"].notna(), None),
    )
    by_id = {r.id: r for r in p.itertuples(index=False)}
    quarters = {r.id: r for r in p.itertuples(index=False) if r.is_quarter and not r.is_year}
    years = {r.id: r for r in p.itertuples(index=False) if r.is_year}
    months = [r for r in p.itertuples(index=False) if not r.is_quarter and not r.is_year]

    def resolve(month):
        """Return (quarter_row, year_row) walking ``parent`` links; tolerate missing links."""
        q = y = None
        node = by_id.get(month.parent) if month.parent else None
        hops = 0
        while node is not None and hops < 5:
            if node.is_year:
                y = node
                break
            if node.is_quarter and q is None:
                q = node
            node = by_id.get(node.parent) if node.parent else None
            hops += 1
        if q is None:  # fall back on date containment
            for cand in quarters.values():
                if cand.start and cand.end and cand.start <= month.start <= cand.end:
                    q = cand
                    break
        if y is None:
            for cand in years.values():
                if cand.start and cand.end and cand.start <= month.start <= cand.end:
                    y = cand
                    break
        return q, y

    rows = []
    for m in sorted(months, key=lambda r: (r.start or dt.date.max, r.id)):
        if m.start is None or m.end is None or m.end < m.start:
            log.warning("skipping period %s (%s): unparsable range %r-%r", m.id, m.periodname, m.startdate, m.enddate)
            continue
        q, y = resolve(m)
        days_in = (m.end - m.start).days + 1
        for i in range(days_in):
            d = m.start + dt.timedelta(days=i)
            rows.append({
                "date": d,
                "period_id": m.id,
                "period_name": m.periodname,
                "period_start_date": m.start,
                "period_end_date": m.end,
                "quarter_id": q.id if q else None,
                "quarter_name": q.periodname if q else None,
                "fiscal_year_id": y.id if y else None,
                "fiscal_year_name": y.periodname if y else None,
                "is_period_closed": bool(m.is_closed) if m.is_closed is not None else None,
                "day_of_period": i + 1,
                "days_in_period": days_in,
                "is_month_end": d == m.end,
                "is_quarter_end": bool(q and q.end == d),
                "is_year_end": bool(y and y.end == d),
            })
    cal = pd.DataFrame(rows)
    if cal.empty:
        return cal
    # a day can only belong to one month period; keep the first by start date if periods overlap
    dup = cal["date"].duplicated(keep="first")
    if dup.any():
        log.warning("%d days matched more than one month period; keeping the earliest period", int(dup.sum()))
        cal = cal[~dup]
    return cal.reset_index(drop=True)


def run(ctx: Context) -> int:
    periods = ctx.read_bronze("netsuite_accountingperiod")
    cal = build_calendar(periods)
    log.info("built %d calendar days from %d periods (%s..%s)", len(cal), len(periods),
             cal["date"].min() if len(cal) else None, cal["date"].max() if len(cal) else None)
    return ctx.write_silver(cal, SCHEMA)
