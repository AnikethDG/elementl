"""Offline unit tests for the silver-transform Cloud Run Job (no BigQuery access).

Run:  .venv/bin/python -m pytest tests/cloud_run -q
"""
from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

JOB_DIR = Path(__file__).resolve().parents[2] / "cloud-run-jobs" / "silver-transform"
sys.path.insert(0, str(JOB_DIR))

import common  # noqa: E402
from transforms import address_enrichment, budget_vs_actual, customer_domains, fiscal_calendar, transaction_anomalies  # noqa: E402

BASE_TS = pd.Timestamp("2026-10-01 06:00:00", tz="UTC")


# ----------------------------------------------------------------------------------------------
# pure helpers
# ----------------------------------------------------------------------------------------------
class TestHelpers:
    def test_parse_ns_date(self):
        assert common.parse_ns_date("1/5/2025") == dt.date(2025, 1, 5)
        assert common.parse_ns_date("12/31/2026") == dt.date(2026, 12, 31)
        assert common.parse_ns_date(None) is None
        assert common.parse_ns_date("not a date") is None
        assert common.parse_ns_date("2/30/2025") is None  # SAFE semantics

    def test_parse_ns_timestamp(self):
        ts = common.parse_ns_timestamp("10/1/2026 6:00:00 AM")
        assert ts == dt.datetime(2026, 10, 1, 6, 0, 0, tzinfo=dt.timezone.utc)
        assert common.parse_ns_timestamp("10/1/2026 12:30:15 PM").hour == 12
        assert common.parse_ns_timestamp("10/1/2026 12:30:15 AM").hour == 0
        assert common.parse_ns_timestamp("10/1/2026 11:30:15 PM").hour == 23
        assert common.parse_ns_timestamp("garbage") is None

    def test_tf_to_bool(self):
        assert common.tf_to_bool("T") is True
        assert common.tf_to_bool("F") is False
        assert common.tf_to_bool(" t ") is True
        assert common.tf_to_bool("anything") is False
        assert common.tf_to_bool(None) is None

    def test_to_decimal(self):
        assert common.to_decimal("1234.5") == Decimal("1234.500000000")
        assert common.to_decimal("-5.25") == Decimal("-5.250000000")
        assert common.to_decimal("abc") is None
        assert common.to_decimal(None) is None
        assert common.to_decimal(float("nan")) is None
        assert common.to_decimal(3) == Decimal("3.000000000")

    def test_latest_version_prefers_latest_ingestion_then_lastmodified(self):
        df = pd.DataFrame({
            "id": ["1", "1", "1", "2"],
            "val": ["old", "new", "tie-newer-lm", "only"],
            "lastmodifieddate": ["9/1/2026 1:00:00 AM", "9/2/2026 1:00:00 AM", "9/3/2026 1:00:00 AM", "9/1/2026 1:00:00 AM"],
            "ingestion_timestamp": [BASE_TS, BASE_TS + pd.Timedelta(hours=36), BASE_TS + pd.Timedelta(hours=36), BASE_TS],
        })
        out = common.latest_version(df, "id")
        assert len(out) == 2
        assert out.set_index("id").loc["1", "val"] == "tie-newer-lm"
        assert out.set_index("id").loc["2", "val"] == "only"
        assert list(out.columns) == list(df.columns)

    def test_prepare_for_load_adds_audit_cols_and_coerces(self):
        schema = [common.field("k", "STRING", "REQUIRED"), common.field("d", "DATE"), common.field("n", "NUMERIC"),
                  common.field("b", "BOOL"), common.field("i", "INT64"), common.field("missing", "STRING")]
        df = pd.DataFrame({"k": [1, 2], "d": ["1/2/2025", None], "n": [1.5, None], "b": [True, None], "i": [3.0, None],
                           "extra": ["x", "y"]})
        loaded_at = dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc)
        out = common.prepare_for_load(df, schema, loaded_at, "run-1")
        assert list(out.columns) == ["k", "d", "n", "b", "i", "missing", "_silver_loaded_at", "_silver_run_id"]
        assert out["k"].tolist() == ["1", "2"]
        assert out["d"].tolist() == [dt.date(2025, 1, 2), None]
        assert out["n"].tolist()[0] == Decimal("1.500000000") and out["n"].tolist()[1] is None
        assert out["b"].tolist() == [True, None]
        assert out["i"].tolist()[0] == 3 and pd.isna(out["i"].tolist()[1])
        assert out["missing"].isna().all()
        assert (out["_silver_run_id"] == "run-1").all()
        assert out["_silver_loaded_at"].dt.tz is not None


# ----------------------------------------------------------------------------------------------
# fiscal calendar
# ----------------------------------------------------------------------------------------------
def _periods(years=(2025, 2026)) -> pd.DataFrame:
    rows = []
    for yi, y in enumerate(years):
        rows.append(dict(id=str(300 + yi), periodname=f"FY {y}", startdate=f"1/1/{y}", enddate=f"12/31/{y}",
                         isquarter="F", isyear="T", closed="T" if yi == 0 else "F", parent=None))
        for q in range(4):
            qid = str(200 + yi * 4 + q)
            start_m = q * 3 + 1
            end_m = start_m + 2
            last = (dt.date(y + (end_m == 12), (end_m % 12) + 1, 1) - dt.timedelta(days=1)).day
            rows.append(dict(id=qid, periodname=f"Q{q + 1} {y}", startdate=f"{start_m}/1/{y}", enddate=f"{end_m}/{last}/{y}",
                             isquarter="T", isyear="F", closed="T" if y == 2025 else "F", parent=str(300 + yi)))
            for m in range(start_m, end_m + 1):
                nxt = dt.date(y + (m == 12), (m % 12) + 1, 1)
                last_d = (nxt - dt.timedelta(days=1)).day
                rows.append(dict(id=str(100 + yi * 12 + m - 1), periodname=dt.date(y, m, 1).strftime("%b %Y"),
                                 startdate=f"{m}/1/{y}", enddate=f"{m}/{last_d}/{y}", isquarter="F", isyear="F",
                                 closed="T" if dt.date(y, m, 1) < dt.date(2026, 7, 1) else "F", parent=qid))
    df = pd.DataFrame(rows)
    df["ingestion_date"] = BASE_TS.date()
    df["ingestion_timestamp"] = BASE_TS
    return df


class TestFiscalCalendar:
    def test_two_years_give_730_unique_days(self):
        cal = fiscal_calendar.build_calendar(_periods())
        assert len(cal) == 730
        assert cal["date"].is_unique
        assert cal["date"].min() == dt.date(2025, 1, 1) and cal["date"].max() == dt.date(2026, 12, 31)
        assert set(cal.columns) >= {f.name for f in fiscal_calendar.SCHEMA}

    def test_hierarchy_and_flags(self):
        cal = fiscal_calendar.build_calendar(_periods()).set_index("date")
        r = cal.loc[dt.date(2025, 2, 28)]
        assert r.period_name == "Feb 2025" and r.quarter_name == "Q1 2025" and r.fiscal_year_name == "FY 2025"
        assert r.day_of_period == 28 and r.days_in_period == 28 and bool(r.is_month_end) is True
        assert bool(r.is_period_closed) is True
        assert bool(cal.loc[dt.date(2025, 3, 31)].is_quarter_end) is True
        assert bool(cal.loc[dt.date(2026, 12, 31)].is_year_end) is True
        assert bool(cal.loc[dt.date(2026, 7, 15)].is_period_closed) is False
        assert cal.loc[dt.date(2026, 7, 15)].day_of_period == 15
        assert (cal.loc[[dt.date(2026, 2, 1)]].days_in_period == 28).all()

    def test_dedupes_period_versions(self):
        p = _periods()
        dup = p[p["id"] == "100"].copy()
        dup["periodname"] = "Jan 2025 v2"
        dup["ingestion_timestamp"] = BASE_TS + pd.Timedelta(hours=1)
        cal = fiscal_calendar.build_calendar(pd.concat([p, dup]))
        assert len(cal) == 730
        assert cal.set_index("date").loc[dt.date(2025, 1, 10)].period_name == "Jan 2025 v2"


# ----------------------------------------------------------------------------------------------
# address enrichment
# ----------------------------------------------------------------------------------------------
class TestAddress:
    @pytest.mark.parametrize("raw,expected", [
        ("7 main st", "7 Main Street"), ("14 OAK AVE", "14 Oak Avenue"), ("21 Elm Street", "21 Elm Street"),
        ("28 Pine Rd.", "28 Pine Road"), ("35 5th avenue", "35 5th Avenue"), (None, None), ("", None),
    ])
    def test_street(self, raw, expected):
        assert address_enrichment.standardise_street(raw) == expected

    def test_state_zip_country_region(self):
        a = address_enrichment
        assert a.normalise_state("texas") == "TX"
        assert a.normalise_state("tx") == "TX"
        assert a.normalise_state("New York") == "NY"
        assert a.normalise_state(None) is None
        assert a.split_zip("10037") == ("10037", None)
        assert a.split_zip("10407-0011") == ("10407", "0011")
        assert a.split_zip("N/A") == (None, None)
        assert a.split_zip("1234") == (None, None)
        assert a.normalise_country("United States") == "US"
        assert a.normalise_country("US") == "US"
        assert a.region_for("TX") == "West South Central"
        assert a.region_for("CO") == "Mountain"
        assert a.region_for("ZZ") == "Unknown"
        assert a.region_for(None) == "Unknown"

    def test_enrich_dataframe(self):
        src = pd.DataFrame({
            "addressinternalid": ["1", "2", "3"],
            "addr1": ["7 main st", "14 OAK AVE", "21 Elm Street"],
            "addr2": [None, "Suite 5", None],
            "city": ["austin", "DALLAS", "san antonio"],
            "state": ["texas", None, "TX"],
            "zip": ["10037", "10074-0002", "N/A"],
            "country": ["United States", "US", "US"],
            "addressee": ["A", "B", "C"],
            "ingestion_date": [BASE_TS.date()] * 3, "ingestion_timestamp": [BASE_TS] * 3,
        })
        out = address_enrichment.enrich(src).set_index("address_id")
        assert out.loc["1", "state"] == "TX" and out.loc["1", "city"] == "Austin" and out.loc["1", "country"] == "US"
        assert out.loc["1", "region"] == "West South Central" and bool(out.loc["1", "is_valid"]) is True
        assert out.loc["1", "validation_errors"] is None
        assert out.loc["2", "state"] is None and out.loc["2", "zip4"] == "0002" and bool(out.loc["2", "is_valid"]) is False
        assert "missing_state" in out.loc["2", "validation_errors"]
        assert out.loc["3", "zip5"] is None and bool(out.loc["3", "is_valid"]) is False
        assert out.loc["3", "validation_errors"] == "missing_zip"
        assert out.loc["3", "city"] == "San Antonio"


# ----------------------------------------------------------------------------------------------
# customer domains
# ----------------------------------------------------------------------------------------------
class TestCustomerDomains:
    def test_domain_helpers(self):
        c = customer_domains
        assert c.email_domain("Contact1@Gmail.com") == "gmail.com"
        assert c.email_domain(None) is None and c.email_domain("nope") is None
        assert c.domain_type("gmail.com") == "free" and c.domain_type("acme.com") == "corporate"
        assert c.normalise_company("Acme Power, Inc.") == "acme power"
        assert c.normalise_company("Globex Energy LLC") == "globex energy"

    def test_clustering(self):
        names = {"1": "acme power 0", "2": "acme power 1", "10": "zeta holdings", "3": "acme powerr 0", "4": ""}
        asg = customer_domains.cluster_companies(names)
        assert asg["1"] == "1" and asg["2"] == "1" and asg["3"] == "1"
        assert asg["10"] == "10" and asg["4"] == "4"

    def test_transform_latest_version_and_types(self):
        src = pd.DataFrame({
            "id": ["1", "1", "2"], "entityid": ["CUST0001"] * 2 + ["CUST0002"],
            "companyname": ["Acme Power 0", "Acme Power 0", "Acme Power 1"],
            "email": ["a@acme.com", "a.v2@gmail.com", "b@yahoo.com"], "phone": ["1", "1", "2"],
            "isinactive": ["F", "T", "F"], "creditlimit": ["5500", "6000", "7000.5"], "balance": ["-10", "-10", "5"],
            "currency": ["USD"] * 3, "datecreated": ["1/1/2025"] * 3,
            "lastmodifieddate": ["9/1/2026 1:00:00 AM", "9/2/2026 1:00:00 AM", "9/1/2026 1:00:00 AM"],
            "ingestion_date": [BASE_TS.date()] * 3,
            "ingestion_timestamp": [BASE_TS, BASE_TS + pd.Timedelta(hours=20), BASE_TS],
        })
        out = customer_domains.transform(src).set_index("customer_id")
        assert len(out) == 2
        assert out.loc["1", "email_domain"] == "gmail.com" and out.loc["1", "domain_type"] == "free"
        assert bool(out.loc["1", "isinactive"]) is True and out.loc["1", "creditlimit"] == Decimal("6000.000000000")
        assert out.loc["2", "creditlimit"] == Decimal("7000.500000000")
        assert out.loc["1", "company_cluster_id"] == "1" and out.loc["2", "company_cluster_id"] == "1"
        assert (out["company_cluster_size"] == 2).all()


# ----------------------------------------------------------------------------------------------
# transaction anomalies / budget vs actual
# ----------------------------------------------------------------------------------------------
def _txn_lines():
    txn = pd.DataFrame({
        "id": ["1", "1", "2", "3"], "tranid": ["INV-1", "INV-1b", "BILL-2", "JE-3"], "type": ["CustInvc"] * 4,
        "trandate": ["1/15/2025", "1/15/2025", "2/3/2025", "2/20/2025"], "posting": ["T", "T", "T", "F"],
        "lastmodifieddate": ["9/1/2026 1:00:00 AM"] * 4,
        "ingestion_timestamp": [BASE_TS, BASE_TS + pd.Timedelta(days=1), BASE_TS, BASE_TS],
    })
    lines = pd.DataFrame({
        "id": ["10", "11", "12", "13", "20", "21", "30", "31"],
        "transaction": ["1", "1", "1", "1", "2", "2", "3", "3"],
        "linesequencenumber": ["0", "1", "2", "3", "0", "1", "0", "1"],
        "account": ["5", "7", "7", "7", "5", "7", "5", "9"], "department": ["1"] * 8, "subsidiary": ["1"] * 8,
        "netamount": ["-100", "10", "12", "11", "-50", "1000", "-5", "5"],
        "mainline": ["T", "F", "F", "F", "T", "F", "T", "F"],
        "linelastmodifieddate": ["9/1/2026 1:00:00 AM"] * 8,
        "ingestion_timestamp": [BASE_TS] * 8,
    })
    return txn, lines


class TestAnomalies:
    def test_zscores(self):
        txn, lines = _txn_lines()
        out = transaction_anomalies.detect(txn, lines).set_index("line_id")
        assert set(out.index) == {"11", "12", "13", "21", "31"}  # detail lines only
        assert out.loc["11", "tranid"] == "INV-1b"  # latest txn version
        assert out.loc["11", "trandate"] == dt.date(2025, 1, 15)
        acct7 = out[out["account"] == "7"]
        assert (acct7["account_line_count"] == 4).all()
        assert out.loc["21", "z_score"] > 1.4 and out.loc["21", "z_score"] == acct7["z_score"].max()
        # single-line account -> std NULL -> z NULL, not anomaly
        assert out.loc["31", "account_std"] is None and pd.isna(out.loc["31", "z_score"]) and bool(out.loc["31", "is_anomaly"]) is False
        assert out["is_anomaly"].dtype == bool
        assert isinstance(out.loc["21", "netamount"], Decimal)

    def test_std_zero_guard(self):
        txn, lines = _txn_lines()
        lines.loc[lines["account"] == "7", "netamount"] = "10"
        out = transaction_anomalies.detect(txn, lines)
        acct7 = out[out["account"] == "7"]
        assert acct7["z_score"].isna().all() and not acct7["is_anomaly"].any()


class TestBudgetVsActual:
    def test_unpivot_and_compare(self):
        budgets = pd.DataFrame({
            "id": ["1"], "account": ["7"], "department": ["1"], "subsidiary": ["1"], "year": ["2025"], "category": ["Operating"],
            **{f"amount_{i}": ["30"] for i in range(1, 12)}, "amount_12": [None],
            "lastmodifieddate": ["9/1/2026 1:00:00 AM"], "ingestion_date": [BASE_TS.date()], "ingestion_timestamp": [BASE_TS],
        })
        long = budget_vs_actual.unpivot_budgets(budgets)
        assert len(long) == 11 and set(long["month"]) == set(range(1, 12))
        txn, lines = _txn_lines()
        actuals = budget_vs_actual.aggregate_actuals(txn, lines)
        # account 7: Jan 2025 = 10+12+11 = 33 (txn 1), Feb 2025 = 1000 (txn 2); txn 3 non-posting excluded
        a = actuals.set_index(["account", "department", "year", "month"])
        assert a.loc[("7", "1", 2025, 1), "actual_amount"] == 33
        assert a.loc[("7", "1", 2025, 2), "actual_amount"] == 1000
        assert ("9", "1", 2025, 2) not in a.index
        out = budget_vs_actual.compare(long, actuals).set_index(["account", "department", "year", "month"])
        jan = out.loc[("7", "1", 2025, 1)]
        assert jan.budget_amount == Decimal("30.000000000") and jan.actual_amount == Decimal("33.000000000")
        assert jan.variance == Decimal("3.000000000") and jan.variance_pct == pytest.approx(0.1) and jan.status == "over"
        mar = out.loc[("7", "1", 2025, 3)]
        assert mar.actual_amount == Decimal("0E-9") and mar.status == "under" and bool(mar.has_actual) is False
        assert len(out) == 11  # all actual keys (Jan, Feb) are budgeted months -> no extra rows
        assert out.loc[("7", "1", 2025, 2)].status == "over"

    def test_zero_budget_gives_null_pct(self):
        long = pd.DataFrame({"account": ["1"], "department": ["1"], "year": [2025], "month": [1], "budget_amount": [0.0]})
        actuals = pd.DataFrame({"account": ["1"], "department": ["1"], "year": [2025], "month": [1], "actual_amount": [5.0],
                                "actual_line_count": [1]})
        out = budget_vs_actual.compare(long, actuals)
        assert pd.isna(out.loc[0, "variance_pct"]) and out.loc[0, "status"] == "over"


# ----------------------------------------------------------------------------------------------
# main.py env contract with a mocked BigQuery client
# ----------------------------------------------------------------------------------------------
class TestMain:
    def test_run_with_mocked_bigquery(self, monkeypatch, capsys):
        import importlib

        main = importlib.import_module("main")
        env = {"TRANSFORM": "fiscal_calendar", "GCP_PROJECT": "p", "BRONZE_DATASET": "b", "SILVER_DATASET": "s",
               "OUTPUT_TABLE": "p.s.dim_netsuite_fiscal_calendar", "EXECUTION_DATE": "2026-10-07", "RUN_ID": "unit",
               "PARAMS": '{"a": 1}'}
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        periods = _periods()
        fake_client = mock.MagicMock()
        fake_client.query.return_value.result.return_value.to_dataframe.return_value = periods
        fake_client.load_table_from_dataframe.return_value.errors = None
        fake_client.load_table_from_dataframe.return_value.output_rows = 730
        with mock.patch("google.cloud.bigquery.Client", return_value=fake_client):
            rc = main.main()
        assert rc == 0
        out = capsys.readouterr().out
        assert '"status": "success"' in out and '"rows_written": 730' in out
        df_loaded, table = fake_client.load_table_from_dataframe.call_args.args[:2]
        assert table == "p.s.dim_netsuite_fiscal_calendar"
        assert len(df_loaded) == 730 and (df_loaded["_silver_run_id"] == "unit").all()
        job_config = fake_client.load_table_from_dataframe.call_args.kwargs["job_config"]
        assert job_config.write_disposition == "WRITE_TRUNCATE" and job_config.autodetect is False
        assert [f.name for f in job_config.schema][-2:] == ["_silver_loaded_at", "_silver_run_id"]

    def test_missing_transform_exits_nonzero(self, monkeypatch):
        import importlib

        main = importlib.import_module("main")
        monkeypatch.delenv("TRANSFORM", raising=False)
        monkeypatch.setenv("GCP_PROJECT", "p")
        assert main.main() == 2

    def test_bad_params_exits_nonzero(self, monkeypatch):
        import importlib

        main = importlib.import_module("main")
        monkeypatch.setenv("TRANSFORM", "fiscal_calendar")
        monkeypatch.setenv("GCP_PROJECT", "p")
        monkeypatch.setenv("PARAMS", "{not json")
        assert main.main() == 2
