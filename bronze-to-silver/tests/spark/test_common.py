"""Offline unit tests for spark/lib/udp_silver_common.py.

Runs without pyspark: covers the pure-Python helpers, regexes and CLI parsing. When pyspark IS
importable, an extra local-session test checks that the Spark column helpers agree with the pure
Python ones (skipped otherwise).

    python3 -m pytest tests/spark -q
"""

import datetime as dt
import pathlib
import sys

import pytest

LIB = pathlib.Path(__file__).resolve().parents[2] / "spark" / "lib"
sys.path.insert(0, str(LIB))

import udp_silver_common as c  # noqa: E402


# ---------------------------------------------------------------- dates / timestamps
@pytest.mark.parametrize("raw, expected", [
    ("2/5/2025", dt.date(2025, 2, 5)),
    ("12/31/2026", dt.date(2026, 12, 31)),
    (" 1/1/2025 ", dt.date(2025, 1, 1)),
    ("02/05/2025", dt.date(2025, 2, 5)),
    ("2025-02-05", None),      # ISO is not the NetSuite format -> NULL (SAFE semantics)
    ("13/40/2025", None),
    ("", None),
    (None, None),
])
def test_parse_ns_date(raw, expected):
    assert c.parse_ns_date_py(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("9/26/2026 6:00:00 AM", dt.datetime(2026, 9, 26, 6, 0, 0)),
    ("9/26/2026 6:00:00 PM", dt.datetime(2026, 9, 26, 18, 0, 0)),
    ("12/1/2025 12:15:30 AM", dt.datetime(2025, 12, 1, 0, 15, 30)),
    ("12/1/2025  12:15:30   PM", dt.datetime(2025, 12, 1, 12, 15, 30)),  # whitespace collapsed
    ("9/26/2026", None),
    ("garbage", None),
    (None, None),
])
def test_parse_ns_timestamp(raw, expected):
    assert c.parse_ns_timestamp_py(raw) == expected


# ---------------------------------------------------------------- booleans
@pytest.mark.parametrize("raw, expected", [
    ("T", True), ("F", False), ("t", True), (" T ", True), ("Y", False), ("", None), (None, None),
])
def test_tf_to_bool(raw, expected):
    assert c.tf_to_bool_py(raw) is expected


# ---------------------------------------------------------------- hashtags
@pytest.mark.parametrize("text, expected", [
    ("Item 1 #solar #module", ["solar", "module"]),
    ("Item 3 #battery #storage #lithium #clearance", ["battery", "storage", "lithium", "clearance"]),
    ("Item 6 ", []),
    ("no tags here", []),
    ("#a#b c#d", ["a", "b", "d"]),
    ("", []),
    (None, []),
])
def test_extract_hashtags(text, expected):
    assert c.extract_hashtags_py(text) == expected


def test_hashtag_regex_constant_is_group_based():
    import re
    assert re.findall(c.HASHTAG_REGEX, "#X_1 #y") == ["X_1", "y"]


# ---------------------------------------------------------------- strings / PII
@pytest.mark.parametrize("raw, expected", [
    ("  solar module 7", "Solar Module 7"),
    ("INVERTER  5", "Inverter 5"),
    (" cable  kit 3", "Cable Kit 3"),
    ("Mounting Rail 9", "Mounting Rail 9"),
    ("   ", None),
    (None, None),
])
def test_normalise_name(raw, expected):
    assert c.normalise_name_py(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("+1-212-555-0199", "***-***-0199"),
    ("(200) 555-0007", "***-***-0007"),
    ("123", None),
    (None, None),
])
def test_mask_phone(raw, expected):
    assert c.mask_phone_py(raw) == expected


def test_initials():
    assert c.initials_py("Ana", "Silva") == "AS"
    assert c.initials_py(" ben", None) == "B"
    assert c.initials_py(None, None) is None


# ---------------------------------------------------------------- CLI contract
def test_parse_args_contract():
    args = c.parse_args([
        "--project", "p", "--bronze-dataset", "b", "--silver-dataset", "s",
        "--output-table", "p.s.stg_x", "--ds", "2026-10-07", "--run-id", "manual__1",
        "--params", '{"base_currency": "USD", "max_levels": 10}',
    ])
    assert (args.project, args.bronze_dataset, args.silver_dataset) == ("p", "b", "s")
    assert args.output_table == "p.s.stg_x"
    assert args.ds == "2026-10-07"
    assert args.run_id == "manual__1"
    assert args.params == {"base_currency": "USD", "max_levels": 10}


def test_parse_args_defaults_params_and_validates():
    base = ["--project", "p", "--bronze-dataset", "b", "--silver-dataset", "s",
            "--output-table", "p.s.t", "--run-id", "r"]
    assert c.parse_args(base + ["--ds", "2026-01-01"]).params == {}
    with pytest.raises(ValueError):
        c.parse_args(base + ["--ds", "01/01/2026"])
    with pytest.raises(SystemExit):
        c.parse_args(base + ["--ds", "2026-01-01", "--output-table", "not_qualified"])
    with pytest.raises(ValueError):
        c.parse_params("[1,2]")


# ---------------------------------------------------------------- Spark parity (optional)
@pytest.mark.skipif(not c.HAS_PYSPARK, reason="pyspark not installed; Spark helpers verified on Dataproc")
def test_spark_helpers_match_python():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("udp-silver-tests")
             .config("spark.sql.ansi.enabled", "false").config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    rows = [("2/5/2025", "9/26/2026 6:00:00 PM", "T", "Item #solar #Module", "  solar  module 7", "+1-212-555-0199"),
            ("bad", "bad", "F", "none", "INVERTER 5", "12"),
            (None, None, None, None, None, None)]
    df = spark.createDataFrame(rows, ["d", "ts", "b", "desc", "name", "phone"])
    got = df.select(c.parse_ns_date("d").alias("d"), c.parse_ns_timestamp("ts").alias("ts"),
                    c.tf_to_bool("b").alias("b"), c.extract_hashtags("desc").alias("tags"),
                    c.normalise_name("name").alias("name"), c.mask_phone("phone").alias("phone")).collect()
    for src, out in zip(rows, got):
        assert out.d == c.parse_ns_date_py(src[0])
        assert out.ts == c.parse_ns_timestamp_py(src[1])
        assert out.b == c.tf_to_bool_py(src[2])
        assert out.tags == c.extract_hashtags_py(src[3])
        assert out.name == c.normalise_name_py(src[4])
        assert out.phone == c.mask_phone_py(src[5])
    spark.stop()
