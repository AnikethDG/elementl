"""Offline tests for bigquery/render.py and the SQL scripts' placeholder discipline.

Run: python3 -m pytest -q tests/bigquery
"""
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bigquery"))

from render import render_sql  # noqa: E402

SQL_DIR = ROOT / "bigquery" / "sql"
CTX = {
    "project": "p",
    "bronze_dataset": "b",
    "silver_dataset": "s",
    "output": "p.s.out",
    "ds": "2026-01-01",
    "run_id": "r",
}
ALLOWED = set(CTX) | {"params.lookback_days"}


def test_render_substitutes_context_and_params():
    out = render_sql("SELECT '${project}', '${ds}', ${params.n} FROM `${output}`", CTX, {"n": 3})
    assert out == "SELECT 'p', '2026-01-01', 3 FROM `p.s.out`"


def test_render_fails_on_unknown_placeholder():
    with pytest.raises(KeyError, match="nope"):
        render_sql("SELECT ${nope}", CTX)


def test_render_fails_on_missing_param():
    with pytest.raises(KeyError, match="params.lookback_days"):
        render_sql("SELECT ${params.lookback_days}", CTX, {})


@pytest.mark.parametrize("sql_file", sorted(SQL_DIR.glob("*.sql")), ids=lambda p: p.stem)
def test_scripts_only_use_contract_placeholders(sql_file):
    used = set(re.findall(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*)\}", sql_file.read_text()))
    assert used <= ALLOWED, f"{sql_file.name} uses non-contract placeholders: {used - ALLOWED}"
    assert "output" in used, f"{sql_file.name} must write to ${{output}}"


@pytest.mark.parametrize("sql_file", sorted(SQL_DIR.glob("*.sql")), ids=lambda p: p.stem)
def test_scripts_render_without_jinja(sql_file):
    rendered = render_sql(sql_file.read_text(), CTX, {"lookback_days": 2})
    assert "${" not in rendered
    assert "{{" not in rendered, "Airflow Jinja syntax is not allowed in engine SQL"
    assert "_silver_run_id" in rendered and "_silver_loaded_at" in rendered


def test_declare_statements_come_first():
    """BigQuery scripting rejects DECLARE after any other statement."""
    for sql_file in SQL_DIR.glob("*.sql"):
        stmts = [s.strip() for s in re.sub(r"--[^\n]*", "", sql_file.read_text()).split(";") if s.strip()]
        seen_non_declare = False
        for stmt in stmts:
            if stmt.upper().startswith("DECLARE"):
                assert not seen_non_declare, f"{sql_file.name}: DECLARE after another statement"
            else:
                seen_non_declare = True
