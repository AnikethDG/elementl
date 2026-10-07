"""Offline structural tests for the bronze-to-silver Dataform project.

No Dataform CLI / network needed: the SQLX files are parsed with regexes and the
workflow_settings.yaml with a tiny YAML reader (PyYAML is used when available).

Run:  .venv/bin/python -m pytest tests/dataform -q
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DATAFORM_DIR = ROOT / "dataform"
MODELS_DIR = DATAFORM_DIR / "definitions" / "01_silver" / "netsuite"
SOURCES_DIR = DATAFORM_DIR / "definitions" / "00_sources"
SETTINGS = DATAFORM_DIR / "workflow_settings.yaml"

EXPECTED_MODELS = {
    "stg_netsuite_account": "table",
    "stg_netsuite_transaction": "incremental",
    "fct_netsuite_transaction_lines": "table",
    "agg_netsuite_posting_daily": "table",
    "stg_netsuite_budgets_monthly": "table",
}
REQUIRED_VARS = {"udp_project", "bronze_dataset", "silver_dataset"}
BRONZE_REF_RE = re.compile(
    r"\bref\(\s*dataform\.projectConfig\.vars\.bronze_dataset\s*,\s*\"(netsuite_\w+)\"\s*\)"
)
SILVER_REF_RE = re.compile(r"\bref\(\s*\"(\w+)\"\s*\)")


def _load_settings() -> dict:
    text = SETTINGS.read_text()
    try:
        import yaml  # type: ignore

        return yaml.safe_load(text)
    except ImportError:  # minimal 2-level reader: `key: value` and one nested `vars:` block
        out: dict = {}
        section = None
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            if not line.startswith(" "):
                key, _, val = line.partition(":")
                if val.strip():
                    out[key.strip()] = val.strip().strip('"')
                    section = None
                else:
                    section = key.strip()
                    out[section] = {}
            elif section:
                key, _, val = line.strip().partition(":")
                out[section][key.strip()] = val.strip().strip('"')
        return out


def _config_block(sqlx: str) -> str:
    m = re.search(r"config\s*\{(.*?)\n\}", sqlx, re.S)
    assert m, "no config {} block"
    return m.group(1)


def _tags(config: str) -> list[str]:
    m = re.search(r"tags\s*:\s*\[(.*?)\]", config, re.S)
    return re.findall(r"\"([^\"]+)\"", m.group(1)) if m else []


def _models() -> dict[str, str]:
    return {p.stem: p.read_text() for p in sorted(MODELS_DIR.glob("*.sqlx"))}


@pytest.fixture(scope="module")
def models() -> dict[str, str]:
    found = _models()
    assert set(found) == set(EXPECTED_MODELS), f"models on disk: {sorted(found)}"
    return found


@pytest.fixture(scope="module")
def settings() -> dict:
    return _load_settings()


# --- workflow_settings.yaml ---------------------------------------------------------------------

def test_settings_core_fields(settings):
    assert settings["defaultProject"] == "ani-labs"
    assert settings["defaultLocation"] == "us-central1"
    assert settings["defaultDataset"] == "elementl_silver"
    assert settings["defaultAssertionDataset"] == "elementl_silver_assertions"
    assert str(settings["dataformCoreVersion"]).startswith("3.")


def test_settings_vars(settings):
    vars_ = settings.get("vars") or {}
    assert REQUIRED_VARS <= set(vars_), f"missing vars: {REQUIRED_VARS - set(vars_)}"
    assert vars_["bronze_dataset"] == "elementl_bronze"
    assert vars_["silver_dataset"] == "elementl_silver"


# --- per-model structure ------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_type(models, name):
    cfg = _config_block(models[name])
    m = re.search(r"type\s*:\s*\"(\w+)\"", cfg)
    assert m and m.group(1) == EXPECTED_MODELS[name]


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_tags(models, name):
    tags = _tags(_config_block(models[name]))
    assert {"netsuite", "silver", name} <= set(tags), f"{name}: tags={tags}"


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_name_matches_file(models, name):
    cfg = _config_block(models[name])
    m = re.search(r"\bname\s*:\s*\"(\w+)\"", cfg)
    assert m and m.group(1) == name


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_database_schema_from_vars(models, name):
    cfg = _config_block(models[name])
    assert "database: dataform.projectConfig.vars.udp_project" in cfg
    assert "schema: dataform.projectConfig.vars.silver_dataset" in cfg


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_has_assertions(models, name):
    cfg = _config_block(models[name])
    assert re.search(r"assertions\s*:\s*\{", cfg), f"{name}: no assertions block"
    assert "uniqueKey" in cfg, f"{name}: no uniqueKey assertion"


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_audit_columns(models, name):
    body = models[name]
    assert "netsuite.audit_columns()" in body or (
        "_silver_loaded_at" in body and "_silver_run_id" in body
    ), f"{name}: missing _silver_loaded_at/_silver_run_id"


@pytest.mark.parametrize("name", sorted(EXPECTED_MODELS))
def test_model_no_hardcoded_bronze(models, name):
    body = models[name]
    assert "elementl_bronze" not in body, f"{name}: hardcoded bronze dataset"
    assert "`ani-labs." not in body, f"{name}: hardcoded project"
    assert "{{" not in body, f"{name}: Jinja braces are not allowed (CONVENTIONS.md)"


def test_models_reference_bronze_via_vars_or_silver_ref(models):
    for name, body in models.items():
        bronze_refs = BRONZE_REF_RE.findall(body)
        silver_refs = SILVER_REF_RE.findall(body)
        assert bronze_refs or silver_refs, f"{name}: no ${{ref(...)}} at all"
        for s in silver_refs:
            assert s in EXPECTED_MODELS, f"{name}: ref to unknown silver model {s}"


def test_every_bronze_ref_has_declaration(models):
    declared = set()
    for p in SOURCES_DIR.glob("*.sqlx"):
        cfg = _config_block(p.read_text())
        assert 'type: "declaration"' in cfg
        assert "schema: dataform.projectConfig.vars.bronze_dataset" in cfg
        declared.add(re.search(r"\bname\s*:\s*\"(\w+)\"", cfg).group(1))
    referenced = {t for body in models.values() for t in BRONZE_REF_RE.findall(body)}
    assert referenced, "no bronze refs found"
    assert referenced <= declared, f"undeclared bronze tables: {referenced - declared}"


# --- model-specific rules -----------------------------------------------------------------------

def test_incremental_transaction_watermark(models):
    body = models["stg_netsuite_transaction"]
    cfg = _config_block(body)
    assert re.search(r"uniqueKey\s*:\s*\[\s*\"id\"\s*\]", cfg)
    assert "when(incremental()" in body
    assert "MAX(_bronze_ingested_at)" in body
    assert "ingestion_timestamp AS _bronze_ingested_at" in body
    assert "${self()}" in body


def test_fact_partitioning(models):
    cfg = _config_block(models["fct_netsuite_transaction_lines"])
    assert re.search(r"partitionBy\s*:\s*\"trandate\"", cfg)
    assert re.search(r"clusterBy\s*:\s*\[\s*\"account\"", cfg)
    assert re.search(r"uniqueKey\s*:\s*\[\s*\"line_id\"\s*\]", cfg)


def test_agg_key(models):
    cfg = _config_block(models["agg_netsuite_posting_daily"])
    assert re.search(r"uniqueKey\s*:\s*\[\s*\"trandate\",\s*\"account\",\s*\"subsidiary\"\s*\]", cfg)
    assert "fct_netsuite_transaction_lines" in models["agg_netsuite_posting_daily"]


def test_budgets_unpivot(models):
    body = models["stg_netsuite_budgets_monthly"]
    assert "UNPIVOT" in body
    for i in range(1, 13):
        assert f"amount_{i} AS {i}" in body
    assert re.search(r"uniqueKey\s*:\s*\[\s*\"budget_id\",\s*\"month\"\s*\]", _config_block(body))


def test_account_assertions(models):
    cfg = _config_block(models["stg_netsuite_account"])
    assert re.search(r"nonNull\s*:\s*\[\s*\"id\"\s*\]", cfg)
    assert "acctnumber IS NOT NULL" in cfg


def test_helpers_exist():
    js = (DATAFORM_DIR / "includes" / "netsuite.js").read_text()
    for fn in ("tf_to_bool", "ns_date", "ns_timestamp", "latest_version", "audit_columns"):
        assert f"function {fn}(" in js, f"missing helper {fn}"
        assert fn in js.split("module.exports")[1], f"{fn} not exported"
    assert "'%m/%d/%Y'" in js or '"%m/%d/%Y"' in js
    assert "%I:%M:%S %p" in js
