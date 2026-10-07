"""stg_netsuite_address_enriched: standardise and validate netsuite_address."""
from __future__ import annotations

import logging
import re

import pandas as pd

from common import Context, field, latest_version

log = logging.getLogger("silver_transform.address_enrichment")

DEFAULT_TABLE = "stg_netsuite_address_enriched"

SCHEMA = [
    field("address_id", "STRING", "REQUIRED", "netsuite_address.addressinternalid (primary key)"),
    field("addressee", "STRING"),
    field("addr1_raw", "STRING"),
    field("addr1", "STRING", description="Title-cased street with suffixes expanded"),
    field("addr2", "STRING"),
    field("city_raw", "STRING"),
    field("city", "STRING"),
    field("state_raw", "STRING"),
    field("state", "STRING", description="2-letter US state code"),
    field("zip_raw", "STRING"),
    field("zip5", "STRING"),
    field("zip4", "STRING"),
    field("country_raw", "STRING"),
    field("country", "STRING", description="ISO-3166 alpha-2"),
    field("region", "STRING", description="US census division, 'Unknown' when state unmapped"),
    field("is_valid", "BOOL"),
    field("validation_errors", "STRING", description="semicolon separated list, NULL when valid"),
    field("ingestion_date", "DATE"),
    field("ingestion_timestamp", "TIMESTAMP"),
]

STREET_SUFFIXES = {
    "st": "Street", "str": "Street",
    "ave": "Avenue", "av": "Avenue",
    "rd": "Road",
    "blvd": "Boulevard",
    "dr": "Drive",
    "ln": "Lane",
    "ct": "Court",
    "pl": "Place",
    "hwy": "Highway",
    "pkwy": "Parkway",
    "ste": "Suite",
    "apt": "Apartment",
}
_ORDINAL_RE = re.compile(r"^\d+(st|nd|rd|th)$", re.I)
_DIRECTIONS = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC", "puerto rico": "PR",
}
STATE_CODES = set(US_STATES.values())

# US census divisions
STATE_REGION = {
    **{s: "New England" for s in ("CT", "ME", "MA", "NH", "RI", "VT")},
    **{s: "Middle Atlantic" for s in ("NJ", "NY", "PA")},
    **{s: "East North Central" for s in ("IL", "IN", "MI", "OH", "WI")},
    **{s: "West North Central" for s in ("IA", "KS", "MN", "MO", "NE", "ND", "SD")},
    **{s: "South Atlantic" for s in ("DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV")},
    **{s: "East South Central" for s in ("AL", "KY", "MS", "TN")},
    **{s: "West South Central" for s in ("AR", "LA", "OK", "TX")},
    **{s: "Mountain" for s in ("AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY")},
    **{s: "Pacific" for s in ("AK", "CA", "HI", "OR", "WA")},
}

COUNTRIES = {
    "united states": "US", "united states of america": "US", "usa": "US", "us": "US", "u.s.": "US", "u.s.a.": "US",
    "canada": "CA", "ca": "CA", "united kingdom": "GB", "uk": "GB", "gb": "GB", "great britain": "GB",
    "germany": "DE", "de": "DE", "india": "IN", "in": "IN", "japan": "JP", "jp": "JP", "mexico": "MX", "mx": "MX",
}

_ZIP_RE = re.compile(r"^\s*(\d{5})(?:\s*-\s*(\d{4}))?\s*$")
_NULLISH = {"", "n/a", "na", "none", "null", "-", "unknown"}


def _clean(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
        return None
    s = str(v).strip()
    return s or None


def _title_token(tok: str) -> str:
    if _ORDINAL_RE.match(tok):
        return tok.lower()
    return tok[:1].upper() + tok[1:].lower()


def standardise_street(addr: str | None) -> str | None:
    """Title-case and expand common suffix abbreviations (``st``->``Street``, ``ave``->``Avenue``...)."""
    addr = _clean(addr)
    if addr is None:
        return None
    out = []
    for raw in re.split(r"\s+", addr):
        core = raw.strip(".,")
        key = core.lower()
        if key in STREET_SUFFIXES:
            out.append(STREET_SUFFIXES[key])
        elif key in _DIRECTIONS and len(core) <= 2:
            out.append(core.upper())
        elif core.isdigit():
            out.append(core)
        elif "-" in core:
            out.append("-".join(_title_token(p) for p in core.split("-")))
        else:
            out.append(_title_token(core) + ("," if raw.endswith(",") else ""))
    return " ".join(x for x in out if x)


def standardise_city(city: str | None) -> str | None:
    city = _clean(city)
    if city is None:
        return None
    return " ".join(_title_token(t) for t in re.split(r"\s+", city))


def normalise_state(state: str | None) -> str | None:
    """``'texas'``/``'tx'``/``'TX'`` -> ``'TX'``; NULL stays NULL; unknown values are passed through upper-cased."""
    state = _clean(state)
    if state is None:
        return None
    key = re.sub(r"\s+", " ", state.lower().strip(" ."))
    if key in US_STATES:
        return US_STATES[key]
    up = key.upper()
    if up in STATE_CODES:
        return up
    return state.upper()


def split_zip(zip_: str | None) -> tuple[str | None, str | None]:
    zip_ = _clean(zip_)
    if zip_ is None or zip_.lower() in _NULLISH:
        return None, None
    m = _ZIP_RE.match(zip_)
    if not m:
        digits = re.sub(r"\D", "", zip_)
        if len(digits) == 9:
            return digits[:5], digits[5:]
        if len(digits) == 5:
            return digits, None
        return None, None
    return m.group(1), m.group(2)


def normalise_country(country: str | None) -> str | None:
    country = _clean(country)
    if country is None:
        return None
    return COUNTRIES.get(country.lower(), country.upper() if len(country) == 2 else country)


def region_for(state: str | None) -> str:
    return STATE_REGION.get(state or "", "Unknown")


def enrich_row(row: dict) -> dict:
    addr1 = standardise_street(row.get("addr1"))
    city = standardise_city(row.get("city"))
    state = normalise_state(row.get("state"))
    zip5, zip4 = split_zip(row.get("zip"))
    country = normalise_country(row.get("country"))
    errors = []
    if not addr1:
        errors.append("missing_addr1")
    if not city:
        errors.append("missing_city")
    if state is None:
        errors.append("missing_state")
    elif state not in STATE_CODES:
        errors.append("unknown_state")
    if zip5 is None:
        errors.append("invalid_zip" if _clean(row.get("zip")) and str(row.get("zip")).strip().lower() not in _NULLISH else "missing_zip")
    is_valid = state in STATE_CODES and zip5 is not None
    return {
        "address_id": _clean(row.get("addressinternalid")),
        "addressee": _clean(row.get("addressee")),
        "addr1_raw": _clean(row.get("addr1")),
        "addr1": addr1,
        "addr2": standardise_street(row.get("addr2")),
        "city_raw": _clean(row.get("city")),
        "city": city,
        "state_raw": _clean(row.get("state")),
        "state": state,
        "zip_raw": _clean(row.get("zip")),
        "zip5": zip5,
        "zip4": zip4,
        "country_raw": _clean(row.get("country")),
        "country": country,
        "region": region_for(state),
        "is_valid": bool(is_valid),
        "validation_errors": ";".join(errors) if errors else None,
        "ingestion_date": row.get("ingestion_date"),
        "ingestion_timestamp": row.get("ingestion_timestamp"),
    }


def enrich(addresses: pd.DataFrame) -> pd.DataFrame:
    if addresses.empty:
        return pd.DataFrame(columns=[f.name for f in SCHEMA])
    latest = latest_version(addresses, "addressinternalid", tiebreak_col=None)
    rows = [enrich_row(r) for r in latest.to_dict("records")]
    return pd.DataFrame(rows)


def run(ctx: Context) -> int:
    src = ctx.read_bronze("netsuite_address")
    out = enrich(src)
    log.info("enriched %d addresses: %d valid, %d invalid", len(out), int(out["is_valid"].sum()) if len(out) else 0,
             int((~out["is_valid"].astype(bool)).sum()) if len(out) else 0)
    return ctx.write_silver(out, SCHEMA)
