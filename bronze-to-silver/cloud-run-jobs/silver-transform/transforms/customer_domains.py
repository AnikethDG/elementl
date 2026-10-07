"""stg_netsuite_customer_domains: latest customer version + email domain + fuzzy company clusters."""
from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher

import pandas as pd

from common import Context, field, latest_version, parse_ns_date, parse_ns_timestamp, tf_to_bool, to_decimal

log = logging.getLogger("silver_transform.customer_domains")

DEFAULT_TABLE = "stg_netsuite_customer_domains"

SCHEMA = [
    field("customer_id", "STRING", "REQUIRED", "netsuite_customer.id (primary key)"),
    field("entityid", "STRING"),
    field("companyname", "STRING"),
    field("companyname_normalised", "STRING"),
    field("email", "STRING"),
    field("email_domain", "STRING"),
    field("domain_type", "STRING", description="'free' | 'corporate' | NULL"),
    field("phone", "STRING"),
    field("isinactive", "BOOL"),
    field("creditlimit", "NUMERIC"),
    field("balance", "NUMERIC"),
    field("currency", "STRING"),
    field("datecreated", "DATE"),
    field("lastmodifieddate", "TIMESTAMP"),
    field("company_cluster_id", "STRING", description="smallest customer id in the fuzzy name cluster"),
    field("company_cluster_size", "INT64"),
    field("ingestion_date", "DATE"),
    field("ingestion_timestamp", "TIMESTAMP"),
]

FREE_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "ymail.com", "outlook.com", "hotmail.com",
    "live.com", "msn.com", "aol.com", "icloud.com", "me.com", "protonmail.com", "proton.me",
}
LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "l.l.c", "ltd", "limited", "co", "company", "corp", "corporation", "gmbh",
    "ag", "sa", "plc", "llp", "lp", "pty", "bv", "nv", "srl", "sarl", "kk", "oy", "ab",
}
SIMILARITY_THRESHOLD = 0.85


def email_domain(email) -> str | None:
    if email is None or (isinstance(email, float) and pd.isna(email)) or email is pd.NA:
        return None
    s = str(email).strip().lower()
    if "@" not in s:
        return None
    dom = s.rsplit("@", 1)[1].strip().strip(".")
    return dom or None


def domain_type(domain: str | None) -> str | None:
    if not domain:
        return None
    return "free" if domain in FREE_DOMAINS else "corporate"


def normalise_company(name) -> str:
    """lowercase, strip punctuation and trailing legal suffixes, collapse whitespace."""
    if name is None or (isinstance(name, float) and pd.isna(name)) or name is pd.NA:
        return ""
    s = re.sub(r"[^\w\s]", " ", str(name).lower().replace("&", " and "))
    tokens = [t for t in s.split() if t]
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _id_sort_key(cid: str):
    try:
        return (0, int(cid), cid)
    except (TypeError, ValueError):
        return (1, 0, str(cid))


def cluster_companies(names: dict[str, str], threshold: float = SIMILARITY_THRESHOLD) -> dict[str, str]:
    """Greedy fuzzy clustering.

    ``names`` maps customer id -> normalised company name. Customers are visited in ascending id
    order; a customer joins the first existing cluster whose representative (the smallest id in
    that cluster) has ``SequenceMatcher.ratio() >= threshold``; otherwise it starts a new cluster.
    Returns customer id -> cluster id (= representative's customer id). Deterministic.
    """
    reps: list[tuple[str, str]] = []  # (cluster_id, normalised name)
    exact: dict[str, str] = {}
    assignment: dict[str, str] = {}
    for cid in sorted(names, key=_id_sort_key):
        name = names[cid]
        if not name:
            assignment[cid] = cid
            continue
        if name in exact:
            assignment[cid] = exact[name]
            continue
        matcher = SequenceMatcher(None, name, "")
        hit = None
        for rep_id, rep_name in reps:
            matcher.set_seq2(rep_name)
            # cheap upper-bound filters before the full ratio
            if matcher.real_quick_ratio() < threshold or matcher.quick_ratio() < threshold:
                continue
            if matcher.ratio() >= threshold:
                hit = rep_id
                break
        if hit is None:
            hit = cid
            reps.append((cid, name))
            exact[name] = cid
        assignment[cid] = hit
    return assignment


def transform(customers: pd.DataFrame) -> pd.DataFrame:
    if customers.empty:
        return pd.DataFrame(columns=[f.name for f in SCHEMA])
    latest = latest_version(customers, "id")
    out = pd.DataFrame({
        "customer_id": latest["id"].astype(str),
        "entityid": latest.get("entityid"),
        "companyname": latest.get("companyname"),
        "email": latest.get("email"),
        "phone": latest.get("phone"),
        "currency": latest.get("currency"),
        "ingestion_date": latest.get("ingestion_date"),
        "ingestion_timestamp": latest.get("ingestion_timestamp"),
    })
    out["companyname_normalised"] = latest["companyname"].map(normalise_company)
    out["email_domain"] = latest["email"].map(email_domain)
    out["domain_type"] = out["email_domain"].map(domain_type)
    out["isinactive"] = latest["isinactive"].map(tf_to_bool)
    out["creditlimit"] = latest["creditlimit"].map(to_decimal)
    out["balance"] = latest["balance"].map(to_decimal)
    out["datecreated"] = latest["datecreated"].map(parse_ns_date)
    out["lastmodifieddate"] = latest["lastmodifieddate"].map(parse_ns_timestamp)

    assignment = cluster_companies(dict(zip(out["customer_id"], out["companyname_normalised"])))
    out["company_cluster_id"] = out["customer_id"].map(assignment)
    sizes = out["company_cluster_id"].value_counts()
    out["company_cluster_size"] = out["company_cluster_id"].map(sizes).astype(int)
    return out.sort_values("customer_id", key=lambda s: s.map(_id_sort_key)).reset_index(drop=True)


def run(ctx: Context) -> int:
    src = ctx.read_bronze("netsuite_customer")
    out = transform(src)
    n_clusters = out["company_cluster_id"].nunique() if len(out) else 0
    log.info("%d bronze rows -> %d customers, %d company clusters, domain_type=%s", len(src), len(out), n_clusters,
             out["domain_type"].value_counts(dropna=False).to_dict() if len(out) else {})
    return ctx.write_silver(out, SCHEMA)
