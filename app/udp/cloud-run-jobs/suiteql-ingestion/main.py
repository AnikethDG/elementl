# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Elementl NetSuite ingestion job.

Authenticates with NetSuite's OAuth 2.0 Machine-to-Machine (M2M) flow: a JWT
signed with our private certificate is exchanged for a bearer token, which is
then used against the SuiteQL endpoint. This mirrors the flow proven by
`gcp-data-lake/scripts/netsuite_m2m_test.py` and `pull_5_records.py`.

SuiteQL is used rather than the REST record API because several entities are
readable via SuiteQL but not via REST (see misc/netsuite-connectivity/README.md,
"5/7 entities readable via REST").

Private key handling
--------------------
The signing key is pulled from Secret Manager and loaded directly into
`cryptography` as bytes. It is never written to the container filesystem, so
there is no key file to leak, clean up, or accidentally bake into an image.
"""

import os
import sys
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

import jwt
import requests
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from common.base_extractor import BaseExtractor
from common.job_runner import run_job

logger = logging.getLogger("netsuite_job")

# =============================================================================
# Credentials -- the five NetSuite values, and where each comes from
# =============================================================================
# These are the same five values that gcp-data-lake/scripts/pull_5_records.py
# reads from its local .env. In Cloud Run there is no .env: four of them live
# in one JSON secret and the fifth (the key) lives in its own.
#
#   .env variable                 Source here                      Required
#   ---------------------------   ------------------------------   --------
#   NETSUITE_ACCOUNT_ID           config secret -> "account_id"    yes
#   NETSUITE_CLIENT_ID            config secret -> "client_id"     yes
#   NETSUITE_CERTIFICATE_ID       config secret -> "certificate_id" yes
#   NETSUITE_SCOPE                config secret -> "scope"         no (default)
#   NETSUITE_PRIVATE_KEY_PATH     secret-netsuite-private-key      yes
#
# CONFIG SECRET -- named by the CONNECTION_SECRET_ID env var. In
# p-nsedusc1-data-services-vg6s that is `secret-netsuite-api-config`:
#
#   {
#     "account_id":     "<PLACEHOLDER: as it appears in the SuiteTalk URL host.
#                        Production is bare digits, e.g. 9867530. Sandbox reads
#                        1234567_SB1 in the UI and is normalised to
#                        1234567-sb1 by _normalise_account_id() below>",
#     "client_id":      "<PLACEHOLDER: Client ID / Consumer Key from the
#                        Integration Record>",
#     "certificate_id": "<PLACEHOLDER: Manage Authentication > OAuth 2.0 Client
#                        Credentials (M2M) Setup. Becomes the JWT 'kid'>",
#     "scope":          "rest_webservices"
#   }
#
#   gcloud secrets versions add secret-netsuite-api-config \
#     --project=p-nsedusc1-data-services-vg6s --data-file=netsuite.json
#
# Each of the four also accepts an env-var override (NETSUITE_ACCOUNT_ID,
# NETSUITE_CLIENT_ID, NETSUITE_CERTIFICATE_ID, NETSUITE_SCOPE) for local runs
# and one-off backfills. Precedence is: config secret > env var > default.
#
# THE PRIVATE KEY is handled differently on purpose -- see below.
# =============================================================================

# ---- Private key handling ---------------------------------------------------
# The key is the one genuinely secret value; the other four are identifiers.
# It is therefore:
#
#   * kept in its OWN secret, so it can be rotated without rewriting the config
#     blob, and so the blob can be read/logged more freely;
#   * read as BYTES and passed straight to cryptography's
#     load_pem_private_key(), which accepts bytes -- so it is never written to
#     the container filesystem, never baked into an image, and there is no
#     tempfile to clean up or leak;
#   * NEVER exposed as an environment variable. Cloud Run env vars are visible
#     to anyone with run.jobs.get in the console and in `gcloud run jobs
#     describe` output. This is the reason there is no NETSUITE_PRIVATE_KEY
#     override to match the other four.
#
# Resolution order:
#   1. "private_key_pem" inline in the config blob (discouraged -- puts the key
#      in the same payload as the identifiers), else
#   2. "private_key_secret_id" in the config blob, else
#   3. the NETSUITE_PRIVATE_KEY_SECRET_ID env var (a secret *name*, not the
#      key itself), else
#   4. DEFAULT_PRIVATE_KEY_SECRET_ID below.
#
#   gcloud secrets versions add secret-netsuite-private-key \
#     --project=p-nsedusc1-data-services-vg6s --data-file=private.pem
DEFAULT_PRIVATE_KEY_SECRET_ID = "secret-netsuite-private-key"

# Applied when neither the config secret nor the environment supplies one.
DEFAULT_SCOPE = "rest_webservices"

# Hard safety cap on rows returned per task. Every SuiteQL statement is
# rewritten to carry a row limit and the result set is truncated to match, so
# a misconfigured query cannot pull a large volume from production NetSuite.
# Override with NETSUITE_MAX_RECORDS once the pipeline is trusted.
DEFAULT_MAX_RECORDS = 5

# JWTs must expire less than an hour after issue; 50 minutes is comfortably
# inside that and leaves room for clock skew.
JWT_TTL_SECONDS = 3000



def self_or_secret_lookup(secret_id: str) -> str:
    """Fetches a single secret value from Secret Manager if present."""
    from google.cloud import secretmanager
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    if not project_id:
        return ""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("utf-8").strip()


class NetSuiteAuthError(RuntimeError):
    """Private key could not be loaded/signed, or the token exchange failed."""


class NetSuiteQueryError(RuntimeError):
    """A SuiteQL query returned a non-200 response."""


def _max_records() -> int:
    raw = os.environ.get("NETSUITE_MAX_RECORDS", "").strip()
    if not raw:
        return DEFAULT_MAX_RECORDS
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "NETSUITE_MAX_RECORDS=%r is not an integer; using %s",
            raw,
            DEFAULT_MAX_RECORDS,
        )
        return DEFAULT_MAX_RECORDS
    if value < 1:
        logger.warning("NETSUITE_MAX_RECORDS=%s is < 1; using 1", value)
        return 1
    return value


def _normalise_account_id(account_id: str) -> str:
    """Converts an account id into its REST hostname form.

    NetSuite writes sandbox accounts as ``1234567_SB1`` but the SuiteTalk host
    requires ``1234567-sb1``. Passing the raw form through produces a DNS
    failure that looks like a network problem rather than a config problem.
    """
    return account_id.strip().lower().replace("_", "-")


class NetSuiteExtractor(BaseExtractor):
    """SuiteQL extractor using NetSuite OAuth 2.0 client-credentials (M2M)."""

    def __init__(self) -> None:
        super().__init__()
        self._token: Optional[str] = None

    # -------------------------------------------------------------------- auth

    @staticmethod
    def _load_key_and_alg(private_key_pem: str):
        """Loads the PEM and selects the JWT algorithm matching the key type.

        EC -> ES256, RSA -> PS256. NetSuite rejects a mismatched alg with an
        opaque error, so this is derived from the key rather than configured.
        """
        try:
            key = load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
        except Exception as exc:
            raise NetSuiteAuthError(f"Could not parse the private key PEM: {exc}") from exc

        if isinstance(key, ec.EllipticCurvePrivateKey):
            return key, "ES256"
        if isinstance(key, rsa.RSAPrivateKey):
            return key, "PS256"
        raise NetSuiteAuthError(
            f"Unsupported key type {type(key).__name__}; use an EC (P-256) or RSA key."
        )

    @staticmethod
    def _build_assertion(
        client_id: str, certificate_id: str, scope: str, token_url: str, key, alg: str
    ) -> str:
        now = int(time.time())
        return jwt.encode(
            {
                "iss": client_id,
                "scope": scope,
                # Must exactly equal the token endpoint URL or NetSuite rejects it.
                "aud": token_url,
                "iat": now,
                "exp": now + JWT_TTL_SECONDS,
            },
            key,
            algorithm=alg,
            headers={"alg": alg, "typ": "JWT", "kid": certificate_id},
        )

    def _get_token(self, creds: Dict[str, Any], token_url: str) -> str:
        if self._token is not None:
            return self._token

        # The key lives in its own secret by default, so the config blob only
        # has to carry the four identifiers.
        key_source = dict(creds)
        if not key_source.get("private_key_pem") and not key_source.get(
            "private_key_secret_id"
        ):
            key_source["private_key_secret_id"] = os.environ.get(
                "NETSUITE_PRIVATE_KEY_SECRET_ID", DEFAULT_PRIVATE_KEY_SECRET_ID
            )

        private_key_pem = self.resolve_pem(
            key_source,
            inline_key="private_key_pem",
            secret_ref_key="private_key_secret_id",
            what="NetSuite signing key",
        )
        key, alg = self._load_key_and_alg(private_key_pem)
        logger.info("Signing client assertion with %s", alg)

        assertion = self._build_assertion(
            client_id=creds["client_id"],
            certificate_id=creds["certificate_id"],
            scope=creds.get("scope") or DEFAULT_SCOPE,
            token_url=token_url,
            key=key,
            alg=alg,
        )

        response = requests.post(
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_assertion_type": (
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                ),
                "client_assertion": assertion,
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise NetSuiteAuthError(
                f"Token exchange failed: HTTP {response.status_code}: "
                f"{response.text[:1000]}"
            )
        token = response.json().get("access_token")
        if not token:
            raise NetSuiteAuthError(
                f"Token exchange returned 200 without an access_token: "
                f"{response.text[:1000]}"
            )

        self._token = token
        return token

    # ------------------------------------------------------------------- query

    @staticmethod
    def _apply_row_limit(suiteql: str, max_records: int) -> str:
        """Ensures the statement carries an explicit row limit.

        Applied belt-and-braces alongside the page size and the post-fetch
        truncation: the limit must hold even if a future caller changes how
        pagination works.
        """
        statement = suiteql.strip().rstrip(";").strip()
        lowered = statement.lower()
        if "fetch first" in lowered or "rownum" in lowered:
            logger.info("Query already carries its own row limit; leaving it intact.")
            return statement
        return f"{statement} FETCH FIRST {max_records} ROWS ONLY"

    @staticmethod
    def _clean(row: Dict[str, Any]) -> Dict[str, Any]:
        """Drops SuiteQL's HATEOAS `links` array, which BigQuery cannot use."""
        return {k: v for k, v in row.items() if k != "links"}

    def _query(self, suiteql_url: str, token: str, suiteql: str, max_records: int):
        rows: List[Dict[str, Any]] = []
        offset = 0
        page_size = max_records

        while len(rows) < max_records:
            response = requests.post(
                f"{suiteql_url}?limit={page_size}&offset={offset}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    # Required by NetSuite for SuiteQL result sets.
                    "Prefer": "transient",
                },
                json={"q": suiteql},
                timeout=60,
            )
            if response.status_code != 200:
                raise NetSuiteQueryError(
                    f"SuiteQL failed: HTTP {response.status_code}: "
                    f"{response.text[:1000]}"
                )

            payload = response.json()
            items = payload.get("items", [])
            rows.extend(self._clean(item) for item in items)
            logger.info(
                "Fetched %s row(s) at offset %s (running total %s)",
                len(items),
                offset,
                len(rows),
            )

            if not payload.get("hasMore") or not items:
                break
            offset += page_size

        # Final guard so the cap holds regardless of what the API returned.
        return rows[:max_records]

    # ----------------------------------------------------------------- extract

    @staticmethod
    def _resolve_field(
        creds: Dict[str, Any], key: str, env_var: str, default: str = ""
    ) -> str:
        """Resolves one credential field: config secret > env var > default.

        Each resolution is logged with its origin. A NetSuite auth failure
        caused by the wrong client_id is otherwise indistinguishable from one
        caused by a bad key -- the API returns the same opaque error for both.
        """
        value = str(creds.get(key) or "").strip()
        if value:
            logger.info("%s: from config secret", key)
            return value

        value = os.environ.get(env_var, "").strip()
        if value:
            logger.info("%s: from %s env var", key, env_var)
            return value

        secret_map = {
            "account_id": "secret-netsuite-account-id",
            "client_id": "secret-netsuite-client-id",
            "certificate_id": "secret-netsuite-certificate-id",
            "scope": "secret-netsuite-scope",
        }
        if key in secret_map:
            try:
                sec_val = self_or_secret_lookup(secret_map[key])
                if sec_val:
                    logger.info("%s: from individual secret %s", key, secret_map[key])
                    return sec_val
            except Exception:
                pass

        if default:
            logger.info("%s: using default %r", key, default)
        return default

    def extract(self, config) -> Tuple[int, int, List[str]]:
        creds = self.parse_secret_json(config.connection_secret_id)

        # The five values. Four resolve here; the key is deliberately excluded
        # from env-var resolution and is fetched inside _get_token().
        resolved = {
            "account_id": self._resolve_field(
                creds, "account_id", "NETSUITE_ACCOUNT_ID"
            ),
            "client_id": self._resolve_field(creds, "client_id", "NETSUITE_CLIENT_ID"),
            "certificate_id": self._resolve_field(
                creds, "certificate_id", "NETSUITE_CERTIFICATE_ID"
            ),
            "scope": self._resolve_field(
                creds, "scope", "NETSUITE_SCOPE", DEFAULT_SCOPE
            ),
        }

        missing = [
            field
            for field in ("account_id", "client_id", "certificate_id")
            if not resolved[field]
        ]
        if missing:
            raise ValueError(
                f"NetSuite credentials incomplete: {missing}. Populate "
                f"'{config.connection_secret_id or 'secret-netsuite-api-config'}' "
                "or set the matching NETSUITE_* environment variables."
            )

        # Carry through the optional private-key pointers from the raw secret.
        for passthrough in ("private_key_pem", "private_key_secret_id"):
            if creds.get(passthrough):
                resolved[passthrough] = creds[passthrough]
        creds = resolved

        if not config.query:
            raise ValueError(
                f"Task '{config.task_id}' has no SuiteQL query. Set source.query "
                "in the task's YAML metadata."
            )

        account = _normalise_account_id(creds["account_id"])
        base = f"https://{account}.suitetalk.api.netsuite.com"
        token_url = f"{base}/services/rest/auth/oauth2/v1/token"
        suiteql_url = f"{base}/services/rest/query/v1/suiteql"

        max_records = _max_records()
        suiteql = self._apply_row_limit(config.query, max_records)

        logger.info("NetSuite account %s, max_records=%s", account, max_records)
        logger.info("SuiteQL: %s", suiteql)

        token = self._get_token(creds, token_url)
        logger.info("Obtained bearer token.")

        rows = self._query(suiteql_url, token, suiteql, max_records)
        logger.info("Retrieved %s row(s) from NetSuite.", len(rows))

        rec_count, byte_count, gcs_uris = self.write_records_to_gcs(
            records=rows,
            gcs_bucket_name=config.gcs_bucket,
            gcs_prefix=config.gcs_prefix,
            file_format=config.destination_format,
            partition_date=config.execution_date,
        )
        if gcs_uris and (config.target_dataset or os.environ.get("LOAD_TO_BQ", "true").lower() == "true"):
            try:
                from google.cloud import bigquery
                bq_project = config.audit_project or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GCP_PROJECT")
                bq_dataset = config.target_dataset or "ds_bronze_netsuite"
                bq_table = config.target_table or f"netsuite_{config.task_id.replace('netsuite_', '')}"
                if bq_project and gcs_uris[0].endswith(".parquet"):
                    bq_client = bigquery.Client(project=bq_project)
                    job_cfg = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        autodetect=True,
                    )
                    load_job = bq_client.load_table_from_uri(gcs_uris[0], f"{bq_project}.{bq_dataset}.{bq_table}", job_config=job_cfg)
                    load_job.result()
                    logger.info("Loaded %s rows into BigQuery %s.%s.%s", rec_count, bq_project, bq_dataset, bq_table)
            except Exception as exc:
                logger.warning("BigQuery load warning for %s: %s", config.task_id, exc)
        return rec_count, byte_count, gcs_uris


if __name__ == "__main__":
    sys.exit(run_job(NetSuiteExtractor()))
