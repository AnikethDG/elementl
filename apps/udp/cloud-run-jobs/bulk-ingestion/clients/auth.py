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

"""Extensible Authentication Provider for Ingestion Services.

Supports pluggable authentication strategies:
- 'none': No authentication (default, skipped if not required in configuration)
- 'secret_manager' / 'api_key': Resolves secret payload from GCP Secret Manager
  (with local environment variable fallback) and injects it as an HTTP header or query parameter
- 'bearer_token': Attaches 'Authorization: Bearer <token>' header
- 'token_query': Attaches query parameter with token
- Custom auth handlers: Extensible registry for future authentication methods (OAuth2, mTLS, HMAC)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, Optional, Tuple
import requests

try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = None  # type: ignore

logger = logging.getLogger(__name__)

# Registry for user-defined custom authentication strategies
CUSTOM_AUTH_HANDLERS: Dict[str, Callable[..., Tuple[requests.Session, Dict[str, Any]]]] = {}


def register_auth_handler(auth_type: str) -> Callable:
    """Decorator to register a custom authentication handler for future extensions."""
    def decorator(fn: Callable[..., Tuple[requests.Session, Dict[str, Any]]]) -> Callable:
        CUSTOM_AUTH_HANDLERS[auth_type.lower()] = fn
        return fn
    return decorator


def get_secret_value(project_id: str, secret_id: str) -> str:
    """Retrieves secret payload from GCP Secret Manager or local environment fallback."""
    if not secret_id:
        return ""

    # Check local environment variable fallback (e.g. CENSUS_API_KEY, API_KEY)
    env_var_name = secret_id.upper().replace("-", "_")
    env_val = os.getenv(env_var_name) or os.getenv("API_KEY")
    if env_val:
        logger.info("Using secret value from environment variable for secret_id=%s", secret_id)
        return env_val.strip()

    if secretmanager is None:
        raise RuntimeError(
            "google-cloud-secret-manager is not installed and no local environment variable "
            f"'{env_var_name}' or 'API_KEY' was found for secret_id='{secret_id}'."
        )

    client = secretmanager.SecretManagerServiceClient()
    name = secret_id if secret_id.startswith("projects/") else f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    logger.info("Retrieving secret from GCP Secret Manager: %s", secret_id)
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8").strip()


def apply_authentication(
    session: requests.Session,
    query_params: Dict[str, Any],
    conn_info: Dict[str, Any],
    project_id: str,
    source_id: str = "",
    default_param: Optional[str] = None,
    default_header: Optional[str] = None,
) -> Tuple[requests.Session, Dict[str, Any]]:
    """Applies configured authentication to requests.Session and query_params.

    If auth_type is 'none', omitted, or secret_id is not set, skips authentication completely.
    """
    auth_type = (conn_info.get("auth_type") or "none").lower()
    secret_id = conn_info.get("secret_id")
    auth_header = conn_info.get("auth_header")
    auth_param = conn_info.get("auth_param")

    # 1. Check custom registered handlers first (for future extensibility)
    if auth_type in CUSTOM_AUTH_HANDLERS:
        logger.info("Applying custom authentication handler for auth_type='%s'", auth_type)
        return CUSTOM_AUTH_HANDLERS[auth_type](
            session=session,
            query_params=query_params,
            conn_info=conn_info,
            project_id=project_id,
            source_id=source_id,
        )

    # 2. If 'none' or no secret_id, skip authentication
    if auth_type == "none" or not secret_id:
        logger.debug("Authentication skipped (auth_type='%s', secret_id='%s')", auth_type, secret_id)
        return session, query_params

    # 3. Resolve secret value
    secret_value = get_secret_value(project_id, secret_id)
    if not secret_value:
        logger.warning("Secret value resolved empty for secret_id=%s; proceeding without credentials.", secret_id)
        return session, query_params

    # 4. Apply based on auth_type
    if auth_type in {"secret_manager", "api_key"}:
        if auth_header:
            session.headers[auth_header] = secret_value
            logger.info("Injected API key into request header '%s' for source_id=%s", auth_header, source_id)
        elif auth_param:
            query_params[auth_param] = secret_value
            logger.info("Injected API key into query parameter '%s' for source_id=%s", auth_param, source_id)
        elif default_param:
            query_params[default_param] = secret_value
            logger.info("Injected API key into default query parameter '%s' for source_id=%s", default_param, source_id)
        elif default_header:
            session.headers[default_header] = secret_value
            logger.info("Injected API key into default request header '%s' for source_id=%s", default_header, source_id)
        else:
            session.headers["X-API-Key"] = secret_value
            logger.info("Injected API key into default 'X-API-Key' header for source_id=%s", source_id)

    elif auth_type == "bearer_token":
        session.headers["Authorization"] = f"Bearer {secret_value}"
        logger.info("Injected Bearer token authorization header for source_id=%s", source_id)

    elif auth_type == "token_query":
        param_name = auth_param or default_param or "token"
        query_params[param_name] = secret_value
        logger.info("Injected token into query parameter '%s' for source_id=%s", param_name, source_id)

    else:
        logger.warning(
            "Unrecognized auth_type '%s' for source_id=%s. Supported types: 'none', 'api_key', "
            "'secret_manager', 'bearer_token', 'token_query', or custom registered handlers.",
            auth_type,
            source_id,
        )

    return session, query_params
