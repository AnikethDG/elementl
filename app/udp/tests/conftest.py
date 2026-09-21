"""Pytest fixtures, mock stubs, and configuration for Elementl UDP test suite."""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest

# Default GCS_CONFIG_BUCKET to empty for fast offline unit tests
if "GCS_CONFIG_BUCKET" not in os.environ:
    os.environ["GCS_CONFIG_BUCKET"] = ""

# Ensure app/udp, app/udp/cloud-run-jobs, and common modules are in sys.path
TESTS_DIR = Path(__file__).resolve().parent
UDP_DIR = TESTS_DIR.parent
FW_ROOT = UDP_DIR.parent.parent
CLOUD_RUN_JOBS_DIR = UDP_DIR / "cloud-run-jobs"

for p in [str(UDP_DIR), str(CLOUD_RUN_JOBS_DIR), str(FW_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Stubs for optional/native drivers during local unit testing
for mod_name in ["jwt", "oracledb", "urllib3.util.retry"]:
    if mod_name not in sys.modules:
        try:
            __import__(mod_name)
        except ImportError:
            sys.modules[mod_name] = MagicMock()

DEFAULT_PROJECT_ID = "pid-nse-stg-core-apps-k8ti"
DEFAULT_REGION = "us-central1"


@pytest.fixture(scope="session")
def gcp_project_id() -> str:
    """Returns the GCP project ID configured for integration tests."""
    return (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or DEFAULT_PROJECT_ID
    )


@pytest.fixture(scope="session")
def gcp_region() -> str:
    """Returns the GCP target region."""
    return os.environ.get("GCP_REGION", DEFAULT_REGION)


@pytest.fixture(scope="session")
def bronze_bucket(gcp_project_id: str) -> str:
    """Returns the Bronze Raw landing GCS bucket name."""
    return os.environ.get("RAW_BUCKET_NAME", f"bkt-{gcp_project_id}-udp-bronze-raw")


@pytest.fixture(scope="session")
def config_bucket(gcp_project_id: str) -> str:
    """Returns the configuration YAMLs GCS bucket name."""
    return os.environ.get("GCS_CONFIG_BUCKET") or f"bkt-{gcp_project_id}-udp-configs"


@pytest.fixture(scope="session")
def composer_env_name() -> str:
    """Returns the Composer environment name."""
    return os.environ.get("COMPOSER_ENV_NAME", "composer-elementl-dev")


@pytest.fixture
def mock_storage_client():
    """Mock Google Cloud Storage client for isolated unit testing."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    return mock_client


@pytest.fixture
def mock_secret_client():
    """Mock Google Secret Manager client for isolated unit testing."""
    mock_client = MagicMock()
    payload_mock = MagicMock()
    payload_mock.data = b'{"account_id": "1234567_SB1", "client_id": "test_client", "certificate_id": "cert_123"}'
    response_mock = MagicMock()
    response_mock.payload = payload_mock
    mock_client.access_secret_version.return_value = response_mock
    return mock_client


@pytest.fixture
def sample_records():
    """Returns deterministic sample records for extractor testing."""
    return [
        {
            "id": 101,
            "name": "Engineering Department",
            "is_active": True,
            "created_date": "2026-01-15T08:30:00Z",
            "budget": 250000.50,
        },
        {
            "id": 102,
            "name": "Finance & Accounting",
            "is_active": True,
            "created_date": "2026-02-01T12:00:00Z",
            "budget": 175000.00,
        },
    ]
