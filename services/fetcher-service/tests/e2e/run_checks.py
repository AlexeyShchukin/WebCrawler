"""Run Fetcher checks against the dedicated tests/e2e/compose.yml stack."""

import os
import sys
from pathlib import Path
from urllib.parse import quote

import pytest
from dotenv import dotenv_values


def main() -> int:
    root = Path(__file__).resolve().parents[4]
    credentials = dotenv_values(root / ".env.example")
    password = credentials.get("REDIS_PASSWORD")
    if not password:
        raise ValueError("The isolated test stack requires REDIS_PASSWORD in .env.example")
    # Use only the dedicated Compose ports. Never inherit a production Redis URL.
    os.environ.update({
        "FETCHER_E2E": "1",
        "FETCHER_LIVE_INTEGRATION": "1",
        "LIVE_RABBIT_PORT": "19867",
        "LIVE_REDIS_PORT": "19879",
        "LIVE_MINIO_PORT": "19900",
        "LIVE_FRONTIER_URL": "http://127.0.0.1:19880",
        "E2E_CONTROL_URL": "http://127.0.0.1:19889",
        "TEST_REDIS_URL": f"redis://:{quote(password, safe='')}@127.0.0.1:19879/0",
    })
    return pytest.main(sys.argv[1:] or ["-q", "-p", "no:cacheprovider", "-ra"])


if __name__ == "__main__":
    raise SystemExit(main())
