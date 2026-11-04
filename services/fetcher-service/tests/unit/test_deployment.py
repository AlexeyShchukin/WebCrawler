"""Checks for the Fetcher container's runtime configuration."""

import re
import tomllib
from pathlib import Path

from crawler_fetcher.settings import FetcherSettings

ROOT = Path(__file__).resolve().parents[4]
SERVICE = ROOT / "services" / "fetcher-service"


def test_compose_supplies_all_fetcher_settings_and_restart_policy() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    fetcher = compose.split("  fetcher:\n", 1)[1].split("\n  content:\n", 1)[0]
    environment = fetcher.split("    environment:\n", 1)[1].split("    depends_on:", 1)[0]
    keys = set(re.findall(r"^      ([A-Z_]+):", environment, re.MULTILINE))
    required = {name.upper() for name in FetcherSettings.model_fields} - {"RABBITMQ_URL", "REDIS_URL"}
    assert required <= keys
    assert {"RABBITMQ_URL", "REDIS_URL"}.isdisjoint(keys)
    assert 'restart: unless-stopped' in fetcher
    assert '/health' in fetcher


def test_image_starts_fetcher_app_with_installed_server() -> None:
    dockerfile = (SERVICE / "Dockerfile").read_text(encoding="utf-8")
    project = tomllib.loads((SERVICE / "pyproject.toml").read_text(encoding="utf-8"))
    assert any(dep.startswith("uvicorn") for dep in project["project"]["dependencies"])
    assert '"crawler_fetcher.app:create_app", "--factory"' in dockerfile
