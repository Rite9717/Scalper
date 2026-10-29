from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_compose_and_image_pin_healthchecks() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    init_sql = (REPO_ROOT / "docker" / "postgres" / "init.sql").read_text(encoding="utf-8")
    assert "timescale/timescaledb:2.31.0-pg16" in compose
    assert "redis:7.4-alpine" in compose
    assert compose.count("healthcheck:") >= 3
    assert "127.0.0.1:5432:5432" in compose
    assert "127.0.0.1:6379:6379" in compose
    assert "127.0.0.1:8080:8080" in compose
    assert "USER scalper" in dockerfile
    assert "HEALTHCHECK" in dockerfile
    assert "CREATE EXTENSION IF NOT EXISTS timescaledb" in init_sql
    assert "schema_phase" in init_sql
