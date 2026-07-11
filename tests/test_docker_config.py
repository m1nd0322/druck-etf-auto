from pathlib import Path

import yaml


def test_compose_web_is_loopback_only_and_does_not_mount_optional_local_config():
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))
    web = compose["services"]["web"]

    assert web["ports"] == ["127.0.0.1:${PORT:-8000}:8000"]
    assert all("config.local.yaml" not in volume for volume in web["volumes"])
    assert "trade-state:/app/state" in web["volumes"]
    assert all("/app/trade_log.db" not in volume for volume in web["volumes"])
    assert web["env_file"] == [{"path": ".env", "required": False}]
    assert "trade-state" in compose["volumes"]
