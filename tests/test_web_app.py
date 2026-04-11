from pathlib import Path

from fastapi.testclient import TestClient

from druck.web.app import app


def test_audit_api_returns_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "trade_log.db"
    monkeypatch.chdir(tmp_path)
    from druck.db import init_db, log_trade_audit

    conn = init_db(str(db_path))
    log_trade_audit(conn, "order_execution", ticker="SPY", side="BUY", qty=1, status="submitted", detail="ok")

    client = TestClient(app)
    resp = client.get("/api/audit")
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"][0]["event_type"] == "order_execution"
