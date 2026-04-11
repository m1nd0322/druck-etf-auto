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


def test_ack_api_creates_and_returns_rows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = TestClient(app)

    resp = client.post("/api/ack", json={"ack_type": "partial_fill_replan", "status": "acknowledged", "note": "reviewed"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["row"]["ack_type"] == "partial_fill_replan"

    fetch_resp = client.get("/api/ack")
    assert fetch_resp.status_code == 200
    fetch_body = fetch_resp.json()
    assert fetch_body["rows"][0]["note"] == "reviewed"


def test_runtime_api_returns_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "trade_log.db"
    monkeypatch.chdir(tmp_path)
    from druck.db import init_db, log_runtime_event

    conn = init_db(str(db_path))
    log_runtime_event(conn, category="strategy_halt", message="negative_momentum_halt", detail="3 assets negative")

    client = TestClient(app)
    resp = client.get("/api/runtime")
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"][0]["category"] == "strategy_halt"


def test_runtime_api_resolves_row(tmp_path, monkeypatch):
    db_path = tmp_path / "trade_log.db"
    monkeypatch.chdir(tmp_path)
    from druck.db import init_db, log_runtime_event

    conn = init_db(str(db_path))
    log_runtime_event(conn, category="system_error", message="boom", detail="trace")

    client = TestClient(app)
    rows = client.get("/api/runtime").json()["rows"]
    event_id = rows[0]["id"]
    resp = client.post(f"/api/runtime/{event_id}/resolve", json={"status": "resolved", "note": "reviewed"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["row"]["status"] == "resolved"
