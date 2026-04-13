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


def test_backtest_api_returns_scenarios_and_analytics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = TestClient(app)

    class DummyResult:
        summary = {"total_return": 0.1}
        rebalance_log = __import__("pandas").DataFrame([{"date": "2024-01-31", "turnover": 0.1}])
        scenario_summary = __import__("pandas").DataFrame([{"scenario": "shock", "scenario_total_return": -0.2}])
        analytics = {"capacity_warning": {"status": "warning", "message": "too large"}}

    monkeypatch.setattr("druck.web.app._load_cfg", lambda: {"backtest": {}})
    monkeypatch.setattr("druck.web.app.run_backtest", lambda cfg: DummyResult())

    resp = client.post("/api/backtest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["data"]["scenario_summary"][0]["scenario"] == "shock"
    assert body["data"]["analytics"]["capacity_warning"]["status"] == "warning"


def test_dashboard_template_contains_backtest_sections():
    from pathlib import Path

    dashboard_path = Path(__file__).resolve().parents[1] / "druck" / "web" / "templates" / "dashboard.html"
    text = dashboard_path.read_text(encoding="utf-8")
    assert "Backtest Snapshot" in text
    assert "Scenario Summary" in text
    assert "Recent Rebalance Rows" in text
    assert "backtest-scenario-warning" in text
    assert "backtest-scenario-tags" in text


def test_status_api_surfaces_backtest_capacity_warning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = TestClient(app)

    import druck.web.app as web_app

    web_app._backtest_latest = {
        "summary": {},
        "rows": [],
        "scenario_summary": [
            {
                "scenario": "benchmark_gap_down",
                "severity": "high",
                "tags": ["stress", "benchmark", "gap"],
                "benchmark_relative_return": -0.08,
            }
        ],
        "analytics": {
            "capacity_warning": {
                "status": "warning",
                "message": "portfolio size exceeds estimated safe capacity",
            }
        },
    }

    resp = client.get("/api/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["warnings"]["backtest_capacity_warning"]["status"] == "warning"
    assert body["warnings"]["backtest_capacity_warning"]["priority"] == 1
    assert body["warnings"]["backtest_scenario_warning"]["severity"] == "high"
    assert body["warnings"]["backtest_scenario_warning"]["priority"] == 2
    assert body["warnings"]["backtest_scenario_warning"]["message"] == "high severity backtest scenario detected"
    assert body["warnings"]["backtest_scenario_warning"]["scenario"] == "benchmark_gap_down"
