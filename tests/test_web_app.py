from fastapi.testclient import TestClient

from druck.web.app import app, _format_regime_result


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
        analytics = {
            "capacity_warning": {"status": "warning", "message": "too large"},
            "sleeve_relative_warning": {"status": "warning", "message": "weak sleeve strength", "weak_sleeves": ["factor"], "avg_relative_strength": -0.03, "benchmark_relative_fail_count": 2},
            "selection_score_comparison": {"avg_score_uplift": 0.12, "avg_persistence": 0.55},
            "strategy_comparison": {
                "robustness_summary": "enhanced wins 2, loses 1 across shared scenarios",
                "return_delta": 0.04,
                "active_return_delta": 0.03,
                "turnover_delta": 0.01,
            },
        }

    monkeypatch.setattr("druck.web.app._load_cfg", lambda: {"backtest": {}})
    monkeypatch.setattr("druck.web.app.run_backtest", lambda cfg: DummyResult())

    resp = client.post("/api/backtest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["data"]["scenario_summary"][0]["scenario"] == "shock"
    assert body["data"]["analytics"]["capacity_warning"]["status"] == "warning"
    assert body["data"]["analytics"]["selection_score_comparison"]["avg_score_uplift"] == 0.12
    assert body["data"]["analytics"]["strategy_comparison"]["robustness_summary"].startswith("enhanced wins")


def test_format_regime_result_includes_rotation_policy_and_sleeve_mix():
    import types
    import pandas as pd

    result = {
        "regime": types.SimpleNamespace(state="RISK_ON", risk_score=0.71, details={"spy_trend": 0.8}),
        "scores": pd.DataFrame(
            {
                "score": [1.2, 1.1],
                "momentum": [0.2, 0.1],
                "trend": [1.0, 0.8],
                "vol": [0.1, 0.2],
                "mdd_1y": [-0.05, -0.08],
            },
            index=["MTUM", "SPY"],
        ),
        "target_weights": pd.Series({"MTUM": 0.6, "SPY": 0.4}),
        "report_path": "output/report_test.md",
        "strategy_halt": False,
        "halt_reason": "",
        "halt_detail": "",
        "rotation_policy": {
            "enabled": True,
            "top_n": 2,
            "preferred_sleeves": ["factor"],
            "sleeve_budget": {"factor": 0.7, "core": 0.3},
            "score_tilt": {"factor": 0.2},
        },
        "selected_sleeves": {"MTUM": "factor", "SPY": "core"},
    }
    body = _format_regime_result(result)
    assert body["rotation_policy"]["enabled"] is True
    assert body["rotation_policy"]["top_n"] == 2
    assert body["selected_sleeves"]["MTUM"] == "factor"
    assert body["selected_sleeve_mix"]["factor"] == 60.0
    assert body["etfs"][0]["sleeve"] in {"factor", "core"}


def test_dashboard_template_contains_backtest_sections():
    from pathlib import Path

    dashboard_path = Path(__file__).resolve().parents[1] / "druck" / "web" / "templates" / "dashboard.html"
    text = dashboard_path.read_text(encoding="utf-8")
    assert "Backtest Snapshot" in text
    assert "Selection Score Comparison" in text
    assert "backtest-robustness-summary" in text
    assert "Legacy vs Alpha Top Picks" in text
    assert "Scenario Summary" in text
    assert "Recent Rebalance Rows" in text
    assert "backtest-scenario-warning" in text
    assert "backtest-scenario-ack" in text
    assert "ackScenarioWarning" in text
    assert "backtest-scenario-tags" in text
    assert "btn-filter-active" in text
    assert "tag-count" in text
    assert "note template" in text
    assert "review required" in text or "operator_action" in text

    result_partial = (dashboard_path.parent / "_result.html").read_text(encoding="utf-8")
    assert "Regime Sleeve Rotation" in result_partial
    assert "Selected Mix %" in result_partial
    assert "Preferred sleeves" in result_partial
    assert "Sleeve" in result_partial


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
                "operator_action": "compare active risk versus benchmark and review hedge stance",
                "review_required": True,
                "note_template": "Check benchmark-relative weakness and decide whether to de-risk or hold.",
                "benchmark_relative_return": -0.08,
            }
        ],
        "analytics": {
            "capacity_warning": {
                "status": "warning",
                "message": "portfolio size exceeds estimated safe capacity",
            },
            "sleeve_relative_warning": {
                "status": "warning",
                "message": "selected sleeve mix shows weak benchmark-relative strength",
                "weak_sleeves": ["factor", "sector"],
                "avg_relative_strength": -0.02,
                "benchmark_relative_fail_count": 2,
            },
            "strategy_comparison": {
                "robustness_summary": "enhanced wins 2, loses 1 across shared scenarios",
                "return_delta": 0.04,
                "active_return_delta": 0.03,
                "turnover_delta": 0.01,
            },
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
    assert body["warnings"]["backtest_scenario_warning"]["review_required"] is True
    assert body["warnings"]["backtest_scenario_warning"]["operator_action"] == "compare active risk versus benchmark and review hedge stance"
    assert body["warnings"]["backtest_sleeve_relative_warning"]["priority"] == 2
    assert body["warnings"]["backtest_sleeve_relative_warning"]["weak_sleeves"] == ["factor", "sector"]
    assert body["warnings"]["strategy_comparison_summary"]["priority"] == 3
