from druck.db import fetch_runtime_events, fetch_trade_audit, init_db, log_runtime_event, log_trade_audit, resolve_runtime_event


def test_trade_audit_roundtrip(tmp_path):
    db_path = tmp_path / "trade.db"
    conn = init_db(str(db_path))
    log_trade_audit(conn, "order_intent", ticker="SPY", side="BUY", qty=3, status="planned", detail="test")
    rows = fetch_trade_audit(conn)
    assert rows[0]["event_type"] == "order_intent"
    assert rows[0]["ticker"] == "SPY"


def test_runtime_event_roundtrip(tmp_path):
    db_path = tmp_path / "trade.db"
    conn = init_db(str(db_path))
    log_runtime_event(conn, category="system_error", message="boom", detail="trace")
    rows = fetch_runtime_events(conn)
    assert rows[0]["category"] == "system_error"
    assert rows[0]["message"] == "boom"
    assert rows[0]["status"] == "open"

    resolve_runtime_event(conn, event_id=rows[0]["id"], status="resolved", resolution_note="checked")
    rows = fetch_runtime_events(conn)
    assert rows[0]["status"] == "resolved"
    assert rows[0]["resolution_note"] == "checked"
