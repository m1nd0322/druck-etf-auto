from druck.db import fetch_trade_audit, init_db, log_trade_audit


def test_trade_audit_roundtrip(tmp_path):
    db_path = tmp_path / "trade.db"
    conn = init_db(str(db_path))
    log_trade_audit(conn, "order_intent", ticker="SPY", side="BUY", qty=3, status="planned", detail="test")
    rows = fetch_trade_audit(conn)
    assert rows[0]["event_type"] == "order_intent"
    assert rows[0]["ticker"] == "SPY"
