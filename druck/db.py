from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import Any


def init_db(path: str = "trade_log.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS fills (
        timestamp TEXT,
        code TEXT,
        qty INTEGER,
        price REAL,
        side TEXT
    )
    """
    )
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS trade_audit (
        timestamp TEXT,
        event_type TEXT,
        ticker TEXT,
        side TEXT,
        qty INTEGER,
        status TEXT,
        detail TEXT
    )
    """
    )
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS operator_ack (
        timestamp TEXT,
        ack_type TEXT,
        status TEXT,
        note TEXT
    )
    """
    )
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS runtime_events (
        timestamp TEXT,
        category TEXT,
        message TEXT,
        detail TEXT,
        payload TEXT
    )
    """
    )
    conn.commit()
    return conn


def log_fill(conn: sqlite3.Connection, code: str, qty: int, price: float, side: str):
    c = conn.cursor()
    c.execute(
        "INSERT INTO fills VALUES (?,?,?,?,?)",
        (datetime.now().isoformat(), code, int(qty), float(price), str(side)),
    )
    conn.commit()


def log_trade_audit(
    conn: sqlite3.Connection,
    event_type: str,
    ticker: str = "",
    side: str = "",
    qty: int = 0,
    status: str = "info",
    detail: str = "",
):
    c = conn.cursor()
    c.execute(
        "INSERT INTO trade_audit VALUES (?,?,?,?,?,?,?)",
        (datetime.now().isoformat(), event_type, ticker, side, int(qty), status, detail),
    )
    conn.commit()



def fetch_trade_audit(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    c = conn.cursor()
    rows = c.execute(
        "SELECT timestamp, event_type, ticker, side, qty, status, detail FROM trade_audit ORDER BY timestamp DESC"
    ).fetchall()
    return [
        {
            "timestamp": row[0],
            "event_type": row[1],
            "ticker": row[2],
            "side": row[3],
            "qty": row[4],
            "status": row[5],
            "detail": row[6],
        }
        for row in rows
    ]



def log_operator_ack(
    conn: sqlite3.Connection,
    ack_type: str,
    status: str = "acknowledged",
    note: str = "",
):
    c = conn.cursor()
    c.execute(
        "INSERT INTO operator_ack VALUES (?,?,?,?)",
        (datetime.now().isoformat(), ack_type, status, note),
    )
    conn.commit()



def fetch_operator_ack(conn: sqlite3.Connection, ack_type: str | None = None) -> list[dict[str, Any]]:
    c = conn.cursor()
    if ack_type:
        rows = c.execute(
            "SELECT timestamp, ack_type, status, note FROM operator_ack WHERE ack_type=? ORDER BY timestamp DESC",
            (ack_type,),
        ).fetchall()
    else:
        rows = c.execute(
            "SELECT timestamp, ack_type, status, note FROM operator_ack ORDER BY timestamp DESC"
        ).fetchall()
    return [
        {
            "timestamp": row[0],
            "ack_type": row[1],
            "status": row[2],
            "note": row[3],
        }
        for row in rows
    ]



def log_runtime_event(
    conn: sqlite3.Connection,
    category: str,
    message: str,
    detail: str = "",
    payload: str = "",
):
    c = conn.cursor()
    c.execute(
        "INSERT INTO runtime_events VALUES (?,?,?,?,?)",
        (datetime.now().isoformat(), category, message, detail, payload),
    )
    conn.commit()



def fetch_runtime_events(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    c = conn.cursor()
    rows = c.execute(
        "SELECT timestamp, category, message, detail, payload FROM runtime_events ORDER BY timestamp DESC"
    ).fetchall()
    return [
        {
            "timestamp": row[0],
            "category": row[1],
            "message": row[2],
            "detail": row[3],
            "payload": row[4],
        }
        for row in rows
    ]
