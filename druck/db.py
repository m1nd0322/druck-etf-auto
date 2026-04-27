from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import Any


RUNTIME_EVENTS_COLUMNS = [
    "id",
    "timestamp",
    "category",
    "message",
    "detail",
    "payload",
    "status",
    "resolution_note",
]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    c = conn.cursor()
    rows = c.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


def _ensure_runtime_events_schema(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS runtime_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        category TEXT,
        message TEXT,
        detail TEXT,
        payload TEXT,
        status TEXT,
        resolution_note TEXT
    )
    """
    )
    columns = _table_columns(conn, "runtime_events")
    if columns == RUNTIME_EVENTS_COLUMNS:
        conn.commit()
        return

    # Legacy table without id/status/resolution_note support.
    if columns and columns == ["timestamp", "category", "message", "detail", "payload"]:
        c.execute("ALTER TABLE runtime_events RENAME TO runtime_events_legacy")
        c.execute(
            """
        CREATE TABLE runtime_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            category TEXT,
            message TEXT,
            detail TEXT,
            payload TEXT,
            status TEXT,
            resolution_note TEXT
        )
        """
        )
        c.execute(
            """
        INSERT INTO runtime_events (timestamp, category, message, detail, payload, status, resolution_note)
        SELECT timestamp, category, message, detail, payload, 'open', ''
        FROM runtime_events_legacy
        """
        )
        c.execute("DROP TABLE runtime_events_legacy")
        conn.commit()
        return

    missing = set(RUNTIME_EVENTS_COLUMNS) - set(columns)
    if missing:
        raise sqlite3.OperationalError(
            f"unsupported runtime_events schema: columns={columns}, missing={sorted(missing)}"
        )
    conn.commit()


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
    CREATE TABLE IF NOT EXISTS order_operations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        action_type TEXT,
        ticker TEXT,
        side TEXT,
        api_id TEXT,
        status_code INTEGER,
        return_code TEXT,
        return_msg TEXT,
        request_summary TEXT,
        response_summary TEXT,
        order_ref TEXT,
        success INTEGER
    )
    """
    )
    _ensure_runtime_events_schema(conn)
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


def log_order_operation(
    conn: sqlite3.Connection,
    *,
    action_type: str,
    ticker: str = "",
    side: str = "",
    api_id: str = "",
    status_code: int | None = None,
    return_code: str = "",
    return_msg: str = "",
    request_summary: str = "",
    response_summary: str = "",
    order_ref: str = "",
    success: bool | None = None,
):
    c = conn.cursor()
    c.execute(
        "INSERT INTO order_operations (timestamp, action_type, ticker, side, api_id, status_code, return_code, return_msg, request_summary, response_summary, order_ref, success) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            datetime.now().isoformat(),
            action_type,
            ticker,
            side,
            api_id,
            status_code,
            return_code,
            return_msg,
            request_summary,
            response_summary,
            order_ref,
            None if success is None else int(bool(success)),
        ),
    )
    conn.commit()


def fetch_order_operations(conn: sqlite3.Connection, limit: int = 100) -> list[dict[str, Any]]:
    c = conn.cursor()
    rows = c.execute(
        "SELECT id, timestamp, action_type, ticker, side, api_id, status_code, return_code, return_msg, request_summary, response_summary, order_ref, success FROM order_operations ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "id": row[0],
            "timestamp": row[1],
            "action_type": row[2],
            "ticker": row[3],
            "side": row[4],
            "api_id": row[5],
            "status_code": row[6],
            "return_code": row[7],
            "return_msg": row[8],
            "request_summary": row[9],
            "response_summary": row[10],
            "order_ref": row[11],
            "success": None if row[12] is None else bool(row[12]),
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
    status: str = "open",
    resolution_note: str = "",
):
    c = conn.cursor()
    c.execute(
        "INSERT INTO runtime_events (timestamp, category, message, detail, payload, status, resolution_note) VALUES (?,?,?,?,?,?,?)",
        (datetime.now().isoformat(), category, message, detail, payload, status, resolution_note),
    )
    conn.commit()



def resolve_runtime_event(
    conn: sqlite3.Connection,
    event_id: int,
    status: str = "resolved",
    resolution_note: str = "",
):
    c = conn.cursor()
    c.execute(
        "UPDATE runtime_events SET status=?, resolution_note=? WHERE id=?",
        (status, resolution_note, int(event_id)),
    )
    conn.commit()



def fetch_runtime_events(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    c = conn.cursor()
    rows = c.execute(
        "SELECT id, timestamp, category, message, detail, payload, status, resolution_note FROM runtime_events ORDER BY timestamp DESC"
    ).fetchall()
    return [
        {
            "id": row[0],
            "timestamp": row[1],
            "category": row[2],
            "message": row[3],
            "detail": row[4],
            "payload": row[5],
            "status": row[6],
            "resolution_note": row[7],
        }
        for row in rows
    ]
