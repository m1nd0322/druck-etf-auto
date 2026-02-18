from __future__ import annotations
import sqlite3
from datetime import datetime

def init_db(path: str = "trade_log.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS fills (
        timestamp TEXT,
        code TEXT,
        qty INTEGER,
        price REAL,
        side TEXT
    )
    """)
    conn.commit()
    return conn

def log_fill(conn: sqlite3.Connection, code: str, qty: int, price: float, side: str):
    c = conn.cursor()
    c.execute(
        "INSERT INTO fills VALUES (?,?,?,?,?)",
        (datetime.now().isoformat(), code, int(qty), float(price), str(side)),
    )
    conn.commit()
