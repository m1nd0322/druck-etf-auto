from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .db import init_db, log_runtime_event


class StrategyHaltError(RuntimeError):
    pass


@dataclass
class RuntimeEvent:
    category: str
    message: str
    detail: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeResult:
    ok: bool
    halted: bool
    category: str
    message: str
    detail: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


Reporter = Callable[[RuntimeEvent], None]


def report_event(reporter: Reporter | None, event: RuntimeEvent) -> None:
    if reporter is None:
        return
    try:
        reporter(event)
    except Exception:
        pass


def db_runtime_reporter(db_path: str = "trade_log.db") -> Reporter:
    def reporter(event: RuntimeEvent):
        conn = init_db(str(Path(db_path)))
        try:
            log_runtime_event(
                conn,
                category=event.category,
                message=event.message,
                detail=event.detail,
                payload=json.dumps(event.payload, default=str),
            )
        finally:
            conn.close()
    return reporter


def run_guarded(fn: Callable[[], dict[str, Any]], reporter: Reporter | None = None) -> RuntimeResult:
    try:
        payload = fn() or {}
        halted = bool(payload.get("strategy_halt", False))
        if halted:
            event = RuntimeEvent(
                category="strategy_halt",
                message=str(payload.get("halt_reason", "strategy_halt")),
                detail=str(payload.get("halt_detail", "")),
                payload=payload,
            )
            report_event(reporter, event)
            return RuntimeResult(
                ok=False,
                halted=True,
                category="strategy_halt",
                message=event.message,
                detail=event.detail,
                payload=payload,
            )
        return RuntimeResult(ok=True, halted=False, category="ok", message="ok", payload=payload)
    except StrategyHaltError as exc:
        event = RuntimeEvent(category="strategy_halt", message=str(exc))
        report_event(reporter, event)
        return RuntimeResult(ok=False, halted=True, category="strategy_halt", message=str(exc))
    except Exception as exc:
        event = RuntimeEvent(category="system_error", message=str(exc))
        report_event(reporter, event)
        return RuntimeResult(ok=False, halted=False, category="system_error", message=str(exc))
