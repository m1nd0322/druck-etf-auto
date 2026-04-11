from __future__ import annotations

import sqlite3
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..backtest import run_backtest
from ..config import load_config
from ..db import fetch_trade_audit
from ..engine import run_once

_HERE = Path(__file__).resolve().parent


def _root() -> Path:
    return Path.cwd()

app = FastAPI(title="Druck ETF Auto")
app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
templates = Jinja2Templates(directory=str(_HERE / "templates"))


def _load_cfg() -> dict:
    root = _root()
    return load_config(root / "config.yaml", root / "config.local.yaml")


def _list_reports() -> List[Dict[str, Any]]:
    out_dir = _root() / "output"
    if not out_dir.exists():
        return []
    reports = []
    for f in sorted(out_dir.glob("report_*.md"), reverse=True):
        ts_str = f.stem.replace("report_", "")
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            label = ts.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            label = ts_str
        reports.append({"filename": f.name, "label": label, "path": str(f)})
    return reports


def _read_report(filename: str) -> str | None:
    p = _root() / "output" / filename
    if p.exists() and p.suffix == ".md":
        return p.read_text(encoding="utf-8")
    return None


def _read_trade_audit(limit: int = 50) -> list[dict[str, Any]]:
    db_path = _root() / "trade_log.db"
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = fetch_trade_audit(conn)
        return rows[:limit]
    finally:
        conn.close()


def _format_regime_result(result: dict) -> dict:
    regime = result["regime"]
    scores: pd.DataFrame = result["scores"]
    weights: pd.Series = result["target_weights"]
    trade_plan = result.get("trade_plan")
    trade_review = result.get("trade_review")

    etfs = []
    for ticker in weights.index:
        w = float(weights.get(ticker, 0))
        if w <= 0:
            continue
        row: Dict[str, Any] = {"ticker": ticker, "weight": round(w * 100, 2)}
        if ticker in scores.index:
            s = scores.loc[ticker]
            row["score"] = round(float(s.get("score", 0)), 4)
            row["momentum"] = round(float(s.get("momentum", 0)), 4)
            row["trend"] = round(float(s.get("trend", 0)), 4)
            row["vol"] = round(float(s.get("vol", 0)), 4)
            row["mdd"] = round(float(s.get("mdd_1y", 0)), 4)
        etfs.append(row)
    etfs.sort(key=lambda x: x["weight"], reverse=True)

    details = {}
    for k, v in regime.details.items():
        try:
            details[k] = round(float(v), 4) if v is not None else None
        except (TypeError, ValueError):
            details[k] = str(v)

    trade_summary = None
    if trade_plan is not None:
        trade_summary = {
            "orders": [
                {
                    "ticker": o.ticker,
                    "side": o.side,
                    "qty": o.qty,
                    "est_notional": round(o.est_notional, 2),
                }
                for o in trade_plan.orders
            ],
            "skipped": trade_plan.skipped,
            "warnings": trade_plan.warnings,
            "portfolio_value": round(trade_plan.portfolio_value, 2),
            "cash_available": round(trade_plan.cash_available, 2),
            "review": trade_review.checks if trade_review is not None else [],
        }

    return {
        "state": regime.state,
        "risk_score": round(regime.risk_score, 4),
        "details": details,
        "etfs": etfs,
        "report_path": result.get("report_path", ""),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_plan": trade_summary,
    }


_latest: dict | None = None
_backtest_latest: dict | None = None


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    reports = _list_reports()
    audit_rows = _read_trade_audit()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "latest": _latest,
            "reports": reports[:20],
            "audit_rows": audit_rows,
        },
    )


@app.post("/api/run", response_class=JSONResponse)
async def api_run():
    global _latest
    try:
        cfg = _load_cfg()
        result = run_once(cfg, do_trade=False)
        _latest = _format_regime_result(result)
        return {"ok": True, "data": _latest}
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc), "trace": traceback.format_exc()},
        )


@app.post("/api/backtest", response_class=JSONResponse)
async def api_backtest():
    global _backtest_latest
    try:
        cfg = _load_cfg()
        result = run_backtest(cfg)
        _backtest_latest = {
            "summary": result.summary,
            "rows": result.rebalance_log.to_dict(orient="records"),
        }
        return {"ok": True, "data": _backtest_latest}
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc), "trace": traceback.format_exc()},
        )


@app.get("/api/reports", response_class=JSONResponse)
async def api_reports():
    return {"reports": _list_reports()}


@app.get("/api/reports/{filename}", response_class=JSONResponse)
async def api_report_detail(filename: str):
    content = _read_report(filename)
    if content is None:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return {"filename": filename, "content": content}


@app.get("/api/audit", response_class=JSONResponse)
async def api_audit():
    return {"rows": _read_trade_audit(limit=200)}


@app.get("/report/{filename}", response_class=HTMLResponse)
async def report_page(request: Request, filename: str):
    content = _read_report(filename)
    return templates.TemplateResponse(
        "report.html",
        {
            "request": request,
            "filename": filename,
            "content": content,
        },
    )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "reports": _list_reports(),
        },
    )


@app.get("/api/status", response_class=JSONResponse)
async def api_status():
    return {
        "latest": _latest,
        "backtest": _backtest_latest,
        "reports": _list_reports()[:10],
        "audit": _read_trade_audit(limit=20),
    }
