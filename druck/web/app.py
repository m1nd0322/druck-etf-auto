from __future__ import annotations

import logging
import math
import sqlite3
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
from ..db import fetch_operator_ack, fetch_order_operations, fetch_runtime_events, fetch_trade_audit, init_db, log_operator_ack, resolve_runtime_event
from ..engine import run_once
from ..notifier import send_telegram

_HERE = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)


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


_NAME_CACHE: dict[str, str] | None = None


def _symbol_variants(symbol: str) -> list[str]:
    raw = str(symbol or '').strip()
    if not raw:
        return []
    variants: list[str] = []
    for candidate in [raw, raw.upper()]:
        if candidate and candidate not in variants:
            variants.append(candidate)
    base = raw[:-3] if raw.upper().endswith('.KS') else raw
    for candidate in [base, base.upper()]:
        if candidate and candidate not in variants:
            variants.append(candidate)
    if base.isdigit() and len(base) == 6:
        for candidate in [f'{base}.KS', f'{base}.KS'.upper()]:
            if candidate not in variants:
                variants.append(candidate)
    return variants


def _lookup_ticker_name(symbol: str) -> str:
    names = _load_ticker_names()
    for candidate in _symbol_variants(symbol):
        if candidate in names and names[candidate]:
            return names[candidate]
    return str(symbol)


def _load_ticker_names() -> dict[str, str]:
    global _NAME_CACHE
    if _NAME_CACHE is not None:
        return _NAME_CACHE

    names: dict[str, str] = {}
    listings_root = _root() / 'data' / 'market_data' / 'listings'
    for filename in ['kr_etf.parquet', 'krx_kospi.parquet', 'krx_kosdaq.parquet', 'us_etf.parquet', 'us_nasdaq.parquet', 'us_sp500.parquet']:
        path = listings_root / filename
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            print(f'[web] ticker-name load failed for {path}: {exc}')
            continue
        if df.empty:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        symbol_col = cols.get('symbol') or cols.get('code') or cols.get('ticker')
        name_col = cols.get('name') or cols.get('nm')
        if not symbol_col or not name_col:
            print(f'[web] ticker-name skipped for {path}: missing symbol/name columns in {list(df.columns)}')
            continue
        slim = df[[symbol_col, name_col]].dropna().copy()
        for _, row in slim.iterrows():
            raw_symbol = str(row[symbol_col]).strip()
            raw_name = str(row[name_col]).strip()
            if not raw_symbol or not raw_name:
                continue
            for candidate in _symbol_variants(raw_symbol):
                names.setdefault(candidate, raw_name)

    print(f'[web] ticker-name cache loaded: {len(names)} symbols from {listings_root}')
    _NAME_CACHE = names
    return _NAME_CACHE



def _db_conn() -> sqlite3.Connection | None:
    db_path = _root() / "trade_log.db"
    if not db_path.exists():
        return None
    return sqlite3.connect(db_path)



def _read_trade_audit(limit: int = 50) -> list[dict[str, Any]]:
    conn = _db_conn()
    if conn is None:
        return []
    try:
        rows = fetch_trade_audit(conn)
        return rows[:limit]
    finally:
        conn.close()



def _read_order_operations(limit: int = 50) -> list[dict[str, Any]]:
    conn = _db_conn()
    if conn is None:
        return []
    try:
        rows = fetch_order_operations(conn, limit=limit)
        return rows[:limit]
    finally:
        conn.close()



def _read_operator_ack(limit: int = 20) -> list[dict[str, Any]]:
    conn = _db_conn()
    if conn is None:
        return []
    try:
        rows = fetch_operator_ack(conn)
        return rows[:limit]
    finally:
        conn.close()



def _read_runtime_events(limit: int = 20) -> list[dict[str, Any]]:
    conn = _db_conn()
    if conn is None:
        return []
    try:
        rows = fetch_runtime_events(conn)
        return rows[:limit]
    finally:
        conn.close()



def _num(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(value) or math.isinf(value):
        return default
    return value


def _format_regime_result(result: dict) -> dict:
    regime = result["regime"]
    scores: pd.DataFrame = result["scores"]
    weights: pd.Series = result["target_weights"]
    trade_plan = result.get("trade_plan")
    trade_review = result.get("trade_review")
    rebalance_result = result.get("rebalance_result")

    rotation_policy = result.get("rotation_policy") or {}
    selected_sleeves = result.get("selected_sleeves") or {}
    provider_warnings = result.get("provider_warnings") or []

    etfs = []
    for ticker in weights.index:
        w = _num(weights.get(ticker, 0))
        if w <= 0:
            continue
        row: Dict[str, Any] = {"ticker": ticker, "name": _lookup_ticker_name(ticker), "weight": round(w * 100, 2), "sleeve": selected_sleeves.get(ticker, "core")}
        if ticker in scores.index:
            s = scores.loc[ticker]
            row["score"] = round(_num(s.get("score", 0)), 4)
            row["momentum"] = round(_num(s.get("momentum", 0)), 4)
            row["trend"] = round(_num(s.get("trend", 0)), 4)
            row["vol"] = round(_num(s.get("vol", 0)), 4)
            row["mdd"] = round(_num(s.get("mdd_1y", 0)), 4)
            row["relative_strength"] = round(_num(s.get("relative_strength_6m", 0)), 4)
            row["capacity_score"] = round(_num(s.get("capacity_score", 0)), 4)
            row["diversification_score"] = round(_num(s.get("diversification_score", 0)), 4)
            row["diversification_penalty"] = round(_num(s.get("diversification_penalty", 0)), 4)
            row["residual_strength"] = round(_num(s.get("residual_strength", 0)), 4)
        etfs.append(row)
    etfs.sort(key=lambda x: x["weight"], reverse=True)

    details = {}
    for k, v in regime.details.items():
        try:
            details[k] = round(_num(v), 4) if v is not None else None
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
                    "est_notional": round(_num(o.est_notional), 2),
                }
                for o in trade_plan.orders
            ],
            "skipped": trade_plan.skipped,
            "warnings": trade_plan.warnings,
            "portfolio_value": round(_num(trade_plan.portfolio_value), 2),
            "cash_available": round(_num(trade_plan.cash_available), 2),
            "review": trade_review.checks if trade_review is not None else [],
        }

    rebalance_summary = None
    if rebalance_result is not None:
        rebalance_summary = {
            "needs_replan": rebalance_result.needs_replan,
            "detail": rebalance_result.detail,
            "operator_ack_required": rebalance_result.operator_ack_required,
            "operator_ack_state": rebalance_result.operator_ack_state,
            "executions": rebalance_result.executions,
        }

    sleeve_mix: dict[str, float] = {}
    for ticker in weights.index:
        w = _num(weights.get(ticker, 0.0))
        if w <= 0:
            continue
        sleeve = selected_sleeves.get(ticker, "core")
        sleeve_mix[sleeve] = sleeve_mix.get(sleeve, 0.0) + w
    sleeve_mix = {k: round(v * 100, 2) for k, v in sorted(sleeve_mix.items(), key=lambda item: item[1], reverse=True)}

    score_diagnostics = {}
    if not scores.empty:
        selected_scores = scores.loc[[ticker for ticker in weights.index if ticker in scores.index]].copy()
        if not selected_scores.empty:
            score_diagnostics = {
                "avg_relative_strength": round(_num(selected_scores.get("relative_strength_6m", pd.Series(dtype=float)).mean()), 4) if "relative_strength_6m" in selected_scores.columns else 0.0,
                "avg_capacity_score": round(_num(selected_scores.get("capacity_score", pd.Series(dtype=float)).mean()), 4) if "capacity_score" in selected_scores.columns else 0.0,
                "avg_diversification_score": round(_num(selected_scores.get("diversification_score", pd.Series(dtype=float)).mean()), 4) if "diversification_score" in selected_scores.columns else 0.0,
                "avg_diversification_penalty": round(_num(selected_scores.get("diversification_penalty", pd.Series(dtype=float)).mean()), 4) if "diversification_penalty" in selected_scores.columns else 0.0,
                "avg_residual_strength": round(_num(selected_scores.get("residual_strength", pd.Series(dtype=float)).mean()), 4) if "residual_strength" in selected_scores.columns else 0.0,
            }

    return {
        "state": regime.state,
        "risk_score": round(_num(regime.risk_score), 4),
        "details": details,
        "etfs": etfs,
        "score_diagnostics": score_diagnostics,
        "report_path": result.get("report_path", ""),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_plan": trade_summary,
        "rebalance": rebalance_summary,
        "strategy_halt": bool(result.get("strategy_halt", False)),
        "halt_reason": result.get("halt_reason", ""),
        "halt_detail": result.get("halt_detail", ""),
        "rotation_policy": {
            "enabled": bool(rotation_policy.get("enabled", False)),
            "top_n": rotation_policy.get("top_n"),
            "preferred_sleeves": list(rotation_policy.get("preferred_sleeves", []) or []),
            "sleeve_budget": rotation_policy.get("sleeve_budget", {}) or {},
            "score_tilt": rotation_policy.get("score_tilt", {}) or {},
        },
        "selected_sleeves": selected_sleeves,
        "selected_sleeve_mix": sleeve_mix,
        "provider_warnings": provider_warnings,
    }


_latest: dict | None = None
_backtest_latest: dict | None = None


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _status_warnings() -> dict[str, Any]:
    backtest_warning = None
    scenario_warning = None
    sleeve_relative_warning = None
    strategy_summary = None
    if _backtest_latest is not None:
        analytics = _backtest_latest.get("analytics", {}) or {}
        raw_capacity_warning = analytics.get("capacity_warning")
        if raw_capacity_warning is not None:
            backtest_warning = {
                **raw_capacity_warning,
                "priority": 1,
                "message": raw_capacity_warning.get("message", "portfolio size exceeds estimated safe capacity"),
            }
        scenarios = _backtest_latest.get("scenario_summary", []) or []
        high_severity = [row for row in scenarios if str(row.get("severity", "")).lower() == "high"]
        if high_severity:
            top = high_severity[0]
            scenario_warning = {
                "status": "warning",
                "priority": 2,
                "message": "high severity backtest scenario detected",
                "scenario": top.get("scenario"),
                "severity": top.get("severity"),
                "tags": top.get("tags", []),
                "operator_action": top.get("operator_action"),
                "review_required": top.get("review_required"),
                "note_template": top.get("note_template"),
                "benchmark_relative_return": top.get("benchmark_relative_return"),
            }
        raw_sleeve_warning = analytics.get("sleeve_relative_warning")
        if raw_sleeve_warning is not None:
            sleeve_relative_warning = {
                **raw_sleeve_warning,
                "priority": 2,
            }
        strategy_comp = analytics.get("strategy_comparison") or {}
        if strategy_comp.get("robustness_summary"):
            strategy_summary = {
                "status": "info",
                "priority": 3,
                "message": strategy_comp.get("robustness_summary"),
                "return_delta": strategy_comp.get("return_delta"),
                "active_return_delta": strategy_comp.get("active_return_delta"),
                "turnover_delta": strategy_comp.get("turnover_delta"),
            }
    return {
        "backtest_capacity_warning": backtest_warning,
        "backtest_scenario_warning": scenario_warning,
        "backtest_sleeve_relative_warning": sleeve_relative_warning,
        "strategy_comparison_summary": strategy_summary,
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    reports = _list_reports()
    audit_rows = _read_trade_audit()
    order_rows = _read_order_operations()
    ack_rows = _read_operator_ack()
    runtime_rows = _read_runtime_events()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "latest": _latest,
            "reports": reports[:20],
            "audit_rows": audit_rows,
            "order_rows": order_rows,
            "ack_rows": ack_rows,
            "runtime_rows": runtime_rows,
        },
    )


@app.post("/api/run", response_class=JSONResponse)
async def api_run():
    global _latest
    try:
        cfg = _load_cfg()
        result = run_once(cfg, do_trade=False)
        _latest = _json_safe(_format_regime_result(result))
        try:
            etf_lines = []
            for etf in (_latest.get('etfs') or [])[:10]:
                name = etf.get('name') or _lookup_ticker_name(etf.get('ticker'))
                etf_lines.append(f"- {name} ({etf.get('ticker')}): {etf.get('weight', 0):.1f}%")
            msg_lines = [
                "[Druck ETF] Run Report 완료",
                f"상태: {_latest.get('state')}",
                f"리스크 점수: {_latest.get('risk_score')}",
                f"리포트: {_latest.get('report_path')}",
            ]
            if etf_lines:
                msg_lines.append("")
                msg_lines.append("Selected ETFs")
                msg_lines.extend(etf_lines)
            send_telegram(_load_cfg(), "\n".join(msg_lines))
        except Exception:
            pass
        return {"ok": True, "data": _latest, "debug": {"top_etf_names": [{"ticker": row.get("ticker"), "name": row.get("name")} for row in (_latest.get("etfs") or [])[:10]], "ticker_name_cache_size": len(_load_ticker_names())}}
    except Exception:
        logger.exception("report API failed")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "internal server error"},
        )


@app.post("/api/backtest", response_class=JSONResponse)
async def api_backtest():
    global _backtest_latest
    try:
        cfg = _load_cfg()
        result = run_backtest(cfg)
        _backtest_latest = _json_safe({
            "summary": result.summary,
            "rows": result.rebalance_log.to_dict(orient="records"),
            "scenario_summary": result.scenario_summary.to_dict(orient="records") if result.scenario_summary is not None else [],
            "analytics": result.analytics or {},
        })
        return {"ok": True, "data": _backtest_latest}
    except RuntimeError as exc:
        message = str(exc)
        user_message = message
        if message == "Not enough history to score universe":
            user_message = "백테스트 점수 계산에 필요한 충분한 가격 이력이 없습니다. 현재 유니버스/기간/데이터 상태를 확인해 주세요."
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": user_message, "raw_error": message},
        )
    except Exception:
        logger.exception("backtest API failed")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "internal server error"},
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


@app.get("/api/orders", response_class=JSONResponse)
async def api_orders():
    return {"rows": _read_order_operations(limit=200)}


@app.get("/api/ack", response_class=JSONResponse)
async def api_ack():
    return {"rows": _read_operator_ack(limit=200)}


@app.get("/api/runtime", response_class=JSONResponse)
async def api_runtime():
    return {"rows": _read_runtime_events(limit=200)}


@app.post("/api/runtime/{event_id}/resolve", response_class=JSONResponse)
async def api_runtime_resolve(event_id: int, payload: dict):
    status = str(payload.get("status", "resolved")).strip() or "resolved"
    note = str(payload.get("note", "")).strip()
    db_path = _root() / "trade_log.db"
    conn = init_db(str(db_path))
    try:
        resolve_runtime_event(conn, event_id=event_id, status=status, resolution_note=note)
        rows = fetch_runtime_events(conn)
        row = next((r for r in rows if int(r["id"]) == int(event_id)), None)
        return {"ok": True, "row": row}
    finally:
        conn.close()


@app.post("/api/ack", response_class=JSONResponse)
async def api_ack_create(payload: dict):
    ack_type = str(payload.get("ack_type", "")).strip()
    note = str(payload.get("note", "")).strip()
    status = str(payload.get("status", "acknowledged")).strip() or "acknowledged"
    if not ack_type:
        return JSONResponse(status_code=400, content={"ok": False, "error": "ack_type is required"})

    db_path = _root() / "trade_log.db"
    conn = init_db(str(db_path))
    try:
        log_operator_ack(conn, ack_type=ack_type, status=status, note=note)
        rows = fetch_operator_ack(conn, ack_type=ack_type)
        return {"ok": True, "row": rows[0] if rows else None}
    finally:
        conn.close()


@app.get("/report/{filename}", response_class=HTMLResponse)
async def report_page(request: Request, filename: str):
    content = _read_report(filename)
    return templates.TemplateResponse(
        request,
        "report.html",
        {
            "filename": filename,
            "content": content,
        },
    )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    return templates.TemplateResponse(
        request,
        "history.html",
        {
            "reports": _list_reports(),
        },
    )


@app.get("/api/status", response_class=JSONResponse)
async def api_status():
    return {
        "warnings": _status_warnings(),
        "latest": _latest,
        "backtest": _backtest_latest,
        "reports": _list_reports()[:10],
        "audit": _read_trade_audit(limit=20),
        "ack": _read_operator_ack(limit=20),
        "runtime": _read_runtime_events(limit=20),
    }
