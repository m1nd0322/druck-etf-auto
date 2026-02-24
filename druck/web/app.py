from __future__ import annotations

import os
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml
import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..engine import run_once

_HERE = Path(__file__).resolve().parent
_ROOT = Path.cwd()

app = FastAPI(title="Druck ETF Auto")
app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
templates = Jinja2Templates(directory=str(_HERE / "templates"))

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _load_cfg() -> dict:
    cfg_path = _ROOT / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    local_path = _ROOT / "config.local.yaml"
    if local_path.exists():
        local = yaml.safe_load(local_path.read_text(encoding="utf-8"))
        if local:
            _deep_merge(cfg, local)
    return cfg


def _deep_merge(base: dict, override: dict):
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def _list_reports() -> List[Dict[str, Any]]:
    out_dir = _ROOT / "output"
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
    p = _ROOT / "output" / filename
    if p.exists() and p.suffix == ".md":
        return p.read_text(encoding="utf-8")
    return None


def _format_regime_result(result: dict) -> dict:
    """Convert run_once result to JSON-serialisable dashboard data."""
    regime = result["regime"]
    scores: pd.DataFrame = result["scores"]
    weights: pd.Series = result["target_weights"]

    # selected ETFs table
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

    return {
        "state": regime.state,
        "risk_score": round(regime.risk_score, 4),
        "details": details,
        "etfs": etfs,
        "report_path": result.get("report_path", ""),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# In-memory latest result cache
# ---------------------------------------------------------------------------
_latest: dict | None = None


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    reports = _list_reports()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "latest": _latest,
        "reports": reports[:20],
    })


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


@app.get("/api/reports", response_class=JSONResponse)
async def api_reports():
    return {"reports": _list_reports()}


@app.get("/api/reports/{filename}", response_class=JSONResponse)
async def api_report_detail(filename: str):
    content = _read_report(filename)
    if content is None:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return {"filename": filename, "content": content}


@app.get("/report/{filename}", response_class=HTMLResponse)
async def report_page(request: Request, filename: str):
    content = _read_report(filename)
    return templates.TemplateResponse("report.html", {
        "request": request,
        "filename": filename,
        "content": content,
    })


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    return templates.TemplateResponse("history.html", {
        "request": request,
        "reports": _list_reports(),
    })
