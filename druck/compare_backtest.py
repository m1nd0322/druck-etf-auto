from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any
import json

from .backtest import BacktestResult, run_backtest


def build_baseline_cfg(cfg: dict[str, Any], us_only: bool = True) -> dict[str, Any]:
    base_cfg = deepcopy(cfg)
    selection = base_cfg.setdefault("selection", {})
    score_weights = selection.setdefault("score_weights", {})
    score_weights["capacity_awareness"] = 0.0
    score_weights["residual_strength"] = 0.0
    selection.setdefault("correlation_diversification", {})["enabled"] = False
    selection.setdefault("residual_strength_anchors", {})["enabled"] = False

    if us_only:
        kr = base_cfg.setdefault("universe", {}).setdefault("kr", {})
        kr["auto_generate"] = False
        kr["tickers"] = []
        kr["whitelist_tickers"] = []
        kr["blacklist_tickers"] = []
        kr["include_leveraged"] = False
        kr["include_inverse"] = False
    return base_cfg


def summarize_comparison(new_result: BacktestResult, base_result: BacktestResult) -> dict[str, Any]:
    new_selection = new_result.analytics.get("selection_score_comparison", {}) if new_result.analytics else {}
    base_selection = base_result.analytics.get("selection_score_comparison", {}) if base_result.analytics else {}

    summary_delta = {
        key: float(new_result.summary.get(key, 0.0) - base_result.summary.get(key, 0.0))
        for key in sorted(set(new_result.summary) & set(base_result.summary))
        if isinstance(new_result.summary.get(key), (int, float)) and isinstance(base_result.summary.get(key), (int, float))
    }
    selection_delta = {
        key: float(new_selection.get(key, 0.0) - base_selection.get(key, 0.0))
        for key in sorted(set(new_selection) & set(base_selection))
        if isinstance(new_selection.get(key), (int, float)) and isinstance(base_selection.get(key), (int, float))
    }
    return {
        "new_summary": new_result.summary,
        "base_summary": base_result.summary,
        "summary_delta": summary_delta,
        "new_selection_score_comparison": new_selection,
        "base_selection_score_comparison": base_selection,
        "selection_delta": selection_delta,
        "new_last_alpha_picks": new_selection.get("latest_alpha_top_picks", []),
        "base_last_alpha_picks": base_selection.get("latest_alpha_top_picks", []),
    }


def write_comparison_outputs(outdir: str | Path, payload: dict[str, Any], new_result: BacktestResult, base_result: BacktestResult) -> Path:
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    (out / "scoring_comparison.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / "comparison_notes.txt").write_text(
        "\n".join(
            [
                "Comparative run scope: US strategy universe only when requested.",
                "Baseline disables capacity_awareness, residual_strength, correlation_diversification, residual_strength_anchors.",
                f"New latest picks: {payload.get('new_last_alpha_picks', [])}",
                f"Base latest picks: {payload.get('base_last_alpha_picks', [])}",
                f"Summary delta: {payload.get('summary_delta', {})}",
                f"Selection delta: {payload.get('selection_delta', {})}",
            ]
        ),
        encoding="utf-8",
    )

    for name, result in (("new", new_result), ("baseline", base_result)):
        (out / f"{name}_summary.json").write_text(json.dumps(result.summary, indent=2, ensure_ascii=False), encoding="utf-8")
        (out / f"{name}_selection_score_comparison.json").write_text(
            json.dumps((result.analytics or {}).get("selection_score_comparison", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        result.rebalance_log.to_csv(out / f"{name}_rebalance_log.csv", index=False)
        result.equity_curve.to_csv(out / f"{name}_equity_curve.csv", header=["equity"])

    return out / "scoring_comparison.json"


def run_scoring_comparison(cfg: dict[str, Any], outdir: str | Path, us_only: bool = True) -> dict[str, Any]:
    current_cfg = deepcopy(cfg)
    baseline_cfg = build_baseline_cfg(cfg, us_only=us_only)

    if us_only:
        current_cfg.setdefault("universe", {}).setdefault("kr", {})["auto_generate"] = False
        current_cfg["universe"]["kr"]["tickers"] = []
        current_cfg["universe"]["kr"]["whitelist_tickers"] = []
        current_cfg["universe"]["kr"]["blacklist_tickers"] = []
        current_cfg["universe"]["kr"]["include_leveraged"] = False
        current_cfg["universe"]["kr"]["include_inverse"] = False

    new_result = run_backtest(current_cfg)
    base_result = run_backtest(baseline_cfg)
    payload = summarize_comparison(new_result, base_result)
    output_path = write_comparison_outputs(outdir, payload, new_result, base_result)
    return {"payload": payload, "output_path": str(output_path)}
