from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .engine import run_once


@dataclass
class BacktestResult:
    equity_curve: pd.Series
    rebalance_log: pd.DataFrame
    summary: dict[str, Any]


def _compute_summary(equity_curve: pd.Series) -> dict[str, Any]:
    returns = equity_curve.pct_change().dropna()
    if equity_curve.empty:
        return {"start_value": 0.0, "end_value": 0.0, "total_return": 0.0, "max_drawdown": 0.0}

    peak = equity_curve.cummax()
    drawdown = equity_curve / peak - 1.0

    return {
        "start_value": float(equity_curve.iloc[0]),
        "end_value": float(equity_curve.iloc[-1]),
        "total_return": float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0),
        "max_drawdown": float(drawdown.min()),
        "volatility": float(returns.std() * (252 ** 0.5)) if not returns.empty else 0.0,
    }


def run_backtest(cfg: dict, starting_capital: float = 1.0) -> BacktestResult:
    """Minimal backtest skeleton.

    Current scope:
    - runs the live selection pipeline once
    - builds a placeholder one-point equity curve from target weights
    - returns summary and rebalance log structure for future extension

    This is intentionally conservative scaffolding so future work can expand into
    historical periodic rebalancing with transaction costs.
    """
    result = run_once(cfg, do_trade=False)
    weights = result["target_weights"].sort_values(ascending=False)

    rebalance_log = pd.DataFrame(
        {
            "ticker": weights.index,
            "target_weight": weights.values,
        }
    )
    equity_curve = pd.Series([float(starting_capital)], index=pd.Index([pd.Timestamp.utcnow()], name="date"))
    summary = _compute_summary(equity_curve)
    summary["positions"] = int((weights > 0).sum())
    summary["report_path"] = result.get("report_path")

    return BacktestResult(
        equity_curve=equity_curve,
        rebalance_log=rebalance_log,
        summary=summary,
    )
