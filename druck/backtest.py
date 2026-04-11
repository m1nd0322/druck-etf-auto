from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .data import fetch_prices, get_date_range, make_universe
from .engine import _detect_strategy_halt
from .macro import compute_macro_regime, is_vix_spike
from .portfolio import allocate_weights, apply_risk_cuts, score_universe


@dataclass
class BacktestResult:
    equity_curve: pd.Series
    rebalance_log: pd.DataFrame
    summary: dict[str, Any]
    daily_returns: pd.Series
    benchmark_curve: pd.Series | None = None
    analytics: dict[str, Any] | None = None


@dataclass
class BacktestConfig:
    rebalance_frequency: str = "M"
    transaction_cost_bps: float = 1.5
    starting_capital: float = 1.0
    benchmark_ticker: str = "SPY"


def _compute_summary(equity_curve: pd.Series, daily_returns: pd.Series, benchmark_curve: pd.Series | None = None) -> dict[str, Any]:
    if equity_curve.empty:
        return {
            "start_value": 0.0,
            "end_value": 0.0,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "volatility": 0.0,
            "cagr": 0.0,
            "sharpe": 0.0,
        }

    peak = equity_curve.cummax()
    drawdown = equity_curve / peak - 1.0
    years = max(len(equity_curve) / 252.0, 1 / 252.0)
    cagr = float((equity_curve.iloc[-1] / equity_curve.iloc[0]) ** (1 / years) - 1.0)
    vol = float(daily_returns.std() * (252 ** 0.5)) if not daily_returns.empty else 0.0
    sharpe = float((daily_returns.mean() / daily_returns.std()) * (252 ** 0.5)) if not daily_returns.empty and daily_returns.std() > 0 else 0.0

    out = {
        "start_value": float(equity_curve.iloc[0]),
        "end_value": float(equity_curve.iloc[-1]),
        "total_return": float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0),
        "max_drawdown": float(drawdown.min()),
        "volatility": vol,
        "cagr": cagr,
        "sharpe": sharpe,
    }
    if benchmark_curve is not None and not benchmark_curve.empty:
        out["benchmark_total_return"] = float(benchmark_curve.iloc[-1] / benchmark_curve.iloc[0] - 1.0)
        out["active_return"] = out["total_return"] - out["benchmark_total_return"]
    return out


def _select_weights(cfg: dict, px_window: pd.DataFrame) -> tuple[str, float, pd.Series, pd.DataFrame, pd.DataFrame, bool, str, str]:
    regime = compute_macro_regime(px_window, cfg["macro_filter"]["thresholds"], cfg["macro_filter"]["components"])
    if is_vix_spike(px_window):
        regime.details["vix_spike_halt"] = True

    all_px = px_window.drop(columns=[c for c in ["^VIX"] if c in px_window.columns], errors="ignore")
    scores = score_universe(all_px, cfg["selection"]["score_weights"])
    if scores.empty:
        raise RuntimeError("Not enough history to score universe")

    state = regime.state
    top_on = int(cfg["selection"]["top_n_risk_on"])
    top_off = int(cfg["selection"]["top_n_risk_off"])
    if state == "RISK_ON":
        selected = scores.head(top_on).copy()
    elif state == "RISK_OFF":
        tmp = scores.copy()
        tmp["def_score"] = tmp["score"] - 0.3 * tmp["vol_z"]
        selected = tmp.sort_values("def_score", ascending=False).head(top_off).copy()
    else:
        selected = scores.head(max(3, top_on // 2)).copy()

    weights = allocate_weights(selected, float(cfg["selection"]["max_weight"]))
    cash = cfg["risk_cut"]["action"]["cash_us"]
    final_w, cuts = apply_risk_cuts(all_px, weights, cfg["risk_cut"], cash_ticker=cash)
    strategy_halt, halt_reason, halt_detail = _detect_strategy_halt(cfg, regime, selected, final_w, cuts, scores)
    return state, float(regime.risk_score), final_w, selected, cuts, strategy_halt, halt_reason, halt_detail


def run_backtest(cfg: dict, starting_capital: float | None = None) -> BacktestResult:
    bt_cfg = BacktestConfig(
        rebalance_frequency=str(cfg.get("backtest", {}).get("rebalance_frequency", "M")),
        transaction_cost_bps=float(cfg.get("backtest", {}).get("transaction_cost_bps", cfg.get("rebalance", {}).get("commission_bps", 1.5))),
        starting_capital=float(starting_capital if starting_capital is not None else cfg.get("backtest", {}).get("starting_capital", 1.0)),
        benchmark_ticker=str(cfg.get("backtest", {}).get("benchmark_ticker", "SPY")),
    )

    start, end = get_date_range(cfg["data"]["lookback_years"])
    u = make_universe(cfg)
    tickers = list(dict.fromkeys(u.kr + u.us))
    prefer = cfg["data"].get("price_provider", "auto")
    cache_dir = cfg["data"].get("cache_dir", ".cache")
    use_cache = bool(cfg["data"].get("cache_csv", True))
    prices = fetch_prices(tickers, start, end, prefer=prefer, cache_dir=cache_dir, use_cache=use_cache).sort_index()
    prices = prices.ffill().dropna(how="all")

    if prices.empty or len(prices) < 260:
        raise RuntimeError("Not enough price history for backtest")

    benchmark_curve = None
    if bt_cfg.benchmark_ticker in prices.columns:
        bench = prices[bt_cfg.benchmark_ticker].dropna()
        if not bench.empty:
            benchmark_curve = bench / bench.iloc[0] * bt_cfg.starting_capital

    freq = "ME" if bt_cfg.rebalance_frequency == "M" else bt_cfg.rebalance_frequency
    rebal_dates = prices.resample(freq).last().index
    rebal_dates = [d for d in rebal_dates if d in prices.index and prices.index.get_loc(d) >= 252]
    if not rebal_dates:
        rebal_dates = [prices.index[-1]]

    equity = bt_cfg.starting_capital
    current_weights = pd.Series(dtype=float)
    equity_points: list[tuple[pd.Timestamp, float]] = []
    rebalance_rows: list[dict[str, Any]] = []
    daily_returns: list[tuple[pd.Timestamp, float]] = []

    for i, dt in enumerate(rebal_dates):
        idx = prices.index.get_loc(dt)
        window = prices.iloc[: idx + 1]
        state, risk_score, target_weights, selected, cuts, strategy_halt, halt_reason, halt_detail = _select_weights(cfg, window)

        if strategy_halt:
            target_weights = pd.Series(dtype=float)

        prev_weights = current_weights.copy()
        all_names = sorted(set(prev_weights.index) | set(target_weights.index))
        turnover = 0.0
        if all_names:
            prev = prev_weights.reindex(all_names).fillna(0.0)
            new = target_weights.reindex(all_names).fillna(0.0)
            turnover = float((new - prev).abs().sum())

        cost = equity * (turnover / 2.0) * (bt_cfg.transaction_cost_bps / 10000.0)
        equity -= cost
        current_weights = target_weights.copy()

        next_dt = rebal_dates[i + 1] if i + 1 < len(rebal_dates) else prices.index[-1]
        segment = prices.loc[dt:next_dt]
        if segment.empty:
            continue
        returns = segment.pct_change().fillna(0.0)
        cols = [c for c in current_weights.index if c in returns.columns]
        if cols:
            w = current_weights.reindex(cols).fillna(0.0)
            port_ret = returns[cols].mul(w, axis=1).sum(axis=1)
        else:
            port_ret = pd.Series(0.0, index=segment.index)

        for ts, r in port_ret.items():
            equity *= (1.0 + float(r))
            equity_points.append((ts, equity))
            daily_returns.append((ts, float(r)))

        rebalance_rows.append(
            {
                "date": dt,
                "state": state,
                "risk_score": risk_score,
                "positions": int((current_weights > 0).sum()),
                "turnover": turnover,
                "cost": cost,
                "strategy_halt": strategy_halt,
                "halt_reason": halt_reason,
                "halt_detail": halt_detail,
                "weights": current_weights.to_dict(),
                "cuts": cuts.to_dict(orient="records") if hasattr(cuts, "to_dict") else [],
            }
        )

    equity_curve = pd.Series({ts: val for ts, val in equity_points}).sort_index()
    if equity_curve.empty:
        equity_curve = pd.Series([bt_cfg.starting_capital], index=pd.Index([prices.index[-1]], name="date"))
    daily_returns_series = pd.Series({ts: val for ts, val in daily_returns}).sort_index()
    rebalance_log = pd.DataFrame(rebalance_rows)
    summary = _compute_summary(equity_curve, daily_returns_series, benchmark_curve=benchmark_curve)
    summary["positions"] = int(rebalance_log["positions"].iloc[-1]) if not rebalance_log.empty else 0
    summary["rebalances"] = int(len(rebalance_log))
    summary["avg_turnover"] = float(rebalance_log["turnover"].mean()) if not rebalance_log.empty else 0.0
    summary["total_cost"] = float(rebalance_log["cost"].sum()) if not rebalance_log.empty else 0.0
    summary["halt_count"] = int(rebalance_log["strategy_halt"].sum()) if not rebalance_log.empty else 0

    analytics = {
        "worst_day": float(daily_returns_series.min()) if not daily_returns_series.empty else 0.0,
        "best_day": float(daily_returns_series.max()) if not daily_returns_series.empty else 0.0,
        "avg_daily_return": float(daily_returns_series.mean()) if not daily_returns_series.empty else 0.0,
    }

    return BacktestResult(
        equity_curve=equity_curve,
        rebalance_log=rebalance_log,
        summary=summary,
        daily_returns=daily_returns_series,
        benchmark_curve=benchmark_curve,
        analytics=analytics,
    )
