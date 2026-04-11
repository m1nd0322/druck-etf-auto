from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
    walkforward_summary: pd.DataFrame | None = None
    scenario_summary: pd.DataFrame | None = None


@dataclass
class BacktestConfig:
    rebalance_frequency: str = "M"
    transaction_cost_bps: float = 1.5
    slippage_bps: float = 3.0
    market_impact_bps_per_turnover: float = 5.0
    liquidity_vol_multiplier_bps: float = 2.0
    starting_capital: float = 1.0
    benchmark_ticker: str = "SPY"
    min_history_days: int = 252
    strict_point_in_time: bool = True
    drop_incomplete_assets: bool = True
    enforce_delist_exit: bool = True
    universe_timeline_path: str = ""
    volume_data_path: str = ""


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
            "sortino": 0.0,
            "calmar": 0.0,
        }

    peak = equity_curve.cummax()
    drawdown = equity_curve / peak - 1.0
    years = max(len(equity_curve) / 252.0, 1 / 252.0)
    cagr = float((equity_curve.iloc[-1] / equity_curve.iloc[0]) ** (1 / years) - 1.0)
    vol = float(daily_returns.std() * (252 ** 0.5)) if not daily_returns.empty else 0.0
    sharpe = float((daily_returns.mean() / daily_returns.std()) * (252 ** 0.5)) if not daily_returns.empty and daily_returns.std() > 0 else 0.0
    downside = daily_returns[daily_returns < 0]
    downside_vol = float(downside.std() * (252 ** 0.5)) if not downside.empty else 0.0
    sortino = float((daily_returns.mean() * 252) / downside_vol) if downside_vol > 0 else 0.0
    max_dd = float(drawdown.min())
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else 0.0

    out = {
        "start_value": float(equity_curve.iloc[0]),
        "end_value": float(equity_curve.iloc[-1]),
        "total_return": float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0),
        "max_drawdown": max_dd,
        "volatility": vol,
        "cagr": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
    }
    if benchmark_curve is not None and not benchmark_curve.empty:
        benchmark_total_return = float(benchmark_curve.iloc[-1] / benchmark_curve.iloc[0] - 1.0)
        out["benchmark_total_return"] = benchmark_total_return
        out["active_return"] = out["total_return"] - benchmark_total_return
    return out


def _load_universe_timeline(path: str) -> pd.DataFrame | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    if p.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(p)
    return None


def _load_volume_data(path: str) -> pd.DataFrame | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p, index_col=0, parse_dates=True)
    if p.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(p)
    return None


def _apply_universe_timeline(prices: pd.DataFrame, timeline: pd.DataFrame | None) -> pd.DataFrame:
    if timeline is None or timeline.empty:
        return prices
    required = {"ticker", "start_date"}
    if not required.issubset(set(timeline.columns)):
        return prices
    px = prices.copy()
    for _, row in timeline.iterrows():
        ticker = str(row["ticker"])
        if ticker not in px.columns:
            continue
        start_date = pd.Timestamp(row["start_date"])
        px.loc[px.index < start_date, ticker] = pd.NA
    return px


def _prepare_prices_for_backtest(prices: pd.DataFrame, cfg: BacktestConfig, timeline: pd.DataFrame | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    px = _apply_universe_timeline(prices.sort_index().copy(), timeline)
    diagnostics: dict[str, Any] = {"dropped_incomplete_assets": [], "delisted_assets": [], "timeline_applied": timeline is not None}

    original_last_valid = {col: px[col].last_valid_index() for col in px.columns}
    if cfg.strict_point_in_time:
        px = px.ffill(limit=5)
    else:
        px = px.ffill()

    keep_cols = list(px.columns)
    if cfg.drop_incomplete_assets:
        keep_cols = []
        for col in px.columns:
            first_valid = px[col].first_valid_index()
            if first_valid is None:
                continue
            hist_len = int(px.loc[first_valid:, col].dropna().shape[0])
            if hist_len >= cfg.min_history_days:
                keep_cols.append(col)
            else:
                diagnostics["dropped_incomplete_assets"].append(col)
        px = px[keep_cols]

    if cfg.enforce_delist_exit:
        final_index = px.index[-1] if not px.empty else None
        for col in px.columns:
            last_valid = original_last_valid.get(col)
            if final_index is not None and last_valid is not None and last_valid < final_index:
                diagnostics["delisted_assets"].append(col)

    return px.dropna(how="all"), diagnostics


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


def _estimate_adv_proxy(selected: pd.DataFrame, volume_slice: pd.DataFrame | None, dt: pd.Timestamp) -> float:
    if volume_slice is not None and not volume_slice.empty:
        current = volume_slice.loc[:dt].tail(20)
        if not current.empty:
            return float(current.mean().mean())
    if not selected.empty and "vol" in selected.columns:
        avg_vol = float(selected["vol"].mean()) if selected["vol"].notna().any() else 0.0
        if avg_vol > 0:
            return 1.0 / avg_vol
    return 0.0


def _compute_execution_cost(equity: float, turnover: float, selected: pd.DataFrame, bt_cfg: BacktestConfig, volume_slice: pd.DataFrame | None, dt: pd.Timestamp) -> tuple[float, float, float, float, float]:
    base_cost = equity * (turnover / 2.0) * (bt_cfg.transaction_cost_bps / 10000.0)
    slippage_cost = equity * (turnover / 2.0) * (bt_cfg.slippage_bps / 10000.0)
    impact_cost = equity * ((turnover / 2.0) ** 2) * (bt_cfg.market_impact_bps_per_turnover / 10000.0)
    adv_proxy = _estimate_adv_proxy(selected, volume_slice, dt)
    liquidity_penalty = 0.0
    if adv_proxy > 0:
        liquidity_penalty = equity * (turnover / 2.0) * (bt_cfg.liquidity_vol_multiplier_bps / 10000.0) / max(adv_proxy, 1e-9)
    total_cost = base_cost + slippage_cost + impact_cost + liquidity_penalty
    return total_cost, base_cost, slippage_cost, impact_cost, liquidity_penalty


def _compute_factor_and_regime_attribution(rebalance_log: pd.DataFrame) -> dict[str, Any]:
    if rebalance_log.empty:
        return {"regime_counts": {}, "avg_risk_score": 0.0, "avg_positions": 0.0, "avg_momentum": 0.0, "avg_trend": 0.0, "avg_vol": 0.0}
    return {
        "regime_counts": rebalance_log["state"].value_counts().to_dict() if "state" in rebalance_log.columns else {},
        "avg_risk_score": float(rebalance_log["risk_score"].mean()) if "risk_score" in rebalance_log.columns else 0.0,
        "avg_positions": float(rebalance_log["positions"].mean()) if "positions" in rebalance_log.columns else 0.0,
        "avg_momentum": float(rebalance_log["selected_avg_momentum"].mean()) if "selected_avg_momentum" in rebalance_log.columns else 0.0,
        "avg_trend": float(rebalance_log["selected_avg_trend"].mean()) if "selected_avg_trend" in rebalance_log.columns else 0.0,
        "avg_vol": float(rebalance_log["selected_avg_vol"].mean()) if "selected_avg_vol" in rebalance_log.columns else 0.0,
    }


def _compute_scenario_report(daily_returns: pd.Series, cfg: dict) -> pd.DataFrame:
    scenario_cfg = cfg.get("backtest", {}).get("scenarios", {})
    if not scenario_cfg.get("enabled", False) or daily_returns.empty:
        return pd.DataFrame()
    shock = float(scenario_cfg.get("stress_return_shock", -0.05))
    vol_multiplier = float(scenario_cfg.get("vol_multiplier", 1.5))
    stressed = daily_returns * vol_multiplier
    stressed.iloc[:] = stressed.iloc[:] + shock / max(len(stressed), 1)
    return pd.DataFrame(
        [
            {
                "scenario": "return_shock_and_vol_up",
                "avg_return": float(stressed.mean()),
                "worst_day": float(stressed.min()),
                "volatility": float(stressed.std() * (252 ** 0.5)) if len(stressed) > 1 else 0.0,
            }
        ]
    )


def _run_single_backtest(cfg: dict, bt_cfg: BacktestConfig, prices: pd.DataFrame, prep_diagnostics: dict[str, Any], volume_data: pd.DataFrame | None) -> BacktestResult:
    benchmark_curve = None
    if bt_cfg.benchmark_ticker in prices.columns:
        bench = prices[bt_cfg.benchmark_ticker].dropna()
        if not bench.empty:
            benchmark_curve = bench / bench.iloc[0] * bt_cfg.starting_capital

    freq = "ME" if bt_cfg.rebalance_frequency == "M" else bt_cfg.rebalance_frequency
    rebal_dates = prices.resample(freq).last().index
    rebal_dates = [d for d in rebal_dates if d in prices.index and prices.index.get_loc(d) >= bt_cfg.min_history_days]
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

        total_cost, base_cost, slippage_cost, impact_cost, liquidity_penalty = _compute_execution_cost(equity, turnover, selected, bt_cfg, volume_data, dt)
        equity -= total_cost
        current_weights = target_weights.copy()

        next_dt = rebal_dates[i + 1] if i + 1 < len(rebal_dates) else prices.index[-1]
        segment = prices.loc[dt:next_dt]
        if segment.empty:
            continue
        returns = segment.pct_change(fill_method=None).fillna(0.0)
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
                "cost": total_cost,
                "base_cost": base_cost,
                "slippage_cost": slippage_cost,
                "impact_cost": impact_cost,
                "liquidity_penalty": liquidity_penalty,
                "strategy_halt": strategy_halt,
                "halt_reason": halt_reason,
                "halt_detail": halt_detail,
                "selected_avg_momentum": float(selected["momentum"].mean()) if not selected.empty and "momentum" in selected.columns else 0.0,
                "selected_avg_trend": float(selected["trend"].mean()) if not selected.empty and "trend" in selected.columns else 0.0,
                "selected_avg_vol": float(selected["vol"].mean()) if not selected.empty and "vol" in selected.columns else 0.0,
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
    summary["total_slippage_cost"] = float(rebalance_log["slippage_cost"].sum()) if not rebalance_log.empty else 0.0
    summary["total_impact_cost"] = float(rebalance_log["impact_cost"].sum()) if not rebalance_log.empty else 0.0
    summary["total_liquidity_penalty"] = float(rebalance_log["liquidity_penalty"].sum()) if not rebalance_log.empty else 0.0
    summary["halt_count"] = int(rebalance_log["strategy_halt"].sum()) if not rebalance_log.empty else 0
    summary["dropped_incomplete_assets"] = prep_diagnostics.get("dropped_incomplete_assets", [])
    summary["delisted_assets"] = prep_diagnostics.get("delisted_assets", [])
    summary["timeline_applied"] = prep_diagnostics.get("timeline_applied", False)

    factor_regime = _compute_factor_and_regime_attribution(rebalance_log)
    analytics = {
        "worst_day": float(daily_returns_series.min()) if not daily_returns_series.empty else 0.0,
        "best_day": float(daily_returns_series.max()) if not daily_returns_series.empty else 0.0,
        "avg_daily_return": float(daily_returns_series.mean()) if not daily_returns_series.empty else 0.0,
        "win_rate": float((daily_returns_series > 0).mean()) if not daily_returns_series.empty else 0.0,
        "factor_regime_attribution": factor_regime,
    }

    return BacktestResult(
        equity_curve=equity_curve,
        rebalance_log=rebalance_log,
        summary=summary,
        daily_returns=daily_returns_series,
        benchmark_curve=benchmark_curve,
        analytics=analytics,
        walkforward_summary=None,
        scenario_summary=_compute_scenario_report(daily_returns_series, cfg),
    )


def _run_walkforward(cfg: dict, bt_cfg: BacktestConfig, prices: pd.DataFrame, prep_diagnostics: dict[str, Any], volume_data: pd.DataFrame | None) -> pd.DataFrame:
    wf_cfg = cfg.get("backtest", {}).get("walkforward", {})
    if not wf_cfg.get("enabled", False):
        return pd.DataFrame()

    train_days = int(wf_cfg.get("train_days", 252))
    test_days = int(wf_cfg.get("test_days", 63))
    step_days = int(wf_cfg.get("step_days", test_days))
    rows: list[dict[str, Any]] = []
    idx = prices.index

    start_i = train_days
    while start_i + test_days < len(idx):
        test_start = idx[start_i]
        test_end = idx[min(start_i + test_days, len(idx) - 1)]
        segment = prices.loc[:test_end]
        result = _run_single_backtest(cfg, bt_cfg, segment, prep_diagnostics, volume_data)
        rows.append(
            {
                "test_start": test_start,
                "test_end": test_end,
                "total_return": result.summary.get("total_return", 0.0),
                "cagr": result.summary.get("cagr", 0.0),
                "sharpe": result.summary.get("sharpe", 0.0),
                "max_drawdown": result.summary.get("max_drawdown", 0.0),
                "halt_count": result.summary.get("halt_count", 0),
            }
        )
        start_i += step_days
    return pd.DataFrame(rows)


def run_backtest(cfg: dict, starting_capital: float | None = None) -> BacktestResult:
    bt_cfg = BacktestConfig(
        rebalance_frequency=str(cfg.get("backtest", {}).get("rebalance_frequency", "M")),
        transaction_cost_bps=float(cfg.get("backtest", {}).get("transaction_cost_bps", cfg.get("rebalance", {}).get("commission_bps", 1.5))),
        slippage_bps=float(cfg.get("backtest", {}).get("slippage_bps", 3.0)),
        market_impact_bps_per_turnover=float(cfg.get("backtest", {}).get("market_impact_bps_per_turnover", 5.0)),
        liquidity_vol_multiplier_bps=float(cfg.get("backtest", {}).get("liquidity_vol_multiplier_bps", 2.0)),
        starting_capital=float(starting_capital if starting_capital is not None else cfg.get("backtest", {}).get("starting_capital", 1.0)),
        benchmark_ticker=str(cfg.get("backtest", {}).get("benchmark_ticker", "SPY")),
        min_history_days=int(cfg.get("backtest", {}).get("min_history_days", 252)),
        strict_point_in_time=bool(cfg.get("backtest", {}).get("strict_point_in_time", True)),
        drop_incomplete_assets=bool(cfg.get("backtest", {}).get("drop_incomplete_assets", True)),
        enforce_delist_exit=bool(cfg.get("backtest", {}).get("enforce_delist_exit", True)),
        universe_timeline_path=str(cfg.get("backtest", {}).get("universe_timeline_path", "")),
        volume_data_path=str(cfg.get("backtest", {}).get("volume_data_path", "")),
    )

    start, end = get_date_range(cfg["data"]["lookback_years"])
    u = make_universe(cfg)
    tickers = list(dict.fromkeys(u.kr + u.us))
    prefer = cfg["data"].get("price_provider", "auto")
    cache_dir = cfg["data"].get("cache_dir", ".cache")
    use_cache = bool(cfg["data"].get("cache_csv", True))
    raw_prices = fetch_prices(tickers, start, end, prefer=prefer, cache_dir=cache_dir, use_cache=use_cache)
    timeline = _load_universe_timeline(bt_cfg.universe_timeline_path)
    volume_data = _load_volume_data(bt_cfg.volume_data_path)
    prices, prep_diagnostics = _prepare_prices_for_backtest(raw_prices, bt_cfg, timeline)

    if prices.empty or len(prices) < bt_cfg.min_history_days + 5:
        raise RuntimeError("Not enough price history for backtest")

    result = _run_single_backtest(cfg, bt_cfg, prices, prep_diagnostics, volume_data)
    walkforward = _run_walkforward(cfg, bt_cfg, prices, prep_diagnostics, volume_data)
    result.walkforward_summary = walkforward
    if result.analytics is None:
        result.analytics = {}
    result.analytics["walkforward_windows"] = int(len(walkforward)) if not walkforward.empty else 0
    if not walkforward.empty:
        result.analytics["walkforward_avg_return"] = float(walkforward["total_return"].mean())
        result.analytics["walkforward_avg_sharpe"] = float(walkforward["sharpe"].mean())
    return result
