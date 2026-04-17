from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .data import fetch_prices, get_date_range, make_universe
from .engine import _detect_strategy_halt
from .macro import compute_macro_regime, is_vix_spike
from .portfolio import allocate_weights, apply_risk_cuts, score_universe, build_sleeve_map, resolve_regime_rotation, apply_sleeve_rotation


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
    adv_window_days: int = 20
    max_participation_rate: float = 0.10
    capacity_safety_factor: float = 0.25


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
    required = {"ticker", "start_date", "end_date"}
    if not required.issubset(set(timeline.columns)):
        return prices
    px = prices.copy()
    for _, row in timeline.iterrows():
        ticker = str(row["ticker"])
        if ticker not in px.columns:
            continue
        start_date = pd.Timestamp(row["start_date"])
        end_raw = row["end_date"]
        end_date = pd.Timestamp(end_raw) if pd.notna(end_raw) and str(end_raw).strip() else None
        px.loc[px.index < start_date, ticker] = pd.NA
        if end_date is not None:
            px.loc[px.index > end_date, ticker] = pd.NA
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
    active_cols = [col for col in all_px.columns if pd.notna(all_px[col].iloc[-1])]
    all_px = all_px[active_cols]
    state = regime.state
    scores = score_universe(
        all_px,
        cfg["selection"]["score_weights"],
        regime_state=state,
        regime_factor_map=cfg.get("selection", {}).get("regime_factor_bias", {}),
    )
    if scores.empty:
        raise RuntimeError("Not enough history to score universe")
    top_on = int(cfg["selection"]["top_n_risk_on"])
    top_off = int(cfg["selection"]["top_n_risk_off"])
    rotation = resolve_regime_rotation(cfg.get("selection", {}), state, top_on, top_off)
    sleeve_map_all = build_sleeve_map(scores.index, cfg.get("universe", {}).get("us", {}))
    rotated_scores = apply_sleeve_rotation(scores, sleeve_map_all, rotation)
    if state == "RISK_ON":
        selected = rotated_scores.head(rotation["top_n"]).copy()
    elif state == "RISK_OFF":
        tmp = rotated_scores.copy()
        tmp["def_score"] = tmp["score"] - 0.3 * tmp["vol_z"]
        selected = tmp.sort_values("def_score", ascending=False).head(rotation["top_n"]).copy()
    else:
        selected = rotated_scores.head(rotation["top_n"]).copy()

    cash = cfg["risk_cut"]["action"]["cash_us"]
    sleeve_factor = set(cfg.get("universe", {}).get("us", {}).get("factor_tickers", []))
    factor_selected = [ticker for ticker in selected.index if ticker in sleeve_factor]
    sleeve_map = build_sleeve_map(selected.index, cfg.get("universe", {}).get("us", {}))
    weights = allocate_weights(selected, float(cfg["selection"]["max_weight"]), sleeve_map=sleeve_map, sleeve_budget=rotation.get("sleeve_budget"))
    final_w, cuts = apply_risk_cuts(all_px, weights, cfg["risk_cut"], cash_ticker=cash)
    strategy_halt, halt_reason, halt_detail = _detect_strategy_halt(cfg, regime, selected, final_w, cuts, scores)
    selected.attrs["rotation_policy"] = rotation
    selected.attrs["selected_sleeves"] = {ticker: sleeve_map.get(ticker, "core") for ticker in selected.index}
    return state, float(regime.risk_score), final_w, selected, cuts, strategy_halt, halt_reason, halt_detail


def _estimate_adv_metrics(selected: pd.DataFrame, volume_slice: pd.DataFrame | None, dt: pd.Timestamp, bt_cfg: BacktestConfig) -> tuple[float, float, float]:
    if volume_slice is not None and not volume_slice.empty:
        cols = [c for c in selected.index if c in volume_slice.columns]
        current = volume_slice.loc[:dt, cols].tail(bt_cfg.adv_window_days) if cols else pd.DataFrame()
        if not current.empty:
            adv_20d = float(current.mean().mean())
            participation_rate = min(bt_cfg.max_participation_rate, 1.0)
            capacity = adv_20d * participation_rate * bt_cfg.capacity_safety_factor
            return adv_20d, participation_rate, capacity
    if not selected.empty and "vol" in selected.columns:
        avg_vol = float(selected["vol"].mean()) if selected["vol"].notna().any() else 0.0
        if avg_vol > 0:
            adv_proxy = 1.0 / avg_vol
            participation_rate = min(bt_cfg.max_participation_rate, 1.0)
            capacity = adv_proxy * participation_rate * bt_cfg.capacity_safety_factor
            return adv_proxy, participation_rate, capacity
    return 0.0, min(bt_cfg.max_participation_rate, 1.0), 0.0


def _compute_execution_cost(equity: float, turnover: float, selected: pd.DataFrame, bt_cfg: BacktestConfig, volume_slice: pd.DataFrame | None, dt: pd.Timestamp) -> tuple[float, float, float, float, float, float, float, float]:
    base_cost = equity * (turnover / 2.0) * (bt_cfg.transaction_cost_bps / 10000.0)
    slippage_cost = equity * (turnover / 2.0) * (bt_cfg.slippage_bps / 10000.0)
    impact_cost = equity * ((turnover / 2.0) ** 2) * (bt_cfg.market_impact_bps_per_turnover / 10000.0)
    adv_20d, participation_rate, capacity = _estimate_adv_metrics(selected, volume_slice, dt, bt_cfg)
    liquidity_penalty = 0.0
    if adv_20d > 0:
        liquidity_penalty = equity * (turnover / 2.0) * (bt_cfg.liquidity_vol_multiplier_bps / 10000.0) / max(adv_20d, 1e-9)
    total_cost = base_cost + slippage_cost + impact_cost + liquidity_penalty
    return total_cost, base_cost, slippage_cost, impact_cost, liquidity_penalty, adv_20d, participation_rate, capacity


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


def _compute_scenario_report(daily_returns: pd.Series, benchmark_returns: pd.Series | None, cfg: dict) -> pd.DataFrame:
    scenario_cfg = cfg.get("backtest", {}).get("scenarios", {})
    if not scenario_cfg.get("enabled", False) or daily_returns.empty:
        return pd.DataFrame()

    presets = scenario_cfg.get(
        "presets",
        [
            {"name": "return_shock_and_vol_up", "return_shock": scenario_cfg.get("stress_return_shock", -0.05), "vol_multiplier": scenario_cfg.get("vol_multiplier", 1.5), "benchmark_shock": 0.0},
            {"name": "benchmark_gap_down", "return_shock": -0.02, "vol_multiplier": 1.2, "benchmark_shock": -0.04},
            {"name": "volatility_crush", "return_shock": 0.0, "vol_multiplier": 0.7, "benchmark_shock": 0.0},
        ],
    )

    rows: list[dict[str, Any]] = []
    for preset in presets:
        scenario_name = str(preset.get("name", "scenario"))
        return_shock = float(preset.get("return_shock", 0.0))
        vol_multiplier = float(preset.get("vol_multiplier", 1.0))
        benchmark_shock = float(preset.get("benchmark_shock", 0.0))

        stressed = daily_returns * vol_multiplier
        stressed.iloc[:] = stressed.iloc[:] + return_shock / max(len(stressed), 1)
        scenario_total_return = float((1.0 + stressed).prod() - 1.0)
        scenario_vol = float(stressed.std() * (252 ** 0.5)) if len(stressed) > 1 else 0.0
        benchmark_total_return = 0.0
        benchmark_relative_return = None
        if benchmark_returns is not None and not benchmark_returns.empty:
            stressed_benchmark = benchmark_returns * vol_multiplier
            stressed_benchmark.iloc[:] = stressed_benchmark.iloc[:] + benchmark_shock / max(len(stressed_benchmark), 1)
            benchmark_total_return = float((1.0 + stressed_benchmark).prod() - 1.0)
            benchmark_relative_return = scenario_total_return - benchmark_total_return

        rows.append(
            {
                "scenario": scenario_name,
                "severity": str(preset.get("severity", "medium")),
                "tags": list(preset.get("tags", [])),
                "operator_action": str(preset.get("operator_action", "review scenario implications")),
                "review_required": bool(preset.get("review_required", False)),
                "note_template": str(preset.get("note_template", "")),
                "return_shock": return_shock,
                "vol_multiplier": vol_multiplier,
                "benchmark_shock": benchmark_shock,
                "avg_return": float(stressed.mean()),
                "worst_day": float(stressed.min()),
                "volatility": scenario_vol,
                "scenario_total_return": scenario_total_return,
                "benchmark_total_return": benchmark_total_return,
                "benchmark_relative_return": benchmark_relative_return,
            }
        )
    return pd.DataFrame(rows)


def _run_single_backtest(cfg: dict, bt_cfg: BacktestConfig, prices: pd.DataFrame, prep_diagnostics: dict[str, Any], volume_data: pd.DataFrame | None) -> BacktestResult:
    benchmark_curve = None
    benchmark_returns = None
    if bt_cfg.benchmark_ticker in prices.columns:
        bench = prices[bt_cfg.benchmark_ticker].dropna()
        if not bench.empty:
            benchmark_curve = bench / bench.iloc[0] * bt_cfg.starting_capital
            benchmark_returns = bench.pct_change(fill_method=None).fillna(0.0)

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

        total_cost, base_cost, slippage_cost, impact_cost, liquidity_penalty, adv_20d, participation_rate, capacity = _compute_execution_cost(equity, turnover, selected, bt_cfg, volume_data, dt)
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

        factor_universe = set(cfg.get("universe", {}).get("us", {}).get("factor_tickers", []))
        factor_selected = [ticker for ticker in selected.index if ticker in factor_universe]
        rotation_policy = selected.attrs.get("rotation_policy", {}) if hasattr(selected, "attrs") else {}
        selected_sleeves = selected.attrs.get("selected_sleeves", {}) if hasattr(selected, "attrs") else {}
        legacy_top = selected.sort_values('legacy_score', ascending=False).head(len(selected)).index.tolist() if not selected.empty and 'legacy_score' in selected.columns else []
        alpha_top = selected.sort_values('score', ascending=False).head(len(selected)).index.tolist() if not selected.empty else []
        overlap = len(set(legacy_top) & set(alpha_top))

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
                "adv_20d": adv_20d,
                "participation_rate": participation_rate,
                "capacity_estimate": capacity,
                "selected_avg_momentum": float(selected["momentum"].mean()) if not selected.empty and "momentum" in selected.columns else 0.0,
                "selected_avg_trend": float(selected["trend"].mean()) if not selected.empty and "trend" in selected.columns else 0.0,
                "selected_avg_persistence": float(selected["persistence"].mean()) if not selected.empty and "persistence" in selected.columns else 0.0,
                "selected_avg_recovery": float(selected["recovery"].mean()) if not selected.empty and "recovery" in selected.columns else 0.0,
                "selected_avg_downside_efficiency": float(selected["downside_efficiency"].mean()) if not selected.empty and "downside_efficiency" in selected.columns else 0.0,
                "selected_avg_score_uplift": float(selected["score_uplift"].mean()) if not selected.empty and "score_uplift" in selected.columns else 0.0,
                "factor_selected_count": len(factor_selected),
                "factor_selected_ratio": float(len(factor_selected) / max(len(selected.index), 1)) if len(selected.index) > 0 else 0.0,
                "factor_selected_tickers": factor_selected,
                "rotation_top_n": int(rotation_policy.get("top_n", len(selected.index))) if rotation_policy else len(selected.index),
                "rotation_preferred_sleeves": list(rotation_policy.get("preferred_sleeves", [])) if rotation_policy else [],
                "rotation_sleeve_budget": dict(rotation_policy.get("sleeve_budget", {})) if rotation_policy else {},
                "selected_sleeves": selected_sleeves,
                "legacy_alpha_overlap": overlap,
                "legacy_alpha_overlap_ratio": float(overlap / max(len(alpha_top), 1)) if alpha_top else 0.0,
                "legacy_top_picks": legacy_top,
                "alpha_top_picks": alpha_top,
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
    summary["avg_adv_20d"] = float(rebalance_log["adv_20d"].mean()) if not rebalance_log.empty and "adv_20d" in rebalance_log.columns else 0.0
    summary["avg_participation_rate"] = float(rebalance_log["participation_rate"].mean()) if not rebalance_log.empty and "participation_rate" in rebalance_log.columns else 0.0
    summary["avg_capacity_estimate"] = float(rebalance_log["capacity_estimate"].mean()) if not rebalance_log.empty and "capacity_estimate" in rebalance_log.columns else 0.0
    summary["dropped_incomplete_assets"] = prep_diagnostics.get("dropped_incomplete_assets", [])
    summary["delisted_assets"] = prep_diagnostics.get("delisted_assets", [])
    summary["timeline_applied"] = prep_diagnostics.get("timeline_applied", False)

    factor_regime = _compute_factor_and_regime_attribution(rebalance_log)
    capacity_warning = None
    if not rebalance_log.empty and "capacity_estimate" in rebalance_log.columns:
        final_capacity = float(rebalance_log["capacity_estimate"].iloc[-1])
        if final_capacity > 0:
            capacity_ratio = bt_cfg.starting_capital / final_capacity
            if capacity_ratio > 1.0:
                capacity_warning = {
                    "status": "warning",
                    "message": "portfolio size exceeds estimated safe capacity",
                    "portfolio_notional": bt_cfg.starting_capital,
                    "capacity_estimate": final_capacity,
                    "capacity_ratio": capacity_ratio,
                }

    analytics = {
        "worst_day": float(daily_returns_series.min()) if not daily_returns_series.empty else 0.0,
        "best_day": float(daily_returns_series.max()) if not daily_returns_series.empty else 0.0,
        "avg_daily_return": float(daily_returns_series.mean()) if not daily_returns_series.empty else 0.0,
        "win_rate": float((daily_returns_series > 0).mean()) if not daily_returns_series.empty else 0.0,
        "factor_regime_attribution": factor_regime,
        "capacity_warning": capacity_warning,
        "selection_score_comparison": {
            "avg_score_uplift": float(rebalance_log['selected_avg_score_uplift'].mean()) if not rebalance_log.empty and 'selected_avg_score_uplift' in rebalance_log.columns else 0.0,
            "avg_persistence": float(rebalance_log['selected_avg_persistence'].mean()) if not rebalance_log.empty and 'selected_avg_persistence' in rebalance_log.columns else 0.0,
            "avg_recovery": float(rebalance_log['selected_avg_recovery'].mean()) if not rebalance_log.empty and 'selected_avg_recovery' in rebalance_log.columns else 0.0,
            "avg_downside_efficiency": float(rebalance_log['selected_avg_downside_efficiency'].mean()) if not rebalance_log.empty and 'selected_avg_downside_efficiency' in rebalance_log.columns else 0.0,
            "avg_factor_selected_ratio": float(rebalance_log['factor_selected_ratio'].mean()) if not rebalance_log.empty and 'factor_selected_ratio' in rebalance_log.columns else 0.0,
            "latest_factor_selected_tickers": rebalance_log.iloc[-1]['factor_selected_tickers'] if not rebalance_log.empty and 'factor_selected_tickers' in rebalance_log.columns else [],
            "avg_overlap_ratio": float(rebalance_log['legacy_alpha_overlap_ratio'].mean()) if not rebalance_log.empty and 'legacy_alpha_overlap_ratio' in rebalance_log.columns else 0.0,
            "avg_rotation_top_n": float(rebalance_log['rotation_top_n'].mean()) if not rebalance_log.empty and 'rotation_top_n' in rebalance_log.columns else 0.0,
            "latest_rotation_preferred_sleeves": rebalance_log.iloc[-1]['rotation_preferred_sleeves'] if not rebalance_log.empty and 'rotation_preferred_sleeves' in rebalance_log.columns else [],
            "latest_rotation_sleeve_budget": rebalance_log.iloc[-1]['rotation_sleeve_budget'] if not rebalance_log.empty and 'rotation_sleeve_budget' in rebalance_log.columns else {},
            "latest_selected_sleeves": rebalance_log.iloc[-1]['selected_sleeves'] if not rebalance_log.empty and 'selected_sleeves' in rebalance_log.columns else {},
            "latest_legacy_top_picks": rebalance_log.iloc[-1]['legacy_top_picks'] if not rebalance_log.empty and 'legacy_top_picks' in rebalance_log.columns else [],
            "latest_alpha_top_picks": rebalance_log.iloc[-1]['alpha_top_picks'] if not rebalance_log.empty and 'alpha_top_picks' in rebalance_log.columns else [],
        },
    }

    return BacktestResult(
        equity_curve=equity_curve,
        rebalance_log=rebalance_log,
        summary=summary,
        daily_returns=daily_returns_series,
        benchmark_curve=benchmark_curve,
        analytics=analytics,
        walkforward_summary=None,
        scenario_summary=_compute_scenario_report(daily_returns_series, benchmark_returns, cfg),
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


def _legacy_score_weights(cfg: dict) -> dict[str, Any]:
    out = dict(cfg.get("selection", {}).get("score_weights", {}))
    out["momentum"] = 0.55
    out["trend"] = 0.25
    out["vol_penalty"] = 0.10
    out["dd_penalty"] = 0.10
    out["persistence"] = 0.0
    out["recovery"] = 0.0
    out["downside_efficiency"] = 0.0
    return out


def _with_score_weights(cfg: dict, score_weights: dict[str, Any]) -> dict[str, Any]:
    cloned = dict(cfg)
    cloned["selection"] = dict(cfg["selection"])
    cloned["selection"]["score_weights"] = dict(score_weights)
    return cloned


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
        adv_window_days=int(cfg.get("backtest", {}).get("adv_window_days", 20)),
        max_participation_rate=float(cfg.get("backtest", {}).get("max_participation_rate", 0.10)),
        capacity_safety_factor=float(cfg.get("backtest", {}).get("capacity_safety_factor", 0.25)),
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

    legacy_cfg = _with_score_weights(cfg, _legacy_score_weights(cfg))
    legacy_result = _run_single_backtest(legacy_cfg, bt_cfg, prices, prep_diagnostics, volume_data)

    enhanced_scenarios = result.scenario_summary.set_index("scenario") if result.scenario_summary is not None and not result.scenario_summary.empty else pd.DataFrame()
    legacy_scenarios = legacy_result.scenario_summary.set_index("scenario") if legacy_result.scenario_summary is not None and not legacy_result.scenario_summary.empty else pd.DataFrame()
    shared_scenarios = sorted(set(enhanced_scenarios.index) & set(legacy_scenarios.index)) if not enhanced_scenarios.empty and not legacy_scenarios.empty else []
    scenario_deltas = {}
    scenario_win_count = 0
    scenario_loss_count = 0
    for scenario in shared_scenarios:
        total_return_delta = float(enhanced_scenarios.loc[scenario, "scenario_total_return"] - legacy_scenarios.loc[scenario, "scenario_total_return"])
        benchmark_relative_delta = float((enhanced_scenarios.loc[scenario, "benchmark_relative_return"] or 0.0) - (legacy_scenarios.loc[scenario, "benchmark_relative_return"] or 0.0))
        verdict = "better" if total_return_delta > 0 else ("worse" if total_return_delta < 0 else "flat")
        if total_return_delta > 0:
            scenario_win_count += 1
        elif total_return_delta < 0:
            scenario_loss_count += 1
        scenario_deltas[scenario] = {
            "scenario_total_return_delta": total_return_delta,
            "benchmark_relative_return_delta": benchmark_relative_delta,
            "verdict": verdict,
        }
    worst_scenario = None
    worst_delta = 0.0
    if scenario_deltas:
        worst_scenario, worst_payload = min(scenario_deltas.items(), key=lambda item: item[1]["scenario_total_return_delta"])
        worst_delta = float(worst_payload["scenario_total_return_delta"])

    robustness_summary = (
        f"enhanced wins {scenario_win_count}, loses {scenario_loss_count} across shared scenarios"
        + (f", worst case: {worst_scenario} ({worst_delta:.2%})" if worst_scenario is not None else "")
    )
    result.analytics["strategy_comparison"] = {
        "enhanced_total_return": result.summary.get("total_return", 0.0),
        "legacy_total_return": legacy_result.summary.get("total_return", 0.0),
        "return_delta": result.summary.get("total_return", 0.0) - legacy_result.summary.get("total_return", 0.0),
        "enhanced_max_drawdown": result.summary.get("max_drawdown", 0.0),
        "legacy_max_drawdown": legacy_result.summary.get("max_drawdown", 0.0),
        "drawdown_delta": result.summary.get("max_drawdown", 0.0) - legacy_result.summary.get("max_drawdown", 0.0),
        "enhanced_active_return": result.summary.get("active_return", 0.0),
        "legacy_active_return": legacy_result.summary.get("active_return", 0.0),
        "active_return_delta": result.summary.get("active_return", 0.0) - legacy_result.summary.get("active_return", 0.0),
        "enhanced_avg_turnover": result.summary.get("avg_turnover", 0.0),
        "legacy_avg_turnover": legacy_result.summary.get("avg_turnover", 0.0),
        "turnover_delta": result.summary.get("avg_turnover", 0.0) - legacy_result.summary.get("avg_turnover", 0.0),
        "enhanced_halt_count": result.summary.get("halt_count", 0),
        "legacy_halt_count": legacy_result.summary.get("halt_count", 0),
        "scenario_robustness_deltas": scenario_deltas,
        "scenario_win_count": scenario_win_count,
        "scenario_loss_count": scenario_loss_count,
        "scenario_net_wins": scenario_win_count - scenario_loss_count,
        "worst_scenario_delta_name": worst_scenario,
        "worst_scenario_total_return_delta": worst_delta,
        "robustness_summary": robustness_summary,
    }
    return result
