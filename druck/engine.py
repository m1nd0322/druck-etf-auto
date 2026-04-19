from __future__ import annotations
import pandas as pd
from .data import make_universe, fetch_prices, get_date_range
from .macro import compute_macro_regime, compute_rates_overlay, is_vix_spike
from .portfolio import score_universe, allocate_weights, apply_risk_cuts, build_sleeve_map, resolve_regime_rotation, apply_sleeve_rotation, resolve_factor_preference


def _combined_sleeve_cfg(cfg: dict) -> dict:
    universe = cfg.get('universe', {}) or {}
    us_cfg = dict(universe.get('us', {}) or {})
    kr_cfg = universe.get('kr', {}) or {}
    us_cfg['kr_core_tickers'] = list(kr_cfg.get('core_tickers', []) or [])
    us_cfg['kr_attack_tickers'] = list(kr_cfg.get('attack_tickers', []) or [])
    us_cfg['kr_satellite_tickers'] = list(kr_cfg.get('satellite_tickers', []) or [])
    us_cfg['kr_defensive_tickers'] = list(kr_cfg.get('defensive_tickers', []) or [kr_cfg.get('cash_ticker', cfg.get('risk_cut', {}).get('action', {}).get('cash_kr', '130730.KS'))])
    return us_cfg
from .report import save_report
from .notifier import send_telegram
from .runtime import StrategyHaltError
from .trading import build_trade_plan, review_live_trade, TradePlanError, run_rebalance_cycle

def _detect_strategy_halt(cfg: dict, regime, selected: pd.DataFrame, final_w: pd.Series, cuts: list[dict], scores: pd.DataFrame) -> tuple[bool, str, str]:
    halt_cfg = cfg.get('strategy_halt', {})
    if not halt_cfg.get('enabled', False):
        return False, '', ''

    max_cut_ratio = float(halt_cfg.get('max_cut_asset_ratio', 0.8))
    min_risk_score = halt_cfg.get('min_risk_score', None)
    max_negative_momentum_assets = halt_cfg.get('max_negative_momentum_assets', None)

    cash_ticker = cfg['risk_cut']['action']['cash_us']
    cash_weight = float(final_w.get(cash_ticker, 0.0)) if cash_ticker in final_w.index else 0.0
    if cash_weight >= max_cut_ratio:
        return True, 'cash_dominance_halt', f'cash weight {cash_weight:.2f} exceeds threshold {max_cut_ratio:.2f}'

    if min_risk_score is not None and float(regime.risk_score) <= float(min_risk_score):
        return True, 'macro_risk_halt', f'risk score {float(regime.risk_score):.2f} <= threshold {float(min_risk_score):.2f}'

    if max_negative_momentum_assets is not None and not selected.empty and 'momentum' in selected.columns:
        negative_count = int((selected['momentum'] < 0).sum())
        if negative_count >= int(max_negative_momentum_assets):
            return True, 'negative_momentum_halt', f'{negative_count} selected assets have negative momentum'

    perf_cfg = halt_cfg.get('performance', {})
    if perf_cfg.get('enabled', False) and not scores.empty:
        min_average_score = perf_cfg.get('min_average_score', None)
        max_average_momentum = perf_cfg.get('max_average_momentum', None)
        if min_average_score is not None:
            avg_score = float(selected['score'].mean()) if 'score' in selected.columns and not selected.empty else 0.0
            if avg_score <= float(min_average_score):
                return True, 'score_degradation_halt', f'average selected score {avg_score:.4f} <= threshold {float(min_average_score):.4f}'
        if max_average_momentum is not None:
            avg_momentum = float(selected['momentum'].mean()) if 'momentum' in selected.columns and not selected.empty else 0.0
            if avg_momentum <= float(max_average_momentum):
                return True, 'momentum_degradation_halt', f'average selected momentum {avg_momentum:.4f} <= threshold {float(max_average_momentum):.4f}'

        recent_total_return = perf_cfg.get('recent_total_return', None)
        benchmark_relative_return = perf_cfg.get('benchmark_relative_return', None)
        benchmark_ticker = perf_cfg.get('benchmark_ticker', 'SPY')
        if recent_total_return is not None and hasattr(scores, 'index'):
            avg_recent_return = float(selected['momentum'].mean()) if 'momentum' in selected.columns and not selected.empty else 0.0
            if avg_recent_return <= float(recent_total_return):
                return True, 'recent_return_halt', f'average selected recent return proxy {avg_recent_return:.4f} <= threshold {float(recent_total_return):.4f}'
        if benchmark_relative_return is not None and benchmark_ticker in scores.index and not selected.empty:
            benchmark_score = float(scores.loc[benchmark_ticker].get('momentum', 0.0)) if 'momentum' in scores.columns else 0.0
            avg_selected_momentum = float(selected['momentum'].mean()) if 'momentum' in selected.columns else 0.0
            relative_gap = avg_selected_momentum - benchmark_score
            if relative_gap <= float(benchmark_relative_return):
                return True, 'benchmark_underperformance_halt', f'selected momentum gap {relative_gap:.4f} <= threshold {float(benchmark_relative_return):.4f}'

    if hasattr(cuts, 'empty'):
        cuts_present = not cuts.empty
    else:
        cuts_present = bool(cuts)
    if cuts_present:
        try:
            cut_iter = cuts.to_dict(orient='records') if hasattr(cuts, 'to_dict') else cuts
        except TypeError:
            cut_iter = cuts
        if all(bool(cut.get('cut_applied', True)) for cut in cut_iter):
            return True, 'risk_cut_cluster_halt', 'all selected assets were affected by risk cut rules'

    return False, '', ''


def _apply_budget_throttle(rotation: dict, risk_score: float) -> dict:
    if not rotation:
        return rotation
    out = dict(rotation)
    throttle_cfg = rotation.get('budget_throttle', {}) or {}
    sleeves = throttle_cfg.get('sleeves', {}) or {}
    if not sleeves:
        out['budget_throttle_applied'] = False
        return out
    sleeve_budget = dict(rotation.get('sleeve_budget', {}) or {})
    changed = False
    for sleeve, rule in sleeves.items():
        if not isinstance(rule, dict):
            continue
        threshold = rule.get('risk_score_below')
        scale = rule.get('scale')
        floor = rule.get('floor', 0.0)
        if threshold is None or scale is None or sleeve not in sleeve_budget:
            continue
        if float(risk_score) < float(threshold):
            sleeve_budget[sleeve] = max(float(floor), float(sleeve_budget[sleeve]) * float(scale))
            changed = True
    if changed:
        out['sleeve_budget'] = sleeve_budget
        out['budget_throttle_applied'] = True
    else:
        out['budget_throttle_applied'] = False
    return out


def run_once(cfg: dict, do_trade: bool=False, broker=None):
    if do_trade and broker is None:
        raise ValueError("broker is required when do_trade=True")
    start, end = get_date_range(cfg['data']['lookback_years'])
    u = make_universe(cfg)

    prefer = cfg['data'].get('price_provider','auto')
    cache_dir = cfg['data'].get('cache_dir','.cache')
    use_cache = bool(cfg['data'].get('cache_csv', True))

    kr_px = fetch_prices(u.kr, start, end, prefer=prefer, cache_dir=cache_dir, use_cache=use_cache)
    us_px = fetch_prices(u.us, start, end, prefer='yf', cache_dir=cache_dir, use_cache=use_cache)

    provider_warnings = []
    for provider_name, frame in (("kr", kr_px), ("us", us_px)):
        summary = getattr(frame, "attrs", {}).get("provider_warning_summary", {}) if frame is not None else {}
        if summary:
            provider_warnings.append({"scope": provider_name, **summary})

    regime = compute_macro_regime(us_px, cfg['macro_filter']['thresholds'], cfg['macro_filter']['components'])

    if is_vix_spike(us_px):
        # trading halt signal (execution path handles liquidation option)
        regime.details['vix_spike_halt'] = True

    all_px = pd.concat([kr_px, us_px.drop(columns=[c for c in ['^VIX'] if c in us_px.columns], errors='ignore')], axis=1)

    state = regime.state
    sleeve_cfg = _combined_sleeve_cfg(cfg)
    sleeve_map_all = build_sleeve_map(all_px.columns, sleeve_cfg)
    rates_overlay = compute_rates_overlay(all_px, cfg.get('macro_filter', {}).get('rates_overlay', {}))
    factor_pref = resolve_factor_preference(cfg.get('selection', {}), state, rates_overlay=rates_overlay)
    scores = score_universe(
        all_px,
        cfg['selection']['score_weights'],
        regime_state=state,
        regime_factor_map=cfg.get('selection', {}).get('regime_factor_bias', {}),
        sleeve_map=sleeve_map_all,
        benchmark_ticker=cfg.get('backtest', {}).get('benchmark_ticker', 'SPY'),
        relative_filter=cfg.get('selection', {}).get('benchmark_relative_filter', {}),
        factor_pref=factor_pref,
        correlation_cfg=cfg.get('selection', {}).get('correlation_diversification', {}),
        residual_cfg=cfg.get('selection', {}).get('residual_strength_anchors', {}),
    )
    if scores.empty:
        raise RuntimeError("Not enough history to score universe")

    top_on = int(cfg['selection']['top_n_risk_on'])
    top_off = int(cfg['selection']['top_n_risk_off'])
    rotation = resolve_regime_rotation(cfg.get('selection', {}), state, top_on, top_off)
    rotation = _apply_budget_throttle(rotation, float(regime.risk_score))
    sleeve_map_all = build_sleeve_map(scores.index, sleeve_cfg)
    candidate_filters = rotation.get('candidate_filters', {}) or {}
    exclude_sleeves = set(candidate_filters.get('exclude_sleeves', []) or [])
    filtered_scores = scores.loc[[ticker for ticker in scores.index if sleeve_map_all.get(ticker, 'core') not in exclude_sleeves]].copy() if exclude_sleeves else scores
    sleeve_map_filtered = build_sleeve_map(filtered_scores.index, sleeve_cfg)
    rotated_scores = apply_sleeve_rotation(filtered_scores, sleeve_map_filtered, rotation, benchmark_ticker=cfg.get('backtest', {}).get('benchmark_ticker', 'SPY'))

    if state == 'RISK_ON':
        selected = rotated_scores.head(rotation['top_n']).copy()
    elif state == 'RISK_OFF':
        tmp = rotated_scores.copy()
        tmp['def_score'] = tmp['score'] - 0.3 * tmp['vol_z']
        selected = tmp.sort_values('def_score', ascending=False).head(rotation['top_n']).copy()
    else:
        selected = rotated_scores.head(rotation['top_n']).copy()

    selected_sleeve_map = build_sleeve_map(selected.index, sleeve_cfg)
    w = allocate_weights(selected, float(cfg['selection']['max_weight']), sleeve_map=selected_sleeve_map, sleeve_budget=rotation.get('sleeve_budget'))

    strategy_family = str(cfg.get('selection', {}).get('strategy_family', 'overlay') or 'overlay').strip().lower()
    overlay_cfg = cfg.get('selection', {}).get('benchmark_overlay', {}) or {}
    if strategy_family == 'dual_momentum':
        benchmark_ticker = str(cfg.get('backtest', {}).get('benchmark_ticker', '069500.KS') or '')
        cash_ticker = str(cfg.get('risk_cut', {}).get('action', {}).get('cash_kr', '130730.KS') or '')
        dual_top_n = int(cfg.get('selection', {}).get('dual_momentum_top_n', 2) or 2)
        eligible = rotated_scores.copy()
        if benchmark_ticker in eligible.index:
            eligible = eligible.loc[(eligible['momentum'].fillna(-999.0) >= 0.0)]
            eligible = eligible.loc[(eligible['relative_strength_6m'].fillna(-999.0) >= -0.02)]
            selected = eligible.sort_values(['relative_strength_6m', 'momentum'], ascending=False).head(dual_top_n).copy()
            selected_sleeve_map = build_sleeve_map(selected.index, sleeve_cfg)
            if selected.empty and cash_ticker:
                w = pd.Series({cash_ticker: 1.0})
            else:
                dm_weights = pd.Series(dtype=float)
                for ticker in selected.index:
                    dm_weights.loc[ticker] = 1.0 / max(len(selected.index), 1)
                if benchmark_ticker not in dm_weights.index and benchmark_ticker in rotated_scores.index:
                    dm_weights.loc[benchmark_ticker] = 0.5
                total = float(dm_weights.sum())
                w = dm_weights / total if total > 0 else dm_weights
    elif bool(overlay_cfg.get('enabled', False)):
        benchmark_ticker = str(overlay_cfg.get('benchmark_ticker', cfg.get('backtest', {}).get('benchmark_ticker', '069500.KS')) or '')
        base_weight = float(overlay_cfg.get('base_weight', 0.0) or 0.0)
        attack_weight = float(overlay_cfg.get('attack_overlay_weight', 0.0) or 0.0)
        satellite_weight = float(overlay_cfg.get('satellite_overlay_weight', 0.0) or 0.0)
        overlay_weights = pd.Series(dtype=float)
        if benchmark_ticker:
            overlay_weights.loc[benchmark_ticker] = base_weight
        for ticker in selected.index:
            sleeve = selected_sleeve_map.get(ticker, 'core')
            if sleeve == 'kr_attack':
                overlay_weights.loc[ticker] = overlay_weights.get(ticker, 0.0) + attack_weight
            elif sleeve == 'kr_satellite':
                overlay_weights.loc[ticker] = overlay_weights.get(ticker, 0.0) + satellite_weight
        if not overlay_weights.empty:
            w = overlay_weights.groupby(level=0).sum()
            total = float(w.sum())
            if total > 0:
                w = w / total

    cash = cfg['risk_cut']['action'].get('cash_kr') if any(str(t).endswith('.KS') for t in selected.index) else cfg['risk_cut']['action']['cash_us']
    final_w, cuts = apply_risk_cuts(all_px, w, cfg['risk_cut'], cash_ticker=cash)

    out = selected.copy()
    out['weight_target'] = out.index.map(lambda t: w.get(t, 0.0))
    out['weight_after_cuts'] = out.index.map(lambda t: final_w.get(t, 0.0))

    # cash ticker may appear only after cuts
    if cash in final_w.index and cash not in out.index and final_w[cash] > 0:
        out.loc[cash, 'weight_target'] = 0.0
        out.loc[cash, 'weight_after_cuts'] = float(final_w[cash])

    strategy_halt, halt_reason, halt_detail = _detect_strategy_halt(cfg, regime, selected, final_w, cuts, scores)

    md_path = save_report('output', out.sort_values('weight_after_cuts', ascending=False), regime.details, cuts)
    trade_plan = None
    trade_review = None
    executed_orders = []
    rebalance_result = None
    if do_trade:
        if strategy_halt:
            raise StrategyHaltError(f"Trading halted: {halt_reason} ({halt_detail})")
        trade_plan = build_trade_plan(cfg, broker, final_w)
        trade_review = review_live_trade(cfg, broker, trade_plan)
        if not trade_review.approved:
            raise TradePlanError(f"Live trade review failed: {trade_review.checks}")
        rebalance_result = run_rebalance_cycle(cfg, broker, final_w)
        executed_orders = rebalance_result.executions

    msg = f"[Druck ETF] {regime.state} score={regime.risk_score:.2f} report={md_path}"
    if trade_plan is not None:
        msg += f" orders={len(trade_plan.orders)}"
    print(msg)
    send_telegram(cfg, msg)

    return {
        'regime': regime,
        'scores': scores,
        'target_weights': final_w,
        'prices': all_px,
        'report_path': md_path,
        'trade_plan': trade_plan,
        'trade_review': trade_review,
        'executed_orders': executed_orders,
        'rebalance_result': rebalance_result,
        'strategy_halt': strategy_halt,
        'halt_reason': halt_reason,
        'halt_detail': halt_detail,
        'rotation_policy': rotation,
        'selected_sleeves': {ticker: selected_sleeve_map.get(ticker, 'core') for ticker in selected.index},
        'provider_warnings': provider_warnings,
    }
