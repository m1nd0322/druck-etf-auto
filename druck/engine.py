from __future__ import annotations
import pandas as pd
from .data import make_universe, fetch_prices, get_date_range
from .macro import compute_macro_regime, is_vix_spike
from .portfolio import score_universe, allocate_weights, apply_risk_cuts
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

    regime = compute_macro_regime(us_px, cfg['macro_filter']['thresholds'], cfg['macro_filter']['components'])

    if is_vix_spike(us_px):
        # trading halt signal (execution path handles liquidation option)
        regime.details['vix_spike_halt'] = True

    all_px = pd.concat([kr_px, us_px.drop(columns=[c for c in ['^VIX'] if c in us_px.columns], errors='ignore')], axis=1)

    scores = score_universe(all_px, cfg['selection']['score_weights'])
    if scores.empty:
        raise RuntimeError("Not enough history to score universe")

    state = regime.state
    top_on = int(cfg['selection']['top_n_risk_on'])
    top_off = int(cfg['selection']['top_n_risk_off'])
    if state == 'RISK_ON':
        selected = scores.head(top_on).copy()
    elif state == 'RISK_OFF':
        tmp = scores.copy()
        tmp['def_score'] = tmp['score'] - 0.3 * tmp['vol_z']
        selected = tmp.sort_values('def_score', ascending=False).head(top_off).copy()
    else:
        selected = scores.head(max(3, top_on//2)).copy()

    w = allocate_weights(selected, float(cfg['selection']['max_weight']))

    cash = cfg['risk_cut']['action']['cash_us']
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
    }
