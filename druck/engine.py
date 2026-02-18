from __future__ import annotations
import pandas as pd
import yaml
from .data import make_universe, fetch_prices, get_date_range
from .macro import compute_macro_regime, is_vix_spike
from .portfolio import score_universe, allocate_weights, apply_risk_cuts
from .report import save_report
from .notifier import send_telegram

def run_once(cfg: dict, do_trade: bool=False, broker=None):
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

    md_path = save_report('output', out.sort_values('weight_after_cuts', ascending=False), regime.details, cuts)
    msg = f"[Druck ETF] {regime.state} score={regime.risk_score:.2f} report={md_path}"
    print(msg)
    send_telegram(cfg, msg)

    return {
        'regime': regime,
        'scores': scores,
        'target_weights': final_w,
        'prices': all_px,
        'report_path': md_path,
    }
