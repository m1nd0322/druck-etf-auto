from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
import numpy as np
import pandas as pd
from .features import momentum_score, trend_score, rolling_vol, max_drawdown, zscore, sma, trailing_drawdown, persistence_score, recovery_score, downside_efficiency, relative_strength_vs_benchmark, capacity_penalty_score, residual_strength_vs_anchors


def compute_diversification_adjustment(scores: pd.DataFrame, correlation_cfg: dict | None = None) -> pd.DataFrame:
    if scores.empty:
        return scores
    correlation_cfg = correlation_cfg or {}
    if not bool(correlation_cfg.get("enabled", False)):
        out = scores.copy()
        out["diversification_penalty"] = 0.0
        out["diversification_score"] = 0.0
        return out

    lookback = int(correlation_cfg.get("lookback", 63) or 63)
    top_k = int(correlation_cfg.get("top_k", 3) or 3)
    penalty_scale = float(correlation_cfg.get("penalty", 0.0) or 0.0)
    threshold = float(correlation_cfg.get("min_correlation", 0.0) or 0.0)

    out = scores.copy()
    corr = out.attrs.get("return_correlation")
    if corr is None or getattr(corr, "empty", True):
        out["diversification_penalty"] = 0.0
        out["diversification_score"] = 0.0
        return out

    corr = corr.reindex(index=out.index, columns=out.index)
    penalties: dict[str, float] = {}
    contributions: dict[str, float] = {}
    rank_order = out.sort_values("score", ascending=False).index.tolist()

    for ticker in out.index:
        peers = [peer for peer in rank_order if peer != ticker][:top_k]
        if not peers:
            penalties[ticker] = 0.0
            contributions[ticker] = 0.0
            continue
        values = corr.loc[ticker, peers].fillna(0.0).astype(float)
        positive_values = values[values > threshold]
        avg_corr = float(positive_values.mean()) if not positive_values.empty else 0.0
        penalty = max(0.0, avg_corr - threshold) * penalty_scale
        penalties[ticker] = penalty
        contributions[ticker] = max(0.0, 1.0 - avg_corr)

    out["diversification_penalty"] = pd.Series(penalties).reindex(out.index).fillna(0.0)
    out["diversification_score"] = pd.Series(contributions).reindex(out.index).fillna(0.0)
    out["score"] = out["score"] - out["diversification_penalty"]
    out.attrs["diversification_cfg"] = {"lookback": lookback, "top_k": top_k, "penalty": penalty_scale, "min_correlation": threshold}
    return out.sort_values("score", ascending=False)


def build_sleeve_map(tickers: list[str] | pd.Index, universe_cfg: dict | None) -> dict[str, str]:
    universe_cfg = universe_cfg or {}
    factor = set(universe_cfg.get("factor_tickers", []) or [])
    sector = set(universe_cfg.get("sector_tickers", []) or [])
    country = set(universe_cfg.get("country_tickers", []) or [])
    kr_core = set(universe_cfg.get("kr_core_tickers", []) or [])
    kr_attack = set(universe_cfg.get("kr_attack_tickers", []) or [])
    kr_satellite = set(universe_cfg.get("kr_satellite_tickers", []) or [])
    kr_defensive = set(universe_cfg.get("kr_defensive_tickers", []) or [])
    sleeve_map: dict[str, str] = {}
    for ticker in [str(t) for t in tickers]:
        if ticker in factor:
            sleeve_map[ticker] = "factor"
        elif ticker in sector:
            sleeve_map[ticker] = "sector"
        elif ticker in country:
            sleeve_map[ticker] = "country"
        elif ticker in kr_attack:
            sleeve_map[ticker] = "kr_attack"
        elif ticker in kr_satellite:
            sleeve_map[ticker] = "kr_satellite"
        elif ticker in kr_defensive:
            sleeve_map[ticker] = "defensive"
        elif ticker in kr_core:
            sleeve_map[ticker] = "kr_core"
        else:
            sleeve_map[ticker] = "core"
    return sleeve_map


def resolve_factor_preference(selection_cfg: dict | None, regime_state: str, rates_overlay: dict | None = None) -> dict:
    selection_cfg = selection_cfg or {}
    factor_map = selection_cfg.get("regime_factor_map", {}) or {}
    regime_cfg = factor_map.get(regime_state, {}) or {}
    rates_overlay = rates_overlay or {}
    relative_gate = regime_cfg.get("relative_strength_gate", {}) or {}
    rates_cfg = regime_cfg.get("rates_overlay", {}) or {}
    rates_direction = str(rates_overlay.get("direction", "neutral") or "neutral")
    overlay = rates_cfg.get(rates_direction, {}) or {}
    overweight = list(regime_cfg.get("overweight", []) or []) + list(overlay.get("overweight", []) or [])
    underweight = list(regime_cfg.get("underweight", []) or []) + list(overlay.get("underweight", []) or [])
    return {
        "enabled": bool(factor_map.get("enabled", False)),
        "overweight": list(dict.fromkeys(overweight)),
        "underweight": list(dict.fromkeys(underweight)),
        "min_count": int(regime_cfg.get("min_count", 0) or 0),
        "bonus": float(regime_cfg.get("bonus", 0.0) or 0.0) + float(overlay.get("bonus", 0.0) or 0.0),
        "penalty": float(regime_cfg.get("penalty", 0.0) or 0.0) + float(overlay.get("penalty", 0.0) or 0.0),
        "rates_direction": rates_direction,
        "rates_overlay": overlay,
        "relative_strength_gate": {
            "enabled": bool(relative_gate.get("enabled", False)),
            "min_relative_strength_6m": float(relative_gate.get("min_relative_strength_6m", 0.0) or 0.0),
            "mode": str(relative_gate.get("mode", "penalty") or "penalty").strip().lower(),
            "penalty": float(relative_gate.get("penalty", 0.0) or 0.0),
        },
    }


def apply_regime_factor_map(scores: pd.DataFrame, factor_pref: dict | None) -> pd.DataFrame:
    if scores.empty or not factor_pref or not factor_pref.get("enabled", False):
        return scores
    out = scores.copy()
    overweight = set(factor_pref.get("overweight", []))
    underweight = set(factor_pref.get("underweight", []))
    bonus = float(factor_pref.get("bonus", 0.0))
    penalty = float(factor_pref.get("penalty", 0.0))
    out["factor_map_bonus"] = [bonus if t in overweight else (-penalty if t in underweight else 0.0) for t in out.index]

    gate = factor_pref.get("relative_strength_gate", {}) or {}
    gate_enabled = bool(gate.get("enabled", False))
    gate_threshold = float(gate.get("min_relative_strength_6m", 0.0))
    gate_mode = str(gate.get("mode", "penalty") or "penalty").strip().lower()
    gate_penalty = float(gate.get("penalty", 0.0))
    gate_mask = out.index.to_series().isin(list(overweight)) & (out.get("relative_strength_6m", 0.0).fillna(-999.0) < gate_threshold)
    out["factor_gate_fail"] = gate_mask if gate_enabled else False
    out["factor_gate_excluded"] = False
    if gate_enabled:
        if gate_mode == "exclude":
            out.loc[gate_mask, "factor_gate_excluded"] = True
            out = out.loc[~gate_mask].copy()
        elif gate_penalty > 0:
            out.loc[gate_mask, "factor_map_bonus"] = out.loc[gate_mask, "factor_map_bonus"] - gate_penalty

    out["score"] = out["score"] + out["factor_map_bonus"]
    return out.sort_values("score", ascending=False)


def resolve_regime_rotation(selection_cfg: dict | None, regime_state: str, default_top_on: int, default_top_off: int) -> dict:
    selection_cfg = selection_cfg or {}
    rotation_cfg = selection_cfg.get("regime_sleeve_rotation", {}) or {}
    regime_cfg = rotation_cfg.get(regime_state, {}) or {}
    top_n = regime_cfg.get("top_n")
    if top_n is None:
        top_n = default_top_off if regime_state == "RISK_OFF" else default_top_on if regime_state == "RISK_ON" else max(3, default_top_on // 2)
    return {
        "enabled": bool(rotation_cfg.get("enabled", False)),
        "top_n": int(top_n),
        "sleeve_budget": regime_cfg.get("sleeve_budget") or selection_cfg.get("sleeve_budget", {}) or {},
        "score_tilt": regime_cfg.get("score_tilt") or {},
        "preferred_sleeves": list(regime_cfg.get("preferred_sleeves", []) or []),
    }


def apply_sleeve_rotation(scores: pd.DataFrame, sleeve_map: dict[str, str] | None, rotation: dict | None) -> pd.DataFrame:
    if scores.empty or not sleeve_map or not rotation or not rotation.get("enabled", False):
        return scores
    out = scores.copy()
    score_tilt = rotation.get("score_tilt", {}) or {}
    preferred_sleeves = set(rotation.get("preferred_sleeves", []) or [])
    out["sleeve"] = [sleeve_map.get(t, "core") for t in out.index]
    out["sleeve_rotation_bonus"] = [float(score_tilt.get(sleeve_map.get(t, "core"), 0.0)) for t in out.index]
    out["rotation_preferred"] = [sleeve_map.get(t, "core") in preferred_sleeves for t in out.index]
    out["score"] = out["score"] + out["sleeve_rotation_bonus"]
    return out.sort_values(["score", "rotation_preferred"], ascending=[False, False])

@dataclass
class SelectionResult:
    scores: pd.DataFrame
    selected: pd.DataFrame
    weights: pd.Series


def _legacy_score(df: pd.DataFrame, sw: dict) -> pd.Series:
    return (
        float(sw['momentum']) * df['mom_z']
        + float(sw['trend']) * df['trend_z']
        - float(sw['vol_penalty']) * df['vol_z']
        - float(sw['dd_penalty']) * df['dd_z']
    )


def apply_regime_factor_bias(scores: pd.DataFrame, regime_state: str, regime_factor_map: dict | None) -> pd.DataFrame:
    if scores.empty or not regime_factor_map or 'ticker' in scores.columns:
        pass
    if scores.empty or not regime_factor_map:
        return scores
    mapping = regime_factor_map.get(regime_state, {}) or {}
    bonuses = {}
    for tickers_key, bonus in mapping.items():
        if isinstance(tickers_key, str):
            for ticker in [t.strip() for t in tickers_key.split(',') if t.strip()]:
                bonuses[ticker] = float(bonus)
    out = scores.copy()
    out['regime_bonus'] = [bonuses.get(t, 0.0) for t in out.index]
    out['score'] = out['score'] + out['regime_bonus']
    return out.sort_values('score', ascending=False)


def score_universe(prices: pd.DataFrame, sw: dict, regime_state: str | None = None, regime_factor_map: dict | None = None, sleeve_map: dict[str, str] | None = None, benchmark_ticker: str | None = "SPY", relative_filter: dict | None = None, factor_pref: dict | None = None, correlation_cfg: dict | None = None, residual_cfg: dict | None = None) -> pd.DataFrame:
    rows=[]
    benchmark = prices[benchmark_ticker].dropna() if benchmark_ticker and benchmark_ticker in prices.columns else None
    relative_filter = relative_filter or {}
    residual_cfg = residual_cfg or {}
    anchor_tickers = [str(t) for t in residual_cfg.get('anchor_tickers', []) or [] if str(t) in prices.columns]
    anchor_frame = prices[anchor_tickers] if bool(residual_cfg.get('enabled', False)) and anchor_tickers else pd.DataFrame()
    residual_lookback = int(residual_cfg.get('lookback', 126) or 126)
    for t in prices.columns:
        p=prices[t].dropna()
        if len(p)<260:
            continue
        sleeve = sleeve_map.get(t, 'core') if sleeve_map else 'core'
        rs_126 = relative_strength_vs_benchmark(p, benchmark, 126) if benchmark is not None and t != benchmark_ticker else 0.0
        rows.append({
            'ticker':t,
            'sleeve': sleeve,
            'momentum':momentum_score(p),
            'trend':trend_score(p),
            'persistence': persistence_score(p, 126),
            'recovery': recovery_score(p, 126),
            'downside_efficiency': downside_efficiency(p, 126),
            'relative_strength_6m': rs_126,
            'capacity_score': capacity_penalty_score(p, 63),
            'residual_strength': residual_strength_vs_anchors(p, anchor_frame.drop(columns=[t], errors='ignore'), residual_lookback) if bool(residual_cfg.get('enabled', False)) else 0.0,
            'vol':rolling_vol(p,63),
            'mdd_1y':max_drawdown(p,252),
        })
    df=pd.DataFrame(rows).set_index('ticker')
    if df.empty:
        return df
    df['mom_z']=zscore(df['momentum'])
    df['trend_z']=zscore(df['trend'].fillna(0.0))
    df['persist_z']=zscore(df['persistence'].fillna(0.0))
    df['recovery_z']=zscore(df['recovery'].fillna(0.0))
    df['downside_z']=zscore(df['downside_efficiency'].fillna(0.0))
    df['vol_z']=zscore(df['vol'])
    df['dd_z']=zscore(df['mdd_1y'])
    df['rel_strength_z']=zscore(df['relative_strength_6m'].fillna(0.0))
    df['capacity_z']=zscore(df['capacity_score'].fillna(0.0))
    df['residual_strength_z']=zscore(df['residual_strength'].fillna(0.0))
    returns_corr = prices.pct_change(fill_method=None).tail(int((correlation_cfg or {}).get('lookback', 63) or 63)).corr() if len(prices.index) > 1 else pd.DataFrame()
    df.attrs['return_correlation'] = returns_corr
    df['legacy_score'] = _legacy_score(df, sw)
    df['score']=(
        float(sw['momentum'])*df['mom_z']
        + float(sw['trend'])*df['trend_z']
        + float(sw.get('persistence', 0.20))*df['persist_z']
        + float(sw.get('recovery', 0.15))*df['recovery_z']
        + float(sw.get('downside_efficiency', 0.15))*df['downside_z']
        + float(sw.get('relative_strength', 0.10))*df['rel_strength_z']
        + float(sw.get('capacity_awareness', 0.0))*df['capacity_z']
        + float(sw.get('residual_strength', 0.0))*df['residual_strength_z']
        - float(sw['vol_penalty'])*df['vol_z']
        - float(sw['dd_penalty'])*df['dd_z']
    )
    df['score_uplift'] = df['score'] - df['legacy_score']

    threshold = relative_filter.get('min_relative_strength_6m') if relative_filter and relative_filter.get('enabled', False) else None
    if threshold is not None:
        penalty = float(relative_filter.get('penalty', 0.0))
        mode = str(relative_filter.get('mode', 'penalty')).strip().lower() or 'penalty'
        enabled_sleeves = set(relative_filter.get('apply_to_sleeves', ['factor', 'sector', 'country']))
        mask = df['sleeve'].isin(enabled_sleeves) & (df['relative_strength_6m'].fillna(-999.0) < float(threshold))
        df['benchmark_relative_fail'] = mask
        df['benchmark_relative_excluded'] = False
        if mode == 'exclude':
            df.loc[mask, 'benchmark_relative_excluded'] = True
            df = df.loc[~mask].copy()
        elif penalty > 0:
            df.loc[mask, 'score'] = df.loc[mask, 'score'] - penalty
    else:
        df['benchmark_relative_fail'] = False
        df['benchmark_relative_excluded'] = False

    df = df.sort_values('score', ascending=False)
    if regime_state is not None:
        df = apply_regime_factor_bias(df, regime_state, regime_factor_map)
    df = apply_regime_factor_map(df, factor_pref)
    df = compute_diversification_adjustment(df, correlation_cfg)
    return df

def apply_sleeve_budget(weights: pd.Series, sleeve_map: dict[str, str] | None, sleeve_budget: dict[str, float] | None) -> pd.Series:
    if weights.empty or not sleeve_map or not sleeve_budget:
        return weights
    adjusted = weights.copy()
    result = adjusted.copy()

    for sleeve in sorted({sleeve_map.get(t, 'other') for t in adjusted.index}):
        members = [ticker for ticker in adjusted.index if sleeve_map.get(ticker, 'other') == sleeve]
        if not members:
            continue
        sleeve_total = float(result.loc[members].sum())
        budget = float(sleeve_budget.get(sleeve, sleeve_total))
        if sleeve_total > budget > 0:
            result.loc[members] = result.loc[members] * (budget / sleeve_total)

    capped_sleeves = {sleeve for sleeve in sleeve_budget}
    uncapped_members = [ticker for ticker in result.index if sleeve_map.get(ticker, 'other') not in capped_sleeves]
    capped_total = float(result.drop(index=uncapped_members).sum()) if uncapped_members else float(result.sum())
    residual = max(0.0, 1.0 - capped_total)
    if uncapped_members and residual > 1e-12:
        uncapped_total = float(result.loc[uncapped_members].sum())
        if uncapped_total > 0:
            result.loc[uncapped_members] = result.loc[uncapped_members] * (residual / uncapped_total)

    return result


def allocate_weights(selected: pd.DataFrame, max_weight: float, sleeve_map: dict[str, str] | None = None, sleeve_budget: dict[str, float] | None = None) -> pd.Series:
    vol = selected['vol'].replace(0, np.nan)
    inv = (1.0/vol).replace([np.inf,-np.inf], np.nan).dropna()
    w = inv/inv.sum()
    w = w.clip(upper=float(max_weight))
    if w.sum()>0:
        w=w/w.sum()
    w = apply_sleeve_budget(w, sleeve_map, sleeve_budget)
    return w

def apply_risk_cuts(prices: pd.DataFrame, target_weights: pd.Series, risk_cfg: dict, cash_ticker: str) -> Tuple[pd.Series, pd.DataFrame]:
    if not risk_cfg.get('enabled', True) or target_weights.empty:
        return target_weights, pd.DataFrame()
    rules = risk_cfg['rules']
    action = risk_cfg['action']

    new_w=target_weights.copy()
    cash_add=0.0
    flags=[]
    for t,w in target_weights.items():
        if w<=0 or t not in prices.columns:
            continue
        p=prices[t].dropna()
        if len(p)<210:
            continue
        cut=False
        reasons=[]
        if rules.get('below_200sma_cut', True):
            s200=sma(p,200)
            if not np.isnan(s200) and p.iloc[-1]<s200:
                cut=True; reasons.append('below_200sma')
        dd_tr=trailing_drawdown(p,126)
        if not np.isnan(dd_tr) and dd_tr<=float(rules.get('trailing_dd_cut',-0.12)):
            cut=True; reasons.append(f"trail_dd{dd_tr:.2%}")
        dd_short=trailing_drawdown(p,63)
        if not np.isnan(dd_short) and dd_short<=float(rules.get('hard_stop_cut',-0.18)):
            cut=True; reasons.append(f"hard_stop{dd_short:.2%}")
        if cut:
            flags.append({'ticker':t,'reasons':','.join(reasons),'cut_weight':float(w)})
            cash_add += float(w)
            new_w[t]=0.0
    if not flags:
        return target_weights, pd.DataFrame()
    if action.get('cut_to_cash', True):
        if cash_ticker not in new_w.index:
            new_w.loc[cash_ticker]=0.0
        new_w.loc[cash_ticker]+=cash_add
    s=new_w.sum()
    if s>0:
        new_w=new_w/s
    return new_w, pd.DataFrame(flags)
