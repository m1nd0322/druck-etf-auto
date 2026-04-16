from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
import numpy as np
import pandas as pd
from .features import momentum_score, trend_score, rolling_vol, max_drawdown, zscore, sma, trailing_drawdown, persistence_score, recovery_score, downside_efficiency

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


def score_universe(prices: pd.DataFrame, sw: dict, regime_state: str | None = None, regime_factor_map: dict | None = None) -> pd.DataFrame:
    rows=[]
    for t in prices.columns:
        p=prices[t].dropna()
        if len(p)<260:
            continue
        rows.append({
            'ticker':t,
            'momentum':momentum_score(p),
            'trend':trend_score(p),
            'persistence': persistence_score(p, 126),
            'recovery': recovery_score(p, 126),
            'downside_efficiency': downside_efficiency(p, 126),
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
    df['legacy_score'] = _legacy_score(df, sw)
    df['score']=(
        float(sw['momentum'])*df['mom_z']
        + float(sw['trend'])*df['trend_z']
        + float(sw.get('persistence', 0.20))*df['persist_z']
        + float(sw.get('recovery', 0.15))*df['recovery_z']
        + float(sw.get('downside_efficiency', 0.15))*df['downside_z']
        - float(sw['vol_penalty'])*df['vol_z']
        - float(sw['dd_penalty'])*df['dd_z']
    )
    df['score_uplift'] = df['score'] - df['legacy_score']
    df = df.sort_values('score', ascending=False)
    if regime_state is not None:
        df = apply_regime_factor_bias(df, regime_state, regime_factor_map)
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
