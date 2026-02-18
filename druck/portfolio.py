from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple
import numpy as np
import pandas as pd
from .features import momentum_score, trend_score, rolling_vol, max_drawdown, zscore, sma, trailing_drawdown

@dataclass
class SelectionResult:
    scores: pd.DataFrame
    selected: pd.DataFrame
    weights: pd.Series

def score_universe(prices: pd.DataFrame, sw: dict) -> pd.DataFrame:
    rows=[]
    for t in prices.columns:
        p=prices[t].dropna()
        if len(p)<260:
            continue
        rows.append({
            'ticker':t,
            'momentum':momentum_score(p),
            'trend':trend_score(p),
            'vol':rolling_vol(p,63),
            'mdd_1y':max_drawdown(p,252),
        })
    df=pd.DataFrame(rows).set_index('ticker')
    if df.empty:
        return df
    df['mom_z']=zscore(df['momentum'])
    df['trend_z']=zscore(df['trend'].fillna(0.0))
    df['vol_z']=zscore(df['vol'])
    df['dd_z']=zscore(df['mdd_1y'])
    df['score']=(float(sw['momentum'])*df['mom_z'] + float(sw['trend'])*df['trend_z'] - float(sw['vol_penalty'])*df['vol_z'] - float(sw['dd_penalty'])*df['dd_z'])
    return df.sort_values('score', ascending=False)

def allocate_weights(selected: pd.DataFrame, max_weight: float) -> pd.Series:
    vol = selected['vol'].replace(0, np.nan)
    inv = (1.0/vol).replace([np.inf,-np.inf], np.nan).dropna()
    w = inv/inv.sum()
    w = w.clip(upper=float(max_weight))
    if w.sum()>0:
        w=w/w.sum()
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
