from __future__ import annotations
from dataclasses import dataclass
from typing import Dict
import numpy as np
import pandas as pd
from .features import sma, momentum_score, pct_change_n

@dataclass
class MacroRegime:
    risk_score: float
    state: str  # RISK_ON|NEUTRAL|RISK_OFF
    details: Dict[str, float]

def _clamp01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))

def is_vix_spike(px: pd.DataFrame, mult: float = 1.25) -> bool:
    vix = px.get("^VIX")
    if vix is None or len(vix.dropna()) < 30:
        return False
    v = vix.dropna()
    cur = float(v.iloc[-1])
    ma20 = float(v.rolling(20).mean().iloc[-1])
    return cur > ma20 * float(mult)

def compute_macro_regime(px: pd.DataFrame, thresholds: dict, weights: dict) -> MacroRegime:
    details: Dict[str, float] = {}

    spy = px.get("SPY")
    spy_trend = np.nan
    if spy is not None and len(spy.dropna()) > 210:
        s200 = sma(spy.dropna(), 200)
        spy_trend = float(spy.dropna().iloc[-1] > s200) if not np.isnan(s200) else np.nan
    details["spy_trend"] = float(spy_trend) if not np.isnan(spy_trend) else np.nan

    uup = px.get("UUP")
    usd_component = np.nan
    if uup is not None and len(uup.dropna()) > 260:
        usd_mom = momentum_score(uup.dropna())
        if not np.isnan(usd_mom):
            usd_component = _clamp01(0.5 - usd_mom)
    details["usd_component"] = float(usd_component) if not np.isnan(usd_component) else np.nan

    hyg, ief = px.get("HYG"), px.get("IEF")
    credit_component = np.nan
    if hyg is not None and ief is not None:
        common = pd.concat([hyg, ief], axis=1).dropna()
        if len(common) > 90:
            hyg_r = float(common.iloc[-1,0] / common.iloc[-64,0] - 1.0)
            ief_r = float(common.iloc[-1,1] / common.iloc[-64,1] - 1.0)
            credit_component = _clamp01(0.5 + (hyg_r - ief_r))
    details["credit_component"] = float(credit_component) if not np.isnan(credit_component) else np.nan

    vix = px.get("^VIX")
    vix_component = np.nan
    if vix is not None and len(vix.dropna()) > 120:
        v = vix.dropna()
        cur = float(v.iloc[-1])
        mom1m = pct_change_n(v, 21)
        base = _clamp01(1.0 - (cur - 15.0) / 20.0)
        shock = _clamp01(1.0 - max(0.0, mom1m) / 0.3) if not np.isnan(mom1m) else 0.5
        vix_component = 0.6 * base + 0.4 * shock
    details["vix_component"] = float(vix_component) if not np.isnan(vix_component) else np.nan

    tlt = px.get("TLT")
    rates_component = np.nan
    if tlt is not None and len(tlt.dropna()) > 260:
        m = momentum_score(tlt.dropna())
        if not np.isnan(m):
            rates_component = _clamp01(0.5 + m)
    details["rates_component"] = float(rates_component) if not np.isnan(rates_component) else np.nan

    comp = {
        "spy_trend": ("spy_trend", "spy_trend_weight"),
        "usd_component": ("usd_component", "usd_mom_weight"),
        "credit_component": ("credit_component", "credit_weight"),
        "vix_component": ("vix_component", "vix_weight"),
        "rates_component": ("rates_component", "rates_weight"),
    }

    used=[]
    for k,(_,wk) in comp.items():
        val=details.get(k, np.nan)
        if not (val is None or np.isnan(val)):
            used.append((k, float(weights.get(wk,0.0))))
    if not used:
        return MacroRegime(0.5,"NEUTRAL",details)

    wsum=sum(w for _,w in used)+1e-12
    score=0.0
    for k,w in used:
        score += (w/wsum)*float(details[k])
    score=float(score)

    on_min=float(thresholds.get("risk_on_score_min",0.55))
    off_max=float(thresholds.get("risk_off_score_max",0.45))
    if score>=on_min:
        state="RISK_ON"
    elif score<=off_max:
        state="RISK_OFF"
    else:
        state="NEUTRAL"
    details["risk_score"]=score
    return MacroRegime(score,state,details)
