from __future__ import annotations
import math
import numpy as np
import pandas as pd

def sma(prices: pd.Series, n: int) -> float:
    if len(prices) < n:
        return float("nan")
    return float(prices.iloc[-n:].mean())

def pct_change_n(prices: pd.Series, n: int) -> float:
    if len(prices) < n + 1:
        return float("nan")
    return float(prices.iloc[-1] / prices.iloc[-n-1] - 1.0)

def rolling_vol(prices: pd.Series, n: int = 63) -> float:
    r = prices.pct_change().dropna()
    if len(r) < n:
        return float("nan")
    return float(r.iloc[-n:].std() * math.sqrt(252))

def max_drawdown(prices: pd.Series, lookback: int = 252) -> float:
    if len(prices) == 0:
        return float("nan")
    if len(prices) < lookback:
        lookback = len(prices)
    p = prices.iloc[-lookback:]
    peak = p.cummax()
    dd = (p / peak - 1.0).min()
    return float(dd)

def trailing_drawdown(prices: pd.Series, lookback: int = 126) -> float:
    if len(prices) < 5:
        return float("nan")
    p = prices.iloc[-lookback:] if len(prices) >= lookback else prices
    peak = p.max()
    return float(p.iloc[-1] / peak - 1.0)

def trend_score(prices: pd.Series) -> float:
    s50 = sma(prices, 50)
    s200 = sma(prices, 200)
    if np.isnan(s50) or np.isnan(s200):
        return float("nan")
    now = prices.iloc[-1]
    score = 0.0
    score += 0.6 if now > s200 else 0.0
    score += 0.4 if s50 > s200 else 0.0
    return float(score)

def momentum_score(prices: pd.Series) -> float:
    r3 = pct_change_n(prices, 63)
    r6 = pct_change_n(prices, 126)
    r12 = pct_change_n(prices, 252)
    if np.isnan(r3) or np.isnan(r6) or np.isnan(r12):
        return float("nan")
    return float(0.5 * r3 + 0.3 * r6 + 0.2 * r12)

def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / (s.std(ddof=0) + 1e-12)
