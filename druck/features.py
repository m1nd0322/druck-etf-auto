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


def persistence_score(prices: pd.Series, lookback: int = 126) -> float:
    r = prices.pct_change().dropna()
    if len(r) < lookback:
        return float("nan")
    window = r.iloc[-lookback:]
    positive_ratio = float((window > 0).mean())
    streak_bonus = float((window.rolling(5).mean().dropna() > 0).mean()) if len(window) >= 5 else 0.0
    return float(0.7 * positive_ratio + 0.3 * streak_bonus)


def recovery_score(prices: pd.Series, lookback: int = 126) -> float:
    if len(prices) < lookback:
        return float("nan")
    p = prices.iloc[-lookback:]
    peak = p.cummax()
    drawdown = p / peak - 1.0
    worst = float(drawdown.min())
    current = float(drawdown.iloc[-1])
    if worst >= 0:
        return 1.0
    return float(1.0 - min(max(current / worst, 0.0), 1.5))


def downside_efficiency(prices: pd.Series, lookback: int = 126) -> float:
    if len(prices) < lookback + 1:
        return float("nan")
    p = prices.iloc[-(lookback + 1):]
    total_return = float(p.iloc[-1] / p.iloc[0] - 1.0)
    drawdown = abs(max_drawdown(p, lookback=min(lookback, len(p))))
    negative_days = float((p.pct_change().dropna() < 0).mean())
    penalty = drawdown + negative_days + 1e-12
    return float(total_return / penalty)


def capacity_penalty_score(prices: pd.Series, lookback: int = 63) -> float:
    if len(prices) < lookback + 1:
        return float("nan")
    r = prices.pct_change().dropna().iloc[-lookback:]
    if r.empty:
        return float("nan")
    avg_abs_return = float(r.abs().mean())
    vol = float(r.std())
    if vol <= 0:
        return 1.0
    score = 1.0 / (1.0 + 10.0 * avg_abs_return + 5.0 * vol)
    return float(score)


def relative_strength_vs_benchmark(prices: pd.Series, benchmark: pd.Series, lookback: int = 126) -> float:
    if len(prices) < lookback + 1 or len(benchmark) < lookback + 1:
        return float("nan")
    p = prices.iloc[-(lookback + 1):]
    b = benchmark.iloc[-(lookback + 1):]
    aligned = pd.concat([p.rename("asset"), b.rename("benchmark")], axis=1).dropna()
    if len(aligned) < lookback + 1:
        return float("nan")
    asset_ret = float(aligned["asset"].iloc[-1] / aligned["asset"].iloc[0] - 1.0)
    bench_ret = float(aligned["benchmark"].iloc[-1] / aligned["benchmark"].iloc[0] - 1.0)
    return float(asset_ret - bench_ret)


def residual_strength_vs_anchors(prices: pd.Series, anchors: pd.DataFrame, lookback: int = 126) -> float:
    if anchors is None or anchors.empty or len(prices) < lookback + 1:
        return float("nan")
    asset_ret = prices.pct_change(fill_method=None)
    anchor_ret = anchors.pct_change(fill_method=None)
    frame = pd.concat([asset_ret.rename("asset"), anchor_ret], axis=1).dropna()
    if len(frame) < max(lookback, 20):
        return float("nan")
    window = frame.iloc[-lookback:]
    y = window["asset"].astype(float)
    x = window.drop(columns=["asset"]).astype(float)
    if x.empty:
        return float("nan")
    x = x.loc[:, x.std(ddof=0) > 1e-12]
    if x.empty:
        return float(y.mean() * 252)
    x_mat = np.column_stack([np.ones(len(x)), x.to_numpy()])
    y_vec = y.to_numpy()
    beta, *_ = np.linalg.lstsq(x_mat, y_vec, rcond=None)
    daily_alpha = beta[0]
    return float(daily_alpha * 252)


def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / (s.std(ddof=0) + 1e-12)
