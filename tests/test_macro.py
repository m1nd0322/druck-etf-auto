import pandas as pd

from druck.macro import compute_macro_regime, is_vix_spike


def make_price_frame():
    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    return pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.5 for i in range(320)], index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(320)], index=idx),
        "HYG": pd.Series([100 + i * 0.15 for i in range(320)], index=idx),
        "IEF": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.04 for i in range(320)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(320)], index=idx),
    })


def test_is_vix_spike_detects_large_jump():
    idx = pd.date_range("2024-01-01", periods=40, freq="D")
    vix = [15.0] * 39 + [30.0]
    px = pd.DataFrame({"^VIX": pd.Series(vix, index=idx)})
    assert is_vix_spike(px) is True


def test_compute_macro_regime_returns_risk_on_for_supportive_inputs():
    px = make_price_frame()
    thresholds = {"risk_on_score_min": 0.55, "risk_off_score_max": 0.45}
    weights = {
        "spy_trend_weight": 0.30,
        "usd_mom_weight": 0.15,
        "credit_weight": 0.20,
        "vix_weight": 0.20,
        "rates_weight": 0.15,
    }
    regime = compute_macro_regime(px, thresholds, weights)
    assert regime.state == "RISK_ON"
    assert regime.risk_score >= 0.55
