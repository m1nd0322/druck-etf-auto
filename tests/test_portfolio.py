import math

import pandas as pd

from druck.portfolio import allocate_weights, apply_risk_cuts, score_universe


def test_allocate_weights_normalizes_and_caps():
    selected = pd.DataFrame({"vol": [0.1, 0.2, 0.4]}, index=["A", "B", "C"])
    weights = allocate_weights(selected, max_weight=0.6)
    assert math.isclose(weights.sum(), 1.0, rel_tol=1e-9)
    assert all(w <= 0.6 for w in weights)


def test_apply_risk_cuts_moves_weight_to_cash():
    idx = pd.date_range("2024-01-01", periods=260, freq="D")
    falling = pd.Series([200 - i * 0.5 for i in range(260)], index=idx)
    prices = pd.DataFrame({"ETF": falling, "SHY": pd.Series([100.0] * 260, index=idx)})
    weights = pd.Series({"ETF": 1.0})
    risk_cfg = {
        "enabled": True,
        "rules": {
            "below_200sma_cut": True,
            "trailing_dd_cut": -0.12,
            "hard_stop_cut": -0.18,
        },
        "action": {"cut_to_cash": True},
    }
    final_w, cuts = apply_risk_cuts(prices, weights, risk_cfg, cash_ticker="SHY")
    assert final_w["ETF"] == 0.0
    assert final_w["SHY"] == 1.0
    assert not cuts.empty


def test_score_universe_sorts_by_score_descending():
    idx = pd.date_range("2024-01-01", periods=300, freq="D")
    prices = pd.DataFrame({
        "A": pd.Series([100 + i for i in range(300)], index=idx),
        "B": pd.Series([100 + i * 0.3 for i in range(300)], index=idx),
        "C": pd.Series([100 + (-1) ** i * 0.5 + i * 0.1 for i in range(300)], index=idx),
    })
    sw = {"momentum": 0.35, "trend": 0.20, "persistence": 0.15, "recovery": 0.15, "downside_efficiency": 0.15, "vol_penalty": 0.10, "dd_penalty": 0.10}
    scores = score_universe(prices, sw)
    assert list(scores.index) == list(scores.sort_values("score", ascending=False).index)
    assert {"persistence", "recovery", "downside_efficiency"}.issubset(scores.columns)
