import math

import pandas as pd

from druck.portfolio import allocate_weights, apply_risk_cuts, score_universe, apply_regime_factor_bias, apply_sleeve_budget, build_sleeve_map, resolve_regime_rotation, apply_sleeve_rotation


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


def test_apply_regime_factor_bias_prefers_configured_factor_tickers():
    scores = pd.DataFrame(
        {
            "score": [1.0, 0.9, 0.8],
        },
        index=["SPY", "MTUM", "USMV"],
    )
    biased = apply_regime_factor_bias(scores, "RISK_ON", {"RISK_ON": {"MTUM": 0.3}})
    assert biased.index[0] == "MTUM"


def test_apply_sleeve_budget_caps_sleeve_totals():
    weights = pd.Series({"SPY": 0.30, "MTUM": 0.30, "QUAL": 0.20, "XLF": 0.20})
    sleeve_map = {"SPY": "core", "MTUM": "factor", "QUAL": "factor", "XLF": "sector"}
    budgeted = apply_sleeve_budget(weights, sleeve_map, {"factor": 0.25, "sector": 0.35, "core": 0.50})
    assert budgeted[["MTUM", "QUAL"]].sum() <= 0.25 + 1e-9


def test_regime_rotation_prefers_configured_sleeves_and_top_n():
    scores = pd.DataFrame(
        {
            "score": [1.0, 0.99, 0.98, 0.97],
            "vol_z": [0.1, 0.1, 0.1, 0.1],
        },
        index=["SPY", "MTUM", "QUAL", "EWY"],
    )
    sleeve_map = build_sleeve_map(scores.index, {
        "factor_tickers": ["MTUM", "QUAL"],
        "sector_tickers": [],
        "country_tickers": ["EWY"],
    })
    rotation = resolve_regime_rotation(
        {
            "regime_sleeve_rotation": {
                "enabled": True,
                "RISK_ON": {
                    "top_n": 2,
                    "preferred_sleeves": ["factor"],
                    "score_tilt": {"factor": 0.2, "core": -0.1},
                    "sleeve_budget": {"factor": 0.7, "core": 0.3},
                }
            }
        },
        "RISK_ON",
        4,
        2,
    )
    rotated = apply_sleeve_rotation(scores, sleeve_map, rotation)
    assert rotated.index[:2].tolist() == ["MTUM", "QUAL"]
    assert rotation["top_n"] == 2


def test_score_universe_applies_benchmark_relative_penalty_to_target_sleeves():
    idx = pd.date_range("2024-01-01", periods=300, freq="D")
    prices = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.3 for i in range(300)], index=idx),
        "MTUM": pd.Series([100 + i * 0.1 for i in range(300)], index=idx),
        "QUAL": pd.Series([100 + i * 0.35 for i in range(300)], index=idx),
    })
    sleeve_map = {"SPY": "core", "MTUM": "factor", "QUAL": "factor"}
    sw = {"momentum": 0.35, "trend": 0.20, "persistence": 0.15, "recovery": 0.15, "downside_efficiency": 0.15, "relative_strength": 0.10, "vol_penalty": 0.10, "dd_penalty": 0.10}
    scored = score_universe(
        prices,
        sw,
        sleeve_map=sleeve_map,
        benchmark_ticker="SPY",
        relative_filter={"enabled": True, "mode": "penalty", "min_relative_strength_6m": -0.01, "penalty": 0.5, "apply_to_sleeves": ["factor"]},
    )
    assert "relative_strength_6m" in scored.columns
    assert "benchmark_relative_fail" in scored.columns
    assert bool(scored.loc["MTUM", "benchmark_relative_fail"]) is True
    assert bool(scored.loc["QUAL", "benchmark_relative_fail"]) is False


def test_score_universe_can_hard_exclude_benchmark_relative_failures():
    idx = pd.date_range("2024-01-01", periods=300, freq="D")
    prices = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.3 for i in range(300)], index=idx),
        "MTUM": pd.Series([100 + i * 0.1 for i in range(300)], index=idx),
        "QUAL": pd.Series([100 + i * 0.35 for i in range(300)], index=idx),
    })
    sleeve_map = {"SPY": "core", "MTUM": "factor", "QUAL": "factor"}
    sw = {"momentum": 0.35, "trend": 0.20, "persistence": 0.15, "recovery": 0.15, "downside_efficiency": 0.15, "relative_strength": 0.10, "vol_penalty": 0.10, "dd_penalty": 0.10}
    scored = score_universe(
        prices,
        sw,
        sleeve_map=sleeve_map,
        benchmark_ticker="SPY",
        relative_filter={"enabled": True, "mode": "exclude", "min_relative_strength_6m": -0.01, "penalty": 0.5, "apply_to_sleeves": ["factor"]},
    )
    assert "MTUM" not in scored.index
    assert "QUAL" in scored.index
