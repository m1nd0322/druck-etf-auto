import math

import pandas as pd
import pytest

from druck.features import pct_change_n, rolling_vol, sma, trailing_drawdown, momentum_score, persistence_score, recovery_score, downside_efficiency, relative_strength_vs_benchmark, capacity_penalty_score, residual_strength_vs_anchors


def test_sma_returns_last_window_average():
    s = pd.Series([1, 2, 3, 4, 5])
    assert sma(s, 3) == 4.0


def test_pct_change_n_uses_n_plus_one_offset():
    s = pd.Series([100, 105, 110, 120])
    assert math.isclose(pct_change_n(s, 2), 120 / 105 - 1.0, rel_tol=1e-9)


def test_rolling_vol_positive_for_changing_series():
    s = pd.Series([100, 101, 103, 102, 106, 108, 107, 110, 111, 115])
    assert rolling_vol(s, 5) > 0


def test_trailing_drawdown_uses_peak_in_lookback():
    s = pd.Series([100, 110, 120, 118, 90])
    assert math.isclose(trailing_drawdown(s, 5), 90 / 120 - 1.0, rel_tol=1e-9)


def test_momentum_score_positive_for_uptrend():
    s = pd.Series(range(1, 400))
    assert momentum_score(s) > 0


def test_persistence_score_higher_for_smoother_uptrend():
    smooth = pd.Series([100 + i for i in range(200)])
    choppy = pd.Series([100 + i + ((-1) ** i) * 3 for i in range(200)])
    assert persistence_score(smooth, 126) > persistence_score(choppy, 126)


def test_recovery_score_improves_after_rebound():
    weak = pd.Series([100] * 50 + [80] * 50 + [82] * 50)
    rebound = pd.Series([100] * 50 + [80] * 50 + [95] * 50)
    assert recovery_score(rebound, 126) > recovery_score(weak, 126)


def test_downside_efficiency_prefers_better_return_per_downside_vol():
    smooth = pd.Series([100 + i * 0.5 for i in range(200)])
    noisy = pd.Series([100 + i * 0.5 + ((-1) ** i) * 2 for i in range(200)])
    assert downside_efficiency(smooth, 126) > downside_efficiency(noisy, 126)


def test_relative_strength_vs_benchmark_positive_when_asset_outperforms():
    asset = pd.Series([100 + i * 1.0 for i in range(200)])
    benchmark = pd.Series([100 + i * 0.5 for i in range(200)])
    assert relative_strength_vs_benchmark(asset, benchmark, 126) > 0


def test_capacity_penalty_score_prefers_smoother_series():
    smooth = pd.Series([100 + i * 0.2 for i in range(200)])
    jumpy = pd.Series([100 + i * 0.2 + ((-1) ** i) * 3 for i in range(200)])
    assert capacity_penalty_score(smooth, 63) > capacity_penalty_score(jumpy, 63)


def _prices_from_returns(returns: pd.Series) -> pd.Series:
    return pd.Series([100.0, *(100.0 * (1.0 + returns).cumprod())])


def test_residual_strength_vs_anchors_returns_annualized_regression_alpha():
    anchor_returns = pd.DataFrame(
        {
            "SPY": pd.Series([(-1) ** i * 0.004 for i in range(130)]),
            "TLT": pd.Series([((i % 5) - 2) * 0.001 for i in range(130)]),
            "UUP": pd.Series([((i % 7) - 3) * 0.0005 for i in range(130)]),
        }
    )
    anchors = anchor_returns.apply(_prices_from_returns)
    asset_returns = 0.001 + 0.5 * anchor_returns["SPY"] - 0.2 * anchor_returns["TLT"]

    result = residual_strength_vs_anchors(_prices_from_returns(asset_returns), anchors, 126)

    assert result == pytest.approx(0.252, abs=1e-10)


def test_residual_strength_vs_anchors_preserves_negative_alpha():
    anchor_returns = pd.DataFrame(
        {
            "SPY": pd.Series([(-1) ** i * 0.004 for i in range(130)]),
            "TLT": pd.Series([((i % 5) - 2) * 0.001 for i in range(130)]),
        }
    )
    anchors = anchor_returns.apply(_prices_from_returns)
    asset_returns = -0.0005 + 0.4 * anchor_returns["SPY"] + 0.1 * anchor_returns["TLT"]

    result = residual_strength_vs_anchors(_prices_from_returns(asset_returns), anchors, 126)

    assert result == pytest.approx(-0.126, abs=1e-10)


def test_residual_strength_vs_anchors_annualizes_when_anchors_are_constant():
    anchors = pd.DataFrame(
        {
            "SPY": _prices_from_returns(pd.Series([0.0] * 130)),
            "TLT": _prices_from_returns(pd.Series([0.0] * 130)),
        }
    )
    asset = _prices_from_returns(pd.Series([0.001] * 130))

    result = residual_strength_vs_anchors(asset, anchors, 126)

    assert result == pytest.approx(0.252, abs=1e-10)
