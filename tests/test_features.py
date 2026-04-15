import math

import pandas as pd

from druck.features import pct_change_n, rolling_vol, sma, trailing_drawdown, momentum_score, persistence_score, recovery_score, downside_efficiency


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
