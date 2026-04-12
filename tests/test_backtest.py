from pathlib import Path

import pytest
import pandas as pd

from druck.backtest import BacktestResult, run_backtest


def _base_cfg():
    return {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
        "universe": {"kr": {"auto_generate": False, "tickers": []}, "us": {"tickers": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"]}},
        "selection": {
            "top_n_risk_on": 2,
            "top_n_risk_off": 1,
            "max_weight": 1.0,
            "score_weights": {"momentum": 0.55, "trend": 0.25, "vol_penalty": 0.10, "dd_penalty": 0.10},
        },
        "macro_filter": {
            "thresholds": {"risk_on_score_min": 0.55, "risk_off_score_max": 0.45},
            "components": {"spy_trend_weight": 0.3, "usd_mom_weight": 0.15, "credit_weight": 0.2, "vix_weight": 0.2, "rates_weight": 0.15},
        },
        "risk_cut": {
            "enabled": True,
            "rules": {"below_200sma_cut": True, "trailing_dd_cut": -0.12, "hard_stop_cut": -0.18},
            "action": {"cut_to_cash": True, "cash_us": "SHY", "cash_kr": "130730.KS"},
        },
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True, "commission_bps": 1.5},
        "strategy_halt": {"enabled": False},
        "backtest": {
            "rebalance_frequency": "M",
            "transaction_cost_bps": 1.5,
            "slippage_bps": 3.0,
            "market_impact_bps_per_turnover": 5.0,
            "liquidity_vol_multiplier_bps": 2.0,
            "starting_capital": 1.0,
            "benchmark_ticker": "SPY",
            "min_history_days": 252,
            "strict_point_in_time": True,
            "drop_incomplete_assets": True,
            "enforce_delist_exit": True,
            "universe_timeline_path": "",
            "volume_data_path": "",
            "adv_window_days": 20,
            "max_participation_rate": 0.10,
            "capacity_safety_factor": 0.25,
            "scenarios": {"enabled": True, "stress_return_shock": -0.05, "vol_multiplier": 1.5},
            "walkforward": {"enabled": True, "train_days": 252, "test_days": 42, "step_days": 42},
        },
    }


def test_run_backtest_returns_expected_shape(monkeypatch):
    cfg = _base_cfg()
    idx = pd.date_range("2024-01-01", periods=420, freq="B")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.2 for i in range(420)], index=idx),
        "SHY": pd.Series([100 + i * 0.01 for i in range(420)], index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(420)], index=idx),
        "HYG": pd.Series([100 + i * 0.1 for i in range(420)], index=idx),
        "IEF": pd.Series([100 + i * 0.03 for i in range(420)], index=idx),
        "TLT": pd.Series([100 + i * 0.02 for i in range(420)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(420)], index=idx),
    })

    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])

    result = run_backtest(cfg)
    assert isinstance(result, BacktestResult)
    assert not result.rebalance_log.empty
    assert "total_return" in result.summary
    assert "cagr" in result.summary
    assert "sharpe" in result.summary
    assert "sortino" in result.summary
    assert "calmar" in result.summary
    assert result.summary["rebalances"] >= 1
    assert result.benchmark_curve is not None
    assert result.analytics is not None
    assert "walkforward_windows" in result.analytics
    assert result.walkforward_summary is not None
    assert "factor_regime_attribution" in result.analytics
    assert result.scenario_summary is not None
    assert not result.scenario_summary.empty


def test_run_backtest_counts_halts(monkeypatch):
    cfg = _base_cfg()
    cfg["strategy_halt"] = {
        "enabled": True,
        "max_cut_asset_ratio": 0.1,
        "min_risk_score": 1.0,
        "max_negative_momentum_assets": 1,
        "performance": {"enabled": True, "min_average_score": 1.0, "max_average_momentum": 1.0, "recent_total_return": 1.0, "benchmark_ticker": "SPY", "benchmark_relative_return": 1.0},
    }
    idx = pd.date_range("2024-01-01", periods=420, freq="B")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.01 for i in range(420)], index=idx),
        "SHY": pd.Series([100.0] * 420, index=idx),
        "UUP": pd.Series([100.0] * 420, index=idx),
        "HYG": pd.Series([100.0] * 420, index=idx),
        "IEF": pd.Series([100.0] * 420, index=idx),
        "TLT": pd.Series([100.0] * 420, index=idx),
        "^VIX": pd.Series([18.0] * 420, index=idx),
    })

    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])

    result = run_backtest(cfg)
    assert result.summary["halt_count"] >= 1


def test_run_backtest_tracks_slippage_impact_and_liquidity_costs(monkeypatch, tmp_path):
    cfg = _base_cfg()
    idx = pd.date_range("2024-01-01", periods=420, freq="B")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.2 for i in range(420)], index=idx),
        "SHY": pd.Series([90 + i * 0.05 for i in range(420)], index=idx),
        "UUP": pd.Series([110 - i * 0.03 for i in range(420)], index=idx),
        "HYG": pd.Series([95 + i * 0.08 for i in range(420)], index=idx),
        "IEF": pd.Series([100 + i * 0.01 for i in range(420)], index=idx),
        "TLT": pd.Series([98 + i * 0.02 for i in range(420)], index=idx),
        "^VIX": pd.Series([16 + (i % 4) * 0.2 for i in range(420)], index=idx),
    })
    volume = pd.DataFrame({ticker: [1_000_000 + i * 1000 for i in range(420)] for ticker in px.columns if ticker != "^VIX"}, index=idx)
    volume_path = tmp_path / "volume.csv"
    volume.to_csv(volume_path)
    cfg["backtest"]["volume_data_path"] = str(volume_path)
    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    result = run_backtest(cfg)
    assert result.summary["total_slippage_cost"] >= 0.0
    assert result.summary["total_impact_cost"] >= 0.0
    assert result.summary["total_liquidity_penalty"] >= 0.0
    assert result.summary["avg_adv_20d"] > 0.0
    assert result.summary["avg_participation_rate"] == pytest.approx(cfg["backtest"]["max_participation_rate"])
    assert result.summary["avg_capacity_estimate"] > 0.0


def test_run_backtest_drops_incomplete_assets_and_marks_delisted(monkeypatch, tmp_path):
    cfg = _base_cfg()
    idx = pd.date_range("2024-01-01", periods=420, freq="B")
    late_series = pd.Series([200 + i * 0.2 for i in range(100)], index=idx[-100:])
    delisted_series = pd.Series([150 + i * 0.1 for i in range(300)], index=idx[:300])
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.2 for i in range(420)], index=idx),
        "SHY": pd.Series([100.0] * 420, index=idx),
        "UUP": pd.Series([100.0] * 420, index=idx),
        "HYG": pd.Series([100.0] * 420, index=idx),
        "IEF": pd.Series([100.0] * 420, index=idx),
        "TLT": pd.Series([100.0] * 420, index=idx),
        "^VIX": pd.Series([18.0] * 420, index=idx),
        "LATE": late_series,
        "DELIST": delisted_series,
    })
    timeline = pd.DataFrame([
        {"ticker": "SPY", "start_date": "2024-01-01", "end_date": ""},
        {"ticker": "LATE", "start_date": str(idx[-100].date()), "end_date": ""},
        {"ticker": "DELIST", "start_date": "2024-01-01", "end_date": str(idx[299].date())},
    ])
    timeline_path = tmp_path / "timeline.csv"
    timeline.to_csv(timeline_path, index=False)
    cfg["backtest"]["universe_timeline_path"] = str(timeline_path)
    cfg["universe"]["us"]["tickers"].extend(["LATE", "DELIST"])
    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    result = run_backtest(cfg)
    for weights in result.rebalance_log["weights"]:
        assert "LATE" not in weights
    assert "LATE" in result.summary["dropped_incomplete_assets"]
    assert "DELIST" in result.summary["delisted_assets"]
    assert result.summary["timeline_applied"] is True
    last_weights = result.rebalance_log.iloc[-1]["weights"]
    assert "DELIST" not in last_weights or last_weights.get("DELIST", 0.0) == 0.0
