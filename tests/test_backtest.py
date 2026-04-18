from pathlib import Path

import pytest
import pandas as pd

from druck.backtest import BacktestResult, run_backtest


def _base_cfg():
    return {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
        "universe": {"kr": {"auto_generate": False, "tickers": []}, "us": {"tickers": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"], "factor_tickers": ["HYG", "IEF"], "sector_tickers": ["TLT"], "country_tickers": ["UUP"]}},
        "selection": {
            "top_n_risk_on": 2,
            "top_n_risk_off": 1,
            "max_weight": 1.0,
            "score_weights": {"momentum": 0.35, "trend": 0.20, "persistence": 0.15, "recovery": 0.15, "downside_efficiency": 0.15, "relative_strength": 0.10, "vol_penalty": 0.10, "dd_penalty": 0.10},
            "benchmark_relative_filter": {"enabled": True, "min_relative_strength_6m": -0.01, "penalty": 0.25, "apply_to_sleeves": ["factor", "sector", "country"]},
            "regime_sleeve_rotation": {
                "enabled": True,
                "RISK_ON": {"top_n": 2, "preferred_sleeves": ["factor"], "sleeve_budget": {"factor": 0.7, "core": 0.3}, "score_tilt": {"factor": 0.2}},
                "NEUTRAL": {"top_n": 2, "preferred_sleeves": ["core"], "sleeve_budget": {"core": 0.6, "factor": 0.2}, "score_tilt": {"core": 0.05}},
                "RISK_OFF": {"top_n": 1, "preferred_sleeves": ["core"], "sleeve_budget": {"core": 0.8, "factor": 0.1}, "score_tilt": {"core": 0.1}},
            },
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
            "scenarios": {
                "enabled": True,
                "stress_return_shock": -0.05,
                "vol_multiplier": 1.5,
                "presets": [
                    {"name": "return_shock_and_vol_up", "severity": "high", "tags": ["stress", "drawdown", "volatility"], "operator_action": "reduce risk and review exposures", "review_required": True, "note_template": "Review drawdown-sensitive positions and confirm defensive posture.", "return_shock": -0.05, "vol_multiplier": 1.5, "benchmark_shock": 0.0},
                    {"name": "benchmark_gap_down", "severity": "high", "tags": ["stress", "benchmark", "gap"], "operator_action": "compare active risk versus benchmark and review hedge stance", "review_required": True, "note_template": "Check benchmark-relative weakness and decide whether to de-risk or hold.", "return_shock": -0.02, "vol_multiplier": 1.2, "benchmark_shock": -0.04},
                    {"name": "volatility_crush", "severity": "medium", "tags": ["comparison", "volatility"], "operator_action": "monitor regime shift assumptions", "review_required": False, "note_template": "Confirm whether lower-volatility assumptions still match current market structure.", "return_shock": 0.0, "vol_multiplier": 0.7, "benchmark_shock": 0.0},
                ],
            },
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
    assert "selection_score_comparison" in result.analytics
    assert "avg_overlap_ratio" in result.analytics["selection_score_comparison"]
    assert "avg_factor_selected_ratio" in result.analytics["selection_score_comparison"]
    assert "latest_factor_selected_tickers" in result.analytics["selection_score_comparison"]
    assert "avg_relative_strength" in result.analytics["selection_score_comparison"]
    assert "avg_benchmark_relative_fail_count" in result.analytics["selection_score_comparison"]
    assert "avg_rotation_top_n" in result.analytics["selection_score_comparison"]
    assert "latest_rotation_preferred_sleeves" in result.analytics["selection_score_comparison"]
    assert "latest_rotation_sleeve_budget" in result.analytics["selection_score_comparison"]
    assert "latest_selected_sleeves" in result.analytics["selection_score_comparison"]
    assert "latest_legacy_top_picks" in result.analytics["selection_score_comparison"]
    assert "latest_alpha_top_picks" in result.analytics["selection_score_comparison"]
    assert "strategy_comparison" in result.analytics
    assert "return_delta" in result.analytics["strategy_comparison"]
    assert "active_return_delta" in result.analytics["strategy_comparison"]
    assert "scenario_robustness_deltas" in result.analytics["strategy_comparison"]
    assert "scenario_win_count" in result.analytics["strategy_comparison"]
    assert "scenario_loss_count" in result.analytics["strategy_comparison"]
    assert "scenario_net_wins" in result.analytics["strategy_comparison"]
    assert "worst_scenario_total_return_delta" in result.analytics["strategy_comparison"]
    assert "robustness_summary" in result.analytics["strategy_comparison"]
    assert result.scenario_summary is not None
    assert not result.scenario_summary.empty
    assert set(["scenario", "severity", "tags", "operator_action", "review_required", "note_template", "scenario_total_return", "benchmark_relative_return"]).issubset(result.scenario_summary.columns)
    assert len(result.scenario_summary) >= 3


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
    assert result.analytics is not None
    assert "capacity_warning" in result.analytics


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


def test_run_backtest_flags_capacity_warning_when_portfolio_exceeds_estimate(monkeypatch, tmp_path):
    cfg = _base_cfg()
    cfg["backtest"]["starting_capital"] = 1_000_000.0
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
    volume = pd.DataFrame({ticker: [10 + i for i in range(420)] for ticker in px.columns if ticker != "^VIX"}, index=idx)
    volume_path = tmp_path / "thin_volume.csv"
    volume.to_csv(volume_path)
    cfg["backtest"]["volume_data_path"] = str(volume_path)
    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    result = run_backtest(cfg)
    assert result.analytics is not None
    assert result.analytics["capacity_warning"] is not None
    assert result.analytics["capacity_warning"]["status"] == "warning"
