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
        "backtest": {"rebalance_frequency": "M", "transaction_cost_bps": 1.5, "starting_capital": 1.0, "benchmark_ticker": "SPY"},
    }


def test_run_backtest_returns_expected_shape(monkeypatch):
    cfg = _base_cfg()
    idx = pd.date_range("2024-01-01", periods=320, freq="B")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.2 for i in range(320)], index=idx),
        "SHY": pd.Series([100 + i * 0.01 for i in range(320)], index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(320)], index=idx),
        "HYG": pd.Series([100 + i * 0.1 for i in range(320)], index=idx),
        "IEF": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.02 for i in range(320)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(320)], index=idx),
    })

    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])

    result = run_backtest(cfg)
    assert isinstance(result, BacktestResult)
    assert not result.rebalance_log.empty
    assert "total_return" in result.summary
    assert "cagr" in result.summary
    assert "sharpe" in result.summary
    assert result.summary["rebalances"] >= 1
    assert result.benchmark_curve is not None
    assert result.analytics is not None


def test_run_backtest_counts_halts(monkeypatch):
    cfg = _base_cfg()
    cfg["strategy_halt"] = {
        "enabled": True,
        "max_cut_asset_ratio": 0.1,
        "min_risk_score": 1.0,
        "max_negative_momentum_assets": 1,
        "performance": {"enabled": True, "min_average_score": 1.0, "max_average_momentum": 1.0, "recent_total_return": 1.0, "benchmark_ticker": "SPY", "benchmark_relative_return": 1.0},
    }
    idx = pd.date_range("2024-01-01", periods=320, freq="B")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.01 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100.0] * 320, index=idx),
        "HYG": pd.Series([100.0] * 320, index=idx),
        "IEF": pd.Series([100.0] * 320, index=idx),
        "TLT": pd.Series([100.0] * 320, index=idx),
        "^VIX": pd.Series([18.0] * 320, index=idx),
    })

    monkeypatch.setattr("druck.backtest.make_universe", lambda cfg: type("U", (), {"kr": [], "us": cfg["universe"]["us"]["tickers"]})())
    monkeypatch.setattr("druck.backtest.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])

    result = run_backtest(cfg)
    assert result.summary["halt_count"] >= 1
