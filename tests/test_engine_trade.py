import pandas as pd
import pytest

from druck.engine import run_once
from druck.runtime import StrategyHaltError


class FakeBroker:
    def get_positions(self):
        return {"SPY": 2}

    def get_cash(self):
        return 100.0

    def get_portfolio_value(self):
        return 300.0

    def get_last_price(self, ticker):
        return {"SPY": 100.0, "SHY": 50.0}.get(ticker, 0.0)

    def place_order(self, ticker, qty, side, order_type="MKT"):
        return None


def test_run_once_blocks_live_execution_when_review_fails(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
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
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
        "strategy_halt": {"enabled": False},
    }

    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.5 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(320)], index=idx),
        "HYG": pd.Series([100 + i * 0.15 for i in range(320)], index=idx),
        "IEF": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.04 for i in range(320)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(320)], index=idx),
    })

    monkeypatch.setattr("druck.engine.make_universe", lambda cfg: type("U", (), {"kr": [], "us": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"]})())
    monkeypatch.setattr("druck.engine.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    monkeypatch.setattr("druck.engine.save_report", lambda out_dir, selection, regime_details, cuts: "output/report_test.md")
    monkeypatch.setattr("druck.engine.send_telegram", lambda cfg, msg: None)

    with pytest.raises(Exception):
        run_once(cfg, do_trade=True, broker=FakeBroker())


def test_run_once_halts_trading_when_strategy_signal_is_dangerous(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": True, "dry_run": False},
        "kiwoom": {"account_no": "123"},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
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
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
        "strategy_halt": {"enabled": True, "max_cut_asset_ratio": 0.7, "min_risk_score": 0.4, "max_negative_momentum_assets": 1},
    }

    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    px = pd.DataFrame({
        "SPY": pd.Series([100 - i * 0.2 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "HYG": pd.Series([100 - i * 0.10 for i in range(320)], index=idx),
        "IEF": pd.Series([100 + i * 0.01 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.01 for i in range(320)], index=idx),
        "^VIX": pd.Series([30 + (i % 3) * 0.2 for i in range(320)], index=idx),
    })

    monkeypatch.setattr("druck.engine.make_universe", lambda cfg: type("U", (), {"kr": [], "us": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"]})())
    monkeypatch.setattr("druck.engine.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    monkeypatch.setattr("druck.engine.save_report", lambda out_dir, selection, regime_details, cuts: "output/report_test.md")
    monkeypatch.setattr("druck.engine.send_telegram", lambda cfg, msg: None)

    with pytest.raises(StrategyHaltError):
        run_once(cfg, do_trade=True, broker=FakeBroker())


def test_run_once_halts_trading_on_performance_degradation(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": True, "dry_run": False},
        "kiwoom": {"account_no": "123"},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
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
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
        "strategy_halt": {
            "enabled": True,
            "max_cut_asset_ratio": 0.99,
            "min_risk_score": 0.0,
            "max_negative_momentum_assets": 99,
            "performance": {"enabled": True, "min_average_score": 0.5, "max_average_momentum": -1.0},
        },
    }

    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + ((-1) ** i) * 0.01 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100.0] * 320, index=idx),
        "HYG": pd.Series([100 + ((-1) ** i) * 0.01 for i in range(320)], index=idx),
        "IEF": pd.Series([100.0] * 320, index=idx),
        "TLT": pd.Series([100.0] * 320, index=idx),
        "^VIX": pd.Series([18.0] * 320, index=idx),
    })

    monkeypatch.setattr("druck.engine.make_universe", lambda cfg: type("U", (), {"kr": [], "us": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"]})())
    monkeypatch.setattr("druck.engine.fetch_prices", lambda tickers, start, end, prefer='auto', cache_dir=None, use_cache=True: px[tickers])
    monkeypatch.setattr("druck.engine.save_report", lambda out_dir, selection, regime_details, cuts: "output/report_test.md")
    monkeypatch.setattr("druck.engine.send_telegram", lambda cfg, msg: None)

    with pytest.raises(StrategyHaltError):
        run_once(cfg, do_trade=True, broker=FakeBroker())
