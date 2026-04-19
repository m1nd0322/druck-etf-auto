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
            "regime_sleeve_rotation": {"enabled": True, "RISK_ON": {"top_n": 2, "preferred_sleeves": ["factor"], "sleeve_budget": {"factor": 0.7, "core": 0.3}, "score_tilt": {"factor": 0.2}}},
            "regime_factor_bias": {"RISK_ON": {"HYG": 0.1}},
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
            "regime_sleeve_rotation": {"enabled": True, "RISK_OFF": {"top_n": 1, "preferred_sleeves": ["core"], "sleeve_budget": {"core": 0.8, "factor": 0.2}, "score_tilt": {"core": 0.1}}},
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
            "regime_sleeve_rotation": {"enabled": True, "NEUTRAL": {"top_n": 2, "preferred_sleeves": ["core"], "sleeve_budget": {"core": 0.6, "factor": 0.2}, "score_tilt": {"core": 0.05}}},
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
            "performance": {
                "enabled": True,
                "min_average_score": 0.5,
                "max_average_momentum": -1.0,
                "recent_total_return": -0.5,
                "benchmark_ticker": "SPY",
                "benchmark_relative_return": -1.0,
            },
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


def test_run_once_supports_kr_sleeve_rotation_and_budgeting(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
        "universe": {
            "kr": {
                "auto_generate": False,
                "tickers": ["069500.KS", "091160.KS", "139230.KS", "130730.KS"],
                "core_tickers": ["069500.KS"],
                "attack_tickers": ["091160.KS"],
                "satellite_tickers": ["139230.KS"],
                "defensive_tickers": ["130730.KS"],
                "cash_ticker": "130730.KS",
            },
            "us": {"tickers": ["SPY", "TLT", "UUP", "^VIX"], "factor_tickers": [], "sector_tickers": [], "country_tickers": []},
        },
        "selection": {
            "top_n_risk_on": 2,
            "top_n_risk_off": 1,
            "max_weight": 1.0,
            "score_weights": {"momentum": 0.35, "trend": 0.20, "persistence": 0.15, "recovery": 0.15, "downside_efficiency": 0.15, "relative_strength": 0.10, "vol_penalty": 0.10, "dd_penalty": 0.10},
            "benchmark_relative_filter": {"enabled": True, "mode": "exclude", "min_relative_strength_6m": 0.0, "penalty": 0.25, "apply_to_sleeves": ["kr_attack"]},
            "strategy_family": "dual_momentum",
            "dual_momentum_top_n": 2,
            "benchmark_overlay": {"enabled": False, "benchmark_ticker": "069500.KS", "base_weight": 0.70, "attack_overlay_weight": 0.20, "satellite_overlay_weight": 0.10},
            "regime_sleeve_rotation": {"enabled": True, "RISK_ON": {"top_n": 3, "preferred_sleeves": ["kr_attack", "kr_satellite"], "sleeve_budget": {"kr_core": 0.45, "kr_attack": 0.25, "kr_satellite": 0.15, "defensive": 0.20}, "score_tilt": {"kr_attack": 0.18, "kr_satellite": 0.08, "kr_core": 0.04}, "timing_filters": {"kr_satellite": {"mode": "exclude", "min_momentum": 0.05, "min_trend": 0.05}, "kr_attack": {"mode": "penalty", "min_relative_strength_6m": 0.04, "penalty": 0.10}}}},
        },
        "macro_filter": {
            "thresholds": {"risk_on_score_min": 0.55, "risk_off_score_max": 0.45},
            "components": {"spy_trend_weight": 0.35, "usd_mom_weight": 0.20, "credit_weight": 0.0, "vix_weight": 0.25, "rates_weight": 0.20},
            "rates_overlay": {"up_threshold": 0.02, "down_threshold": -0.02},
        },
        "risk_cut": {
            "enabled": True,
            "rules": {"below_200sma_cut": True, "trailing_dd_cut": -0.12, "hard_stop_cut": -0.18},
            "action": {"cut_to_cash": True, "cash_us": "SHY", "cash_kr": "130730.KS"},
        },
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
        "strategy_halt": {"enabled": False},
        "backtest": {"benchmark_ticker": "069500.KS"},
    }

    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    kr_px = pd.DataFrame({
        "069500.KS": pd.Series([100 + i * 0.22 for i in range(320)], index=idx),
        "091160.KS": pd.Series([100 + i * 0.10 for i in range(320)], index=idx),
        "139230.KS": pd.Series([100 + i * 0.18 for i in range(320)], index=idx),
        "130730.KS": pd.Series([100 + i * 0.01 for i in range(320)], index=idx),
    })
    us_px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.40 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(320)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(320)], index=idx),
    })

    monkeypatch.setattr("druck.engine.make_universe", lambda cfg: type("U", (), {"kr": ["069500.KS", "091160.KS", "139230.KS", "130730.KS"], "us": ["SPY", "TLT", "UUP", "^VIX"]})())

    def fake_fetch_prices(tickers, start, end, prefer='auto', cache_dir=None, use_cache=True):
        frame = kr_px if any(str(t).endswith('.KS') for t in tickers) else us_px
        return frame[tickers]

    monkeypatch.setattr("druck.engine.fetch_prices", fake_fetch_prices)
    monkeypatch.setattr("druck.engine.save_report", lambda out_dir, selection, regime_details, cuts: "output/report_test.md")
    monkeypatch.setattr("druck.engine.send_telegram", lambda cfg, msg: None)

    result = run_once(cfg, do_trade=False)
    assert result["target_weights"].sum() == pytest.approx(1.0)
    assert "069500.KS" in result["target_weights"].index
    assert result["selected_sleeves"]["069500.KS"] == "kr_core"
    assert result["rotation_policy"]["preferred_sleeves"] == ["kr_attack", "kr_satellite"]
    assert result["rotation_policy"]["sleeve_budget"]["kr_satellite"] == 0.15
    assert result["rotation_policy"]["timing_filters"]["kr_satellite"]["mode"] == "exclude"
    assert result["rotation_policy"].get("budget_throttle_applied") is False
    assert "091160.KS" not in result["scores"].index



def test_run_once_returns_provider_warning_summary(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
        "selection": {
            "top_n_risk_on": 2,
            "top_n_risk_off": 1,
            "max_weight": 1.0,
            "score_weights": {"momentum": 0.55, "trend": 0.25, "vol_penalty": 0.10, "dd_penalty": 0.10},
            "regime_sleeve_rotation": {"enabled": False},
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
    us_px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.4 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100 - i * 0.02 for i in range(320)], index=idx),
        "HYG": pd.Series([100 + i * 0.10 for i in range(320)], index=idx),
        "IEF": pd.Series([100 + i * 0.03 for i in range(320)], index=idx),
        "TLT": pd.Series([100 + i * 0.04 for i in range(320)], index=idx),
        "^VIX": pd.Series([15 + (i % 3) * 0.1 for i in range(320)], index=idx),
    })
    us_px.attrs["provider_warning_summary"] = {
        "status": "warning",
        "summary": "provider rate-limit detected (XLV)",
        "counts": {"rate_limit": 1},
        "tickers": {"rate_limit": ["XLV"]},
        "messages": ["provider rate-limit detected (XLV)"],
        "issues": [],
    }
    kr_px = pd.DataFrame(index=idx)

    monkeypatch.setattr("druck.engine.make_universe", lambda cfg: type("U", (), {"kr": [], "us": ["SPY", "SHY", "UUP", "HYG", "IEF", "TLT", "^VIX"]})())

    def fake_fetch_prices(tickers, start, end, prefer='auto', cache_dir=None, use_cache=True):
        if prefer == 'yf':
            return us_px[tickers]
        return kr_px

    monkeypatch.setattr("druck.engine.fetch_prices", fake_fetch_prices)
    monkeypatch.setattr("druck.engine.save_report", lambda out_dir, selection, regime_details, cuts: "output/report_test.md")
    monkeypatch.setattr("druck.engine.send_telegram", lambda cfg, msg: None)

    result = run_once(cfg, do_trade=False)
    assert result["provider_warnings"][0]["scope"] == "us"
    assert result["provider_warnings"][0]["summary"] == "provider rate-limit detected (XLV)"


def test_run_once_halts_on_benchmark_underperformance(monkeypatch):
    cfg = {
        "mode": {"enable_kiwoom": True, "dry_run": False},
        "kiwoom": {"account_no": "123"},
        "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
        "selection": {
            "top_n_risk_on": 2,
            "top_n_risk_off": 1,
            "max_weight": 1.0,
            "score_weights": {"momentum": 0.55, "trend": 0.25, "vol_penalty": 0.10, "dd_penalty": 0.10},
            "regime_sleeve_rotation": {"enabled": True, "RISK_ON": {"top_n": 2, "preferred_sleeves": ["factor"], "sleeve_budget": {"factor": 0.7, "core": 0.3}, "score_tilt": {"factor": 0.15}}},
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
            "performance": {
                "enabled": True,
                "min_average_score": -999.0,
                "max_average_momentum": -999.0,
                "recent_total_return": -999.0,
                "benchmark_ticker": "SPY",
                "benchmark_relative_return": -0.001,
            },
        },
    }

    idx = pd.date_range("2024-01-01", periods=320, freq="D")
    px = pd.DataFrame({
        "SPY": pd.Series([100 + i * 0.5 for i in range(320)], index=idx),
        "SHY": pd.Series([100.0] * 320, index=idx),
        "UUP": pd.Series([100 - i * 0.01 for i in range(320)], index=idx),
        "HYG": pd.Series([100 - i * 0.01 for i in range(320)], index=idx),
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
