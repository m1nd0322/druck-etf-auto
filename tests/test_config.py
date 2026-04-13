import pytest

from druck.config import ConfigError, load_config, validate_config


VALID_CFG = {
    "mode": {"dry_run": True, "enable_kiwoom": False},
    "data": {"lookback_years": 3, "price_provider": "auto", "cache_csv": True, "cache_dir": ".cache"},
    "universe": {
        "kr": {
            "auto_generate": True,
            "include_leveraged": False,
            "include_inverse": False,
            "whitelist_tickers": [],
            "blacklist_tickers": [],
        },
        "us": {"tickers": ["SPY", "QQQ", "SHY", "UUP", "IEF", "HYG", "TLT", "^VIX"]},
    },
    "macro_filter": {
        "enabled": True,
        "thresholds": {"risk_on_score_min": 0.55, "risk_off_score_max": 0.45},
        "components": {
            "spy_trend_weight": 0.30,
            "usd_mom_weight": 0.15,
            "credit_weight": 0.20,
            "vix_weight": 0.20,
            "rates_weight": 0.15,
        },
    },
    "selection": {
        "top_n_risk_on": 8,
        "top_n_risk_off": 4,
        "score_weights": {"momentum": 0.55, "trend": 0.25, "vol_penalty": 0.10, "dd_penalty": 0.10},
        "max_weight": 0.25,
    },
    "risk_cut": {
        "enabled": True,
        "rules": {"below_200sma_cut": True, "trailing_dd_cut": -0.12, "hard_stop_cut": -0.18},
        "action": {"cut_to_cash": True, "cash_us": "SHY", "cash_kr": "130730.KS"},
    },
    "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True, "commission_bps": 1.5},
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
                {"name": "return_shock_and_vol_up", "severity": "high", "tags": ["stress", "drawdown", "volatility"], "return_shock": -0.05, "vol_multiplier": 1.5, "benchmark_shock": 0.0},
                {"name": "benchmark_gap_down", "severity": "high", "tags": ["stress", "benchmark", "gap"], "return_shock": -0.02, "vol_multiplier": 1.2, "benchmark_shock": -0.04},
            ],
        },
        "walkforward": {"enabled": True, "train_days": 252, "test_days": 42, "step_days": 42},
    },
    "schedule": {
        "timezone": "Asia/Seoul",
        "report_weekly": {"day_of_week": "sat", "hour": 9, "minute": 5},
        "risk_check_daily": {"hour": 16, "minute": 5},
    },
    "notifier": {"telegram": {"enabled": False, "bot_token_env": "TELEGRAM_BOT_TOKEN", "chat_id_env": "TELEGRAM_CHAT_ID"}},
    "kiwoom": {"account_no": "", "market_order": True, "split_n": 3, "slippage_limit_bps": 30},
}


def test_validate_config_accepts_valid_config():
    cfg = validate_config(VALID_CFG)
    assert cfg["data"]["lookback_years"] == 3


def test_validate_config_rejects_invalid_threshold_order():
    cfg = VALID_CFG | {"macro_filter": VALID_CFG["macro_filter"] | {"thresholds": {"risk_on_score_min": 0.4, "risk_off_score_max": 0.5}}}
    with pytest.raises(ConfigError):
        validate_config(cfg)


def test_validate_config_rejects_invalid_schedule_hour():
    cfg = VALID_CFG | {"schedule": VALID_CFG["schedule"] | {"risk_check_daily": {"hour": 25, "minute": 5}}}
    with pytest.raises(ConfigError):
        validate_config(cfg)


def test_load_config_reads_yaml(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "mode:\n  dry_run: true\n  enable_kiwoom: false\n"
        "data:\n  lookback_years: 3\n  price_provider: auto\n  cache_csv: true\n  cache_dir: .cache\n"
        "universe:\n  kr:\n    auto_generate: true\n    include_leveraged: false\n    include_inverse: false\n    whitelist_tickers: []\n    blacklist_tickers: []\n  us:\n    tickers: [SPY, SHY, UUP, IEF, HYG, TLT, ^VIX]\n"
        "macro_filter:\n  enabled: true\n  thresholds:\n    risk_on_score_min: 0.55\n    risk_off_score_max: 0.45\n  components:\n    spy_trend_weight: 0.3\n    usd_mom_weight: 0.15\n    credit_weight: 0.2\n    vix_weight: 0.2\n    rates_weight: 0.15\n"
        "selection:\n  top_n_risk_on: 8\n  top_n_risk_off: 4\n  score_weights:\n    momentum: 0.55\n    trend: 0.25\n    vol_penalty: 0.1\n    dd_penalty: 0.1\n  max_weight: 0.25\n"
        "risk_cut:\n  enabled: true\n  rules:\n    below_200sma_cut: true\n    trailing_dd_cut: -0.12\n    hard_stop_cut: -0.18\n  action:\n    cut_to_cash: true\n    cash_us: SHY\n    cash_kr: 130730.KS\n"
        "rebalance:\n  min_trade_weight_diff: 0.01\n  round_shares: true\n  commission_bps: 1.5\n"
        "backtest:\n  rebalance_frequency: M\n  transaction_cost_bps: 1.5\n  slippage_bps: 3.0\n  market_impact_bps_per_turnover: 5.0\n  liquidity_vol_multiplier_bps: 2.0\n  starting_capital: 1.0\n  benchmark_ticker: SPY\n  min_history_days: 252\n  strict_point_in_time: true\n  drop_incomplete_assets: true\n  enforce_delist_exit: true\n  universe_timeline_path: ''\n  volume_data_path: ''\n  adv_window_days: 20\n  max_participation_rate: 0.1\n  capacity_safety_factor: 0.25\n  scenarios:\n    enabled: true\n    stress_return_shock: -0.05\n    vol_multiplier: 1.5\n    presets:\n      - name: return_shock_and_vol_up\n        severity: high\n        tags: [stress, drawdown, volatility]\n        return_shock: -0.05\n        vol_multiplier: 1.5\n        benchmark_shock: 0.0\n      - name: benchmark_gap_down\n        severity: high\n        tags: [stress, benchmark, gap]\n        return_shock: -0.02\n        vol_multiplier: 1.2\n        benchmark_shock: -0.04\n  walkforward:\n    enabled: true\n    train_days: 252\n    test_days: 42\n    step_days: 42\n"
        "schedule:\n  timezone: Asia/Seoul\n  report_weekly:\n    day_of_week: sat\n    hour: 9\n    minute: 5\n  risk_check_daily:\n    hour: 16\n    minute: 5\n"
        "notifier:\n  telegram:\n    enabled: false\n    bot_token_env: TELEGRAM_BOT_TOKEN\n    chat_id_env: TELEGRAM_CHAT_ID\n"
        "kiwoom:\n  account_no: ''\n  market_order: true\n  split_n: 3\n  slippage_limit_bps: 30\n",
        encoding="utf-8",
    )
    cfg = load_config(config_path, tmp_path / "config.local.yaml")
    assert cfg["selection"]["max_weight"] == 0.25
