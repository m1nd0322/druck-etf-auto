from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


DEFAULT_CONFIG_PATH = "config.yaml"
LOCAL_CONFIG_PATH = "config.local.yaml"


class ConfigSection(dict[str, Any]):
    """Thin typed mapping wrapper for dot-style access in editor tooling."""


class AppConfig(ConfigSection):
    pass


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _require(mapping: dict[str, Any], key: str, ctx: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"Missing required config key: {ctx}.{key}")
    return mapping[key]


def _require_dict(mapping: dict[str, Any], key: str, ctx: str) -> dict[str, Any]:
    value = _require(mapping, key, ctx)
    if not isinstance(value, dict):
        raise ConfigError(f"Config section must be a mapping: {ctx}.{key}")
    return value


def _require_number(mapping: dict[str, Any], key: str, ctx: str) -> float:
    value = _require(mapping, key, ctx)
    if not isinstance(value, (int, float)):
        raise ConfigError(f"Config value must be numeric: {ctx}.{key}")
    return float(value)


def _require_bool(mapping: dict[str, Any], key: str, ctx: str) -> bool:
    value = _require(mapping, key, ctx)
    if not isinstance(value, bool):
        raise ConfigError(f"Config value must be boolean: {ctx}.{key}")
    return value


def _validate_hour_minute(section: dict[str, Any], ctx: str):
    hour = _require_number(section, "hour", ctx)
    minute = _require_number(section, "minute", ctx)
    if not 0 <= hour <= 23:
        raise ConfigError(f"{ctx}.hour must be between 0 and 23")
    if not 0 <= minute <= 59:
        raise ConfigError(f"{ctx}.minute must be between 0 and 59")


def _validate_weights_sum(weights: dict[str, Any], ctx: str):
    total = 0.0
    for value in weights.values():
        if not isinstance(value, (int, float)):
            raise ConfigError(f"All weight values must be numeric in {ctx}")
        total += float(value)
    if total <= 0:
        raise ConfigError(f"Weight sum must be > 0 in {ctx}")


def validate_config(cfg: dict[str, Any]) -> AppConfig:
    if not isinstance(cfg, dict):
        raise ConfigError("Config root must be a mapping")

    mode = _require_dict(cfg, "mode", "config")
    data = _require_dict(cfg, "data", "config")
    universe = _require_dict(cfg, "universe", "config")
    selection = _require_dict(cfg, "selection", "config")
    macro_filter = _require_dict(cfg, "macro_filter", "config")
    risk_cut = _require_dict(cfg, "risk_cut", "config")
    rebalance = _require_dict(cfg, "rebalance", "config")
    backtest = _require_dict(cfg, "backtest", "config")
    schedule = _require_dict(cfg, "schedule", "config")
    notifier = _require_dict(cfg, "notifier", "config")
    kiwoom = _require_dict(cfg, "kiwoom", "config")

    _require_bool(mode, "dry_run", "config.mode")
    _require_bool(mode, "enable_kiwoom", "config.mode")

    lookback_years = _require_number(data, "lookback_years", "config.data")
    if lookback_years < 1:
        raise ConfigError("config.data.lookback_years must be >= 1")
    _require_bool(data, "cache_csv", "config.data")
    _require(data, "cache_dir", "config.data")
    provider = _require(data, "price_provider", "config.data")
    if provider not in {"auto", "yf", "fdr"}:
        raise ConfigError("config.data.price_provider must be one of: auto, yf, fdr")

    kr = _require_dict(universe, "kr", "config.universe")
    us = _require_dict(universe, "us", "config.universe")
    for key in ["auto_generate", "include_leveraged", "include_inverse"]:
        _require_bool(kr, key, "config.universe.kr")
    for key in ["whitelist_tickers", "blacklist_tickers"]:
        if not isinstance(_require(kr, key, "config.universe.kr"), list):
            raise ConfigError(f"config.universe.kr.{key} must be a list")
    if not isinstance(_require(us, "tickers", "config.universe.us"), list):
        raise ConfigError("config.universe.us.tickers must be a list")

    top_n_risk_on = _require_number(selection, "top_n_risk_on", "config.selection")
    top_n_risk_off = _require_number(selection, "top_n_risk_off", "config.selection")
    max_weight = _require_number(selection, "max_weight", "config.selection")
    if top_n_risk_on < 1 or top_n_risk_off < 1:
        raise ConfigError("top_n_risk_on and top_n_risk_off must be >= 1")
    if not 0 < max_weight <= 1:
        raise ConfigError("config.selection.max_weight must be between 0 and 1")

    score_weights = _require_dict(selection, "score_weights", "config.selection")
    for key in ["momentum", "trend", "vol_penalty", "dd_penalty"]:
        _require_number(score_weights, key, "config.selection.score_weights")
    _validate_weights_sum(score_weights, "config.selection.score_weights")

    regime_factor_bias = selection.get("regime_factor_bias", {})
    if regime_factor_bias and not isinstance(regime_factor_bias, dict):
        raise ConfigError("config.selection.regime_factor_bias must be a mapping")

    regime_factor_map = selection.get("regime_factor_map", {})
    if regime_factor_map:
        if not isinstance(regime_factor_map, dict):
            raise ConfigError("config.selection.regime_factor_map must be a mapping")
        enabled = regime_factor_map.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ConfigError("config.selection.regime_factor_map.enabled must be boolean")
        for regime_key in ["RISK_ON", "NEUTRAL", "RISK_OFF"]:
            regime_cfg = regime_factor_map.get(regime_key, {})
            if regime_cfg and not isinstance(regime_cfg, dict):
                raise ConfigError(f"config.selection.regime_factor_map.{regime_key} must be a mapping")
            if not regime_cfg:
                continue
            for list_key in ["overweight", "underweight"]:
                values = regime_cfg.get(list_key, [])
                if values and (not isinstance(values, list) or not all(isinstance(v, str) and v.strip() for v in values)):
                    raise ConfigError(f"config.selection.regime_factor_map.{regime_key}.{list_key} must be a string list")
            for number_key in ["bonus", "penalty", "min_count"]:
                if number_key in regime_cfg and not isinstance(regime_cfg[number_key], (int, float)):
                    raise ConfigError(f"config.selection.regime_factor_map.{regime_key}.{number_key} must be numeric")

    sleeve_budget = selection.get("sleeve_budget", {})
    if sleeve_budget and not isinstance(sleeve_budget, dict):
        raise ConfigError("config.selection.sleeve_budget must be a mapping")

    benchmark_relative_filter = selection.get("benchmark_relative_filter", {})
    if benchmark_relative_filter:
        if not isinstance(benchmark_relative_filter, dict):
            raise ConfigError("config.selection.benchmark_relative_filter must be a mapping")
        enabled = benchmark_relative_filter.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ConfigError("config.selection.benchmark_relative_filter.enabled must be boolean")
        if "min_relative_strength_6m" in benchmark_relative_filter:
            _require_number(benchmark_relative_filter, "min_relative_strength_6m", "config.selection.benchmark_relative_filter")
        if "penalty" in benchmark_relative_filter:
            _require_number(benchmark_relative_filter, "penalty", "config.selection.benchmark_relative_filter")
        if "mode" in benchmark_relative_filter:
            mode = str(benchmark_relative_filter.get("mode", "penalty"))
            if mode not in {"penalty", "exclude"}:
                raise ConfigError("config.selection.benchmark_relative_filter.mode must be one of: penalty, exclude")
        if "apply_to_sleeves" in benchmark_relative_filter:
            sleeves = benchmark_relative_filter.get("apply_to_sleeves")
            if not isinstance(sleeves, list) or not all(isinstance(v, str) and v.strip() for v in sleeves):
                raise ConfigError("config.selection.benchmark_relative_filter.apply_to_sleeves must be a string list")

    regime_rotation = selection.get("regime_sleeve_rotation", {})
    if regime_rotation:
        if not isinstance(regime_rotation, dict):
            raise ConfigError("config.selection.regime_sleeve_rotation must be a mapping")
        enabled = regime_rotation.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ConfigError("config.selection.regime_sleeve_rotation.enabled must be boolean")
        for regime_key in ["RISK_ON", "NEUTRAL", "RISK_OFF"]:
            regime_cfg = regime_rotation.get(regime_key, {})
            if regime_cfg and not isinstance(regime_cfg, dict):
                raise ConfigError(f"config.selection.regime_sleeve_rotation.{regime_key} must be a mapping")
            if not regime_cfg:
                continue
            if "top_n" in regime_cfg:
                top_n = _require_number(regime_cfg, "top_n", f"config.selection.regime_sleeve_rotation.{regime_key}")
                if top_n < 1:
                    raise ConfigError(f"config.selection.regime_sleeve_rotation.{regime_key}.top_n must be >= 1")
            if "preferred_sleeves" in regime_cfg:
                pref = regime_cfg.get("preferred_sleeves")
                if not isinstance(pref, list) or not all(isinstance(v, str) and v.strip() for v in pref):
                    raise ConfigError(f"config.selection.regime_sleeve_rotation.{regime_key}.preferred_sleeves must be a string list")
            for nested_key in ["sleeve_budget", "score_tilt"]:
                nested = regime_cfg.get(nested_key, {})
                if nested and not isinstance(nested, dict):
                    raise ConfigError(f"config.selection.regime_sleeve_rotation.{regime_key}.{nested_key} must be a mapping")
                for nk, nv in (nested or {}).items():
                    if not isinstance(nv, (int, float)):
                        raise ConfigError(f"config.selection.regime_sleeve_rotation.{regime_key}.{nested_key}.{nk} must be numeric")

    _require_bool(macro_filter, "enabled", "config.macro_filter")
    thresholds = _require_dict(macro_filter, "thresholds", "config.macro_filter")
    risk_on_score_min = _require_number(thresholds, "risk_on_score_min", "config.macro_filter.thresholds")
    risk_off_score_max = _require_number(thresholds, "risk_off_score_max", "config.macro_filter.thresholds")
    if not 0 <= risk_off_score_max < risk_on_score_min <= 1:
        raise ConfigError("macro thresholds must satisfy 0 <= risk_off_score_max < risk_on_score_min <= 1")
    components = _require_dict(macro_filter, "components", "config.macro_filter")
    for key in ["spy_trend_weight", "usd_mom_weight", "credit_weight", "vix_weight", "rates_weight"]:
        _require_number(components, key, "config.macro_filter.components")
    _validate_weights_sum(components, "config.macro_filter.components")

    _require_bool(risk_cut, "enabled", "config.risk_cut")
    rules = _require_dict(risk_cut, "rules", "config.risk_cut")
    _require_bool(rules, "below_200sma_cut", "config.risk_cut.rules")
    trailing_dd_cut = _require_number(rules, "trailing_dd_cut", "config.risk_cut.rules")
    hard_stop_cut = _require_number(rules, "hard_stop_cut", "config.risk_cut.rules")
    if trailing_dd_cut >= 0 or hard_stop_cut >= 0:
        raise ConfigError("risk cut thresholds must be negative percentages")
    if hard_stop_cut > trailing_dd_cut:
        raise ConfigError("hard_stop_cut should be equal to or more negative than trailing_dd_cut")

    action = _require_dict(risk_cut, "action", "config.risk_cut")
    _require_bool(action, "cut_to_cash", "config.risk_cut.action")
    _require(action, "cash_us", "config.risk_cut.action")
    _require(action, "cash_kr", "config.risk_cut.action")

    min_trade_weight_diff = _require_number(rebalance, "min_trade_weight_diff", "config.rebalance")
    if not 0 <= min_trade_weight_diff <= 1:
        raise ConfigError("config.rebalance.min_trade_weight_diff must be between 0 and 1")
    _require_bool(rebalance, "round_shares", "config.rebalance")
    commission_bps = _require_number(rebalance, "commission_bps", "config.rebalance")
    if commission_bps < 0:
        raise ConfigError("config.rebalance.commission_bps must be >= 0")

    _require(backtest, "rebalance_frequency", "config.backtest")
    for key in [
        "transaction_cost_bps",
        "slippage_bps",
        "market_impact_bps_per_turnover",
        "liquidity_vol_multiplier_bps",
        "starting_capital",
        "min_history_days",
        "adv_window_days",
        "max_participation_rate",
        "capacity_safety_factor",
    ]:
        _require_number(backtest, key, "config.backtest")
    for key in ["strict_point_in_time", "drop_incomplete_assets", "enforce_delist_exit"]:
        _require_bool(backtest, key, "config.backtest")
    _require(backtest, "benchmark_ticker", "config.backtest")
    _require(backtest, "universe_timeline_path", "config.backtest")
    _require(backtest, "volume_data_path", "config.backtest")
    scenarios = _require_dict(backtest, "scenarios", "config.backtest")
    _require_bool(scenarios, "enabled", "config.backtest.scenarios")
    _require_number(scenarios, "stress_return_shock", "config.backtest.scenarios")
    _require_number(scenarios, "vol_multiplier", "config.backtest.scenarios")
    presets = _require(scenarios, "presets", "config.backtest.scenarios")
    if not isinstance(presets, list) or not presets:
        raise ConfigError("config.backtest.scenarios.presets must be a non-empty list")
    for i, preset in enumerate(presets):
        if not isinstance(preset, dict):
            raise ConfigError(f"config.backtest.scenarios.presets[{i}] must be a mapping")
        _require(preset, "name", f"config.backtest.scenarios.presets[{i}]")
        severity = _require(preset, "severity", f"config.backtest.scenarios.presets[{i}]")
        if severity not in {"low", "medium", "high"}:
            raise ConfigError(f"config.backtest.scenarios.presets[{i}].severity must be one of: low, medium, high")
        tags = _require(preset, "tags", f"config.backtest.scenarios.presets[{i}]")
        if not isinstance(tags, list) or not all(isinstance(tag, str) and tag.strip() for tag in tags):
            raise ConfigError(f"config.backtest.scenarios.presets[{i}].tags must be a non-empty string list")
        _require(preset, "operator_action", f"config.backtest.scenarios.presets[{i}]")
        _require_bool(preset, "review_required", f"config.backtest.scenarios.presets[{i}]")
        _require(preset, "note_template", f"config.backtest.scenarios.presets[{i}]")
        _require_number(preset, "return_shock", f"config.backtest.scenarios.presets[{i}]")
        vol_multiplier = _require_number(preset, "vol_multiplier", f"config.backtest.scenarios.presets[{i}]")
        _require_number(preset, "benchmark_shock", f"config.backtest.scenarios.presets[{i}]")
        if vol_multiplier <= 0:
            raise ConfigError(f"config.backtest.scenarios.presets[{i}].vol_multiplier must be > 0")
    walkforward = _require_dict(backtest, "walkforward", "config.backtest")
    _require_bool(walkforward, "enabled", "config.backtest.walkforward")
    for key in ["train_days", "test_days", "step_days"]:
        value = _require_number(walkforward, key, "config.backtest.walkforward")
        if value < 1:
            raise ConfigError(f"config.backtest.walkforward.{key} must be >= 1")
    if _require_number(backtest, "adv_window_days", "config.backtest") < 1:
        raise ConfigError("config.backtest.adv_window_days must be >= 1")
    max_participation_rate = _require_number(backtest, "max_participation_rate", "config.backtest")
    if not 0 < max_participation_rate <= 1:
        raise ConfigError("config.backtest.max_participation_rate must be between 0 and 1")
    capacity_safety_factor = _require_number(backtest, "capacity_safety_factor", "config.backtest")
    if not 0 < capacity_safety_factor <= 1:
        raise ConfigError("config.backtest.capacity_safety_factor must be between 0 and 1")

    _require(schedule, "timezone", "config.schedule")
    report_weekly = _require_dict(schedule, "report_weekly", "config.schedule")
    risk_check_daily = _require_dict(schedule, "risk_check_daily", "config.schedule")
    _validate_hour_minute(report_weekly, "config.schedule.report_weekly")
    _validate_hour_minute(risk_check_daily, "config.schedule.risk_check_daily")
    _require(report_weekly, "day_of_week", "config.schedule.report_weekly")

    telegram = _require_dict(notifier, "telegram", "config.notifier")
    _require_bool(telegram, "enabled", "config.notifier.telegram")
    _require(telegram, "bot_token_env", "config.notifier.telegram")
    _require(telegram, "chat_id_env", "config.notifier.telegram")

    _require(kiwoom, "account_no", "config.kiwoom")
    _require_bool(kiwoom, "market_order", "config.kiwoom")
    slippage_limit_bps = _require_number(kiwoom, "slippage_limit_bps", "config.kiwoom")
    split_n = _require_number(kiwoom, "split_n", "config.kiwoom")
    if slippage_limit_bps <= 0:
        raise ConfigError("config.kiwoom.slippage_limit_bps must be > 0")
    if split_n < 1:
        raise ConfigError("config.kiwoom.split_n must be >= 1")

    return AppConfig(cfg)


def load_config(path: str | Path = DEFAULT_CONFIG_PATH, local_path: str | Path = LOCAL_CONFIG_PATH) -> AppConfig:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise ConfigError(f"Config file not found: {cfg_path}")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    local_cfg_path = Path(local_path)
    if local_cfg_path.exists():
        local_cfg = yaml.safe_load(local_cfg_path.read_text(encoding="utf-8")) or {}
        _deep_merge(cfg, local_cfg)

    return validate_config(cfg)
