from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


DEFAULT_CONFIG_PATH = "config.yaml"
LOCAL_CONFIG_PATH = "config.local.yaml"


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


def _require_number(mapping: dict[str, Any], key: str, ctx: str) -> float:
    value = _require(mapping, key, ctx)
    if not isinstance(value, (int, float)):
        raise ConfigError(f"Config value must be numeric: {ctx}.{key}")
    return float(value)


def validate_config(cfg: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(cfg, dict):
        raise ConfigError("Config root must be a mapping")

    data = _require(cfg, "data", "config")
    selection = _require(cfg, "selection", "config")
    macro_filter = _require(cfg, "macro_filter", "config")
    risk_cut = _require(cfg, "risk_cut", "config")
    schedule = _require(cfg, "schedule", "config")
    kiwoom = _require(cfg, "kiwoom", "config")

    lookback_years = _require_number(data, "lookback_years", "config.data")
    if lookback_years < 1:
        raise ConfigError("config.data.lookback_years must be >= 1")

    top_n_risk_on = _require_number(selection, "top_n_risk_on", "config.selection")
    top_n_risk_off = _require_number(selection, "top_n_risk_off", "config.selection")
    max_weight = _require_number(selection, "max_weight", "config.selection")
    if top_n_risk_on < 1 or top_n_risk_off < 1:
        raise ConfigError("top_n_risk_on and top_n_risk_off must be >= 1")
    if not 0 < max_weight <= 1:
        raise ConfigError("config.selection.max_weight must be between 0 and 1")

    thresholds = _require(macro_filter, "thresholds", "config.macro_filter")
    risk_on_score_min = _require_number(thresholds, "risk_on_score_min", "config.macro_filter.thresholds")
    risk_off_score_max = _require_number(thresholds, "risk_off_score_max", "config.macro_filter.thresholds")
    if not 0 <= risk_off_score_max < risk_on_score_min <= 1:
        raise ConfigError("macro thresholds must satisfy 0 <= risk_off_score_max < risk_on_score_min <= 1")

    score_weights = _require(selection, "score_weights", "config.selection")
    for key in ["momentum", "trend", "vol_penalty", "dd_penalty"]:
        _require_number(score_weights, key, "config.selection.score_weights")

    rules = _require(risk_cut, "rules", "config.risk_cut")
    trailing_dd_cut = _require_number(rules, "trailing_dd_cut", "config.risk_cut.rules")
    hard_stop_cut = _require_number(rules, "hard_stop_cut", "config.risk_cut.rules")
    if trailing_dd_cut >= 0 or hard_stop_cut >= 0:
        raise ConfigError("risk cut thresholds must be negative percentages")
    if hard_stop_cut > trailing_dd_cut:
        raise ConfigError("hard_stop_cut should be equal to or more negative than trailing_dd_cut")

    action = _require(risk_cut, "action", "config.risk_cut")
    _require(action, "cash_us", "config.risk_cut.action")
    _require(action, "cash_kr", "config.risk_cut.action")

    _require(schedule, "timezone", "config.schedule")
    report_weekly = _require(schedule, "report_weekly", "config.schedule")
    risk_check_daily = _require(schedule, "risk_check_daily", "config.schedule")
    for key in ["hour", "minute"]:
        _require_number(report_weekly, key, "config.schedule.report_weekly")
        _require_number(risk_check_daily, key, "config.schedule.risk_check_daily")
    _require(report_weekly, "day_of_week", "config.schedule.report_weekly")

    slippage_limit_bps = _require_number(kiwoom, "slippage_limit_bps", "config.kiwoom")
    split_n = _require_number(kiwoom, "split_n", "config.kiwoom")
    if slippage_limit_bps <= 0:
        raise ConfigError("config.kiwoom.slippage_limit_bps must be > 0")
    if split_n < 1:
        raise ConfigError("config.kiwoom.split_n must be >= 1")

    return cfg


def load_config(path: str | Path = DEFAULT_CONFIG_PATH, local_path: str | Path = LOCAL_CONFIG_PATH) -> dict[str, Any]:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise ConfigError(f"Config file not found: {cfg_path}")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    local_cfg_path = Path(local_path)
    if local_cfg_path.exists():
        local_cfg = yaml.safe_load(local_cfg_path.read_text(encoding="utf-8")) or {}
        _deep_merge(cfg, local_cfg)

    return validate_config(cfg)
