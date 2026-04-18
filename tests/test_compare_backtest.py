from pathlib import Path

import pandas as pd
import pytest

from druck.backtest import BacktestResult
from druck.compare_backtest import build_baseline_cfg, run_scoring_comparison, summarize_comparison


def test_build_baseline_cfg_disables_new_scoring_wave_and_kr_auto_generate():
    cfg = {
        "selection": {
            "score_weights": {"capacity_awareness": 0.08, "residual_strength": 0.10},
            "correlation_diversification": {"enabled": True},
            "residual_strength_anchors": {"enabled": True},
        },
        "universe": {"kr": {"auto_generate": True, "tickers": ["069500.KS"], "whitelist_tickers": ["069500.KS"], "blacklist_tickers": [], "include_leveraged": True, "include_inverse": True}},
    }
    baseline = build_baseline_cfg(cfg, us_only=True)
    assert baseline["selection"]["score_weights"]["capacity_awareness"] == 0.0
    assert baseline["selection"]["score_weights"]["residual_strength"] == 0.0
    assert baseline["selection"]["correlation_diversification"]["enabled"] is False
    assert baseline["selection"]["residual_strength_anchors"]["enabled"] is False
    assert baseline["universe"]["kr"]["auto_generate"] is False
    assert baseline["universe"]["kr"]["tickers"] == []


def test_summarize_comparison_returns_numeric_deltas():
    result_a = BacktestResult(
        equity_curve=pd.Series([1.0]),
        rebalance_log=pd.DataFrame(),
        summary={"total_return": 0.2, "sharpe": 0.8},
        daily_returns=pd.Series([0.0]),
        analytics={"selection_score_comparison": {"avg_capacity_score": 0.9, "avg_diversification_penalty": 0.1, "latest_alpha_top_picks": ["SPY"]}},
    )
    result_b = BacktestResult(
        equity_curve=pd.Series([1.0]),
        rebalance_log=pd.DataFrame(),
        summary={"total_return": 0.1, "sharpe": 0.6},
        daily_returns=pd.Series([0.0]),
        analytics={"selection_score_comparison": {"avg_capacity_score": 0.7, "avg_diversification_penalty": 0.0, "latest_alpha_top_picks": ["QQQ"]}},
    )
    payload = summarize_comparison(result_a, result_b)
    assert payload["summary_delta"]["total_return"] == pytest.approx(0.1)
    assert payload["selection_delta"]["avg_capacity_score"] == pytest.approx(0.2)
    assert payload["new_last_alpha_picks"] == ["SPY"]
    assert payload["base_last_alpha_picks"] == ["QQQ"]


def test_run_scoring_comparison_writes_outputs(monkeypatch, tmp_path):
    dummy = BacktestResult(
        equity_curve=pd.Series([1.0, 1.1]),
        rebalance_log=pd.DataFrame([{"date": "2024-01-31", "turnover": 0.1}]),
        summary={"total_return": 0.1, "sharpe": 0.5},
        daily_returns=pd.Series([0.0, 0.01]),
        analytics={"selection_score_comparison": {"avg_capacity_score": 0.8, "latest_alpha_top_picks": ["SPY"]}},
    )
    baseline = BacktestResult(
        equity_curve=pd.Series([1.0, 1.05]),
        rebalance_log=pd.DataFrame([{"date": "2024-01-31", "turnover": 0.2}]),
        summary={"total_return": 0.05, "sharpe": 0.3},
        daily_returns=pd.Series([0.0, 0.005]),
        analytics={"selection_score_comparison": {"avg_capacity_score": 0.6, "latest_alpha_top_picks": ["QQQ"]}},
    )
    calls = []

    def fake_run_backtest(cfg):
        calls.append(cfg)
        return dummy if len(calls) == 1 else baseline

    monkeypatch.setattr("druck.compare_backtest.run_backtest", fake_run_backtest)

    cfg = {
        "selection": {
            "score_weights": {"capacity_awareness": 0.08, "residual_strength": 0.1},
            "correlation_diversification": {"enabled": True},
            "residual_strength_anchors": {"enabled": True},
        },
        "universe": {"kr": {"auto_generate": True, "tickers": ["069500.KS"], "whitelist_tickers": [], "blacklist_tickers": [], "include_leveraged": False, "include_inverse": False}},
    }
    result = run_scoring_comparison(cfg, outdir=tmp_path / "comparative")
    output_path = Path(result["output_path"])
    assert output_path.exists()
    assert (tmp_path / "comparative" / "comparison_notes.txt").exists()
    assert len(calls) == 2
