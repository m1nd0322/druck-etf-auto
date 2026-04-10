import pandas as pd

from druck.backtest import BacktestResult, run_backtest


def test_run_backtest_returns_expected_shape(monkeypatch):
    def fake_run_once(cfg, do_trade=False):
        return {
            "target_weights": pd.Series({"SPY": 0.6, "SHY": 0.4}),
            "report_path": "output/report_test.md",
        }

    monkeypatch.setattr("druck.backtest.run_once", fake_run_once)
    result = run_backtest({})
    assert isinstance(result, BacktestResult)
    assert list(result.rebalance_log["ticker"]) == ["SPY", "SHY"]
    assert result.summary["positions"] == 2
    assert result.summary["report_path"] == "output/report_test.md"
