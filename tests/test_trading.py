import pandas as pd
import pytest

from druck.db import init_db, log_operator_ack
from druck.trading import TradePlanError, build_trade_plan, execute_trade_plan, review_live_trade, run_rebalance_cycle


class FakeBroker:
    def __init__(self):
        self.positions = {"SPY": 2}
        self.prices = {"SPY": 100.0, "SHY": 50.0}
        self._db = None

    def get_positions(self):
        return self.positions

    def get_cash(self):
        return 100.0

    def get_portfolio_value(self):
        return 300.0

    def get_last_price(self, ticker):
        return self.prices.get(ticker, 0.0)

    def place_order(self, ticker, qty, side, order_type="MKT"):
        return {"status": "submitted", "qty_executed": qty, "detail": "ok"}


class PartialFillBroker(FakeBroker):
    def place_order(self, ticker, qty, side, order_type="MKT"):
        return {"status": "partial_fill", "qty_executed": max(0, qty - 1), "detail": "partial"}


class FailingBroker(FakeBroker):
    def place_order(self, ticker, qty, side, order_type="MKT"):
        raise RuntimeError("broker failure")


class FundingBroker(FailingBroker):
    def place_order(self, ticker, qty, side, order_type="MKT"):
        raise RuntimeError("insufficient cash for order")


def test_build_trade_plan_creates_sell_then_buy_orders():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FakeBroker()
    target = pd.Series({"SPY": 0.0, "SHY": 1.0})
    plan = build_trade_plan(cfg, broker, target)
    assert [o.side for o in plan.orders] == ["SELL", "BUY"]
    assert plan.orders[0].ticker == "SPY"
    assert plan.orders[1].ticker == "SHY"


def test_build_trade_plan_warns_when_buys_exceed_cash_and_sales():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FakeBroker()
    broker.positions = {}
    target = pd.Series({"SHY": 1.0})
    plan = build_trade_plan(cfg, broker, target)
    assert plan.warnings


def test_build_trade_plan_raises_when_portfolio_value_non_positive():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FakeBroker()
    broker.get_cash = lambda: 0.0
    broker.get_portfolio_value = lambda: 0.0
    with pytest.raises(TradePlanError):
        build_trade_plan(cfg, broker, pd.Series({"SHY": 1.0}))


def test_review_live_trade_blocks_until_live_prereqs_are_met():
    cfg = {
        "mode": {"enable_kiwoom": False, "dry_run": True},
        "kiwoom": {"account_no": ""},
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
    }
    broker = FakeBroker()
    plan = build_trade_plan(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}))
    review = review_live_trade(cfg, broker, plan)
    assert review.approved is False
    assert any(check["ok"] is False for check in review.checks)


def test_execute_trade_plan_stops_after_partial_fill():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = PartialFillBroker()
    plan = build_trade_plan(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}))
    executed = execute_trade_plan(broker, plan)
    assert executed[0]["status"] == "partial_fill"
    assert len(executed) == 1


def test_execute_trade_plan_raises_on_broker_error():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FailingBroker()
    plan = build_trade_plan(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}))
    with pytest.raises(TradePlanError):
        execute_trade_plan(broker, plan)


def test_execute_trade_plan_classifies_funding_error():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FundingBroker()
    plan = build_trade_plan(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}))
    with pytest.raises(TradePlanError) as exc:
        execute_trade_plan(broker, plan)
    assert "insufficient cash" in str(exc.value)


def test_run_rebalance_cycle_marks_replan_on_partial_fill():
    cfg = {
        "mode": {"enable_kiwoom": True, "dry_run": False},
        "kiwoom": {"account_no": "123"},
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
    }
    broker = PartialFillBroker()
    result = run_rebalance_cycle(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}), max_replans=0)
    assert result.needs_replan is True
    assert result.detail == "partial_fill_detected"
    assert result.operator_ack_required is True
    assert result.operator_ack_state is None


def test_run_rebalance_cycle_includes_latest_operator_ack_state(tmp_path):
    cfg = {
        "mode": {"enable_kiwoom": True, "dry_run": False},
        "kiwoom": {"account_no": "123"},
        "rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True},
    }
    broker = PartialFillBroker()
    broker._db = init_db(str(tmp_path / "trade_log.db"))
    log_operator_ack(broker._db, ack_type="partial_fill_replan", status="acknowledged", note="checked")

    result = run_rebalance_cycle(cfg, broker, pd.Series({"SPY": 0.0, "SHY": 1.0}), max_replans=0)
    assert result.operator_ack_required is True
    assert result.operator_ack_state is not None
    assert result.operator_ack_state["status"] == "acknowledged"
