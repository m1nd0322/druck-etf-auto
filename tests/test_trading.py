import pandas as pd
import pytest

from druck.trading import TradePlanError, build_trade_plan, execute_trade_plan, review_live_trade


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
