import pandas as pd

from druck.trading import build_trade_plan


class FakeBroker:
    def __init__(self):
        self.positions = {"SPY": 2}
        self.prices = {"SPY": 100.0, "SHY": 50.0}

    def get_positions(self):
        return self.positions

    def get_cash(self):
        return 100.0

    def get_portfolio_value(self):
        return 300.0

    def get_last_price(self, ticker):
        return self.prices.get(ticker, 0.0)


def test_build_trade_plan_creates_sell_then_buy_orders():
    cfg = {"rebalance": {"min_trade_weight_diff": 0.01, "round_shares": True}}
    broker = FakeBroker()
    target = pd.Series({"SPY": 0.0, "SHY": 1.0})
    plan = build_trade_plan(cfg, broker, target)
    assert [o.side for o in plan.orders] == ["SELL", "BUY"]
    assert plan.orders[0].ticker == "SPY"
    assert plan.orders[1].ticker == "SHY"
