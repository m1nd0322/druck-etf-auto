from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .broker_base import Broker


@dataclass
class OrderIntent:
    ticker: str
    side: str
    qty: int
    target_weight: float
    last_price: float
    est_notional: float


@dataclass
class TradePlan:
    orders: list[OrderIntent]
    skipped: list[dict[str, Any]]
    portfolio_value: float
    cash_available: float


def _normalize_positions(positions: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for ticker, qty in positions.items():
        if not ticker:
            continue
        out[str(ticker).strip()] = int(qty)
    return out


def build_trade_plan(cfg: dict, broker: Broker, target_weights: pd.Series, min_trade_weight_diff: float | None = None) -> TradePlan:
    positions = _normalize_positions(broker.get_positions())
    cash_available = float(broker.get_cash())
    portfolio_value = float(getattr(broker, "get_portfolio_value", lambda: cash_available)())
    if portfolio_value <= 0:
        portfolio_value = cash_available

    threshold = float(min_trade_weight_diff if min_trade_weight_diff is not None else cfg["rebalance"]["min_trade_weight_diff"])
    round_shares = bool(cfg["rebalance"].get("round_shares", True))

    orders: list[OrderIntent] = []
    skipped: list[dict[str, Any]] = []
    tracked_tickers = set(target_weights.index) | set(positions.keys())

    for ticker in sorted(tracked_tickers):
        target_weight = float(target_weights.get(ticker, 0.0))
        current_qty = int(positions.get(ticker, 0))
        last_price = float(broker.get_last_price(ticker))
        if last_price <= 0:
            skipped.append({"ticker": ticker, "reason": "missing_price", "target_weight": target_weight})
            continue

        current_value = current_qty * last_price
        current_weight = current_value / portfolio_value if portfolio_value > 0 else 0.0
        diff = target_weight - current_weight
        if abs(diff) < threshold:
            skipped.append({"ticker": ticker, "reason": "below_threshold", "target_weight": target_weight, "current_weight": current_weight})
            continue

        desired_value = target_weight * portfolio_value
        delta_value = desired_value - current_value
        qty = delta_value / last_price
        qty = int(round(qty)) if round_shares else int(qty)
        if qty == 0:
            skipped.append({"ticker": ticker, "reason": "rounded_to_zero", "target_weight": target_weight, "current_weight": current_weight})
            continue

        side = "BUY" if qty > 0 else "SELL"
        orders.append(
            OrderIntent(
                ticker=ticker,
                side=side,
                qty=abs(qty),
                target_weight=target_weight,
                last_price=last_price,
                est_notional=abs(qty) * last_price,
            )
        )

    orders.sort(key=lambda x: (x.side != "SELL", -x.est_notional))
    return TradePlan(
        orders=orders,
        skipped=skipped,
        portfolio_value=portfolio_value,
        cash_available=cash_available,
    )


def execute_trade_plan(broker: Broker, plan: TradePlan, order_type: str = "MKT") -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    for order in plan.orders:
        broker.place_order(order.ticker, order.qty, order.side, order_type=order_type)
        executed.append(
            {
                "ticker": order.ticker,
                "side": order.side,
                "qty": order.qty,
                "est_notional": order.est_notional,
            }
        )
    return executed
