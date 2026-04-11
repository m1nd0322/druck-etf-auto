from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .broker_base import Broker
from .db import log_trade_audit


class TradePlanError(RuntimeError):
    pass


@dataclass
class OrderIntent:
    ticker: str
    side: str
    qty: int
    target_weight: float
    last_price: float
    est_notional: float


@dataclass
class OrderExecutionResult:
    ticker: str
    side: str
    qty_requested: int
    qty_executed: int
    est_notional: float
    status: str
    detail: str = ""


@dataclass
class TradePlan:
    orders: list[OrderIntent]
    skipped: list[dict[str, Any]]
    portfolio_value: float
    cash_available: float
    warnings: list[str]


@dataclass
class LiveTradeReview:
    plan: TradePlan
    checks: list[dict[str, Any]]
    approved: bool


@dataclass
class RebalanceCycleResult:
    executions: list[dict[str, Any]]
    needs_replan: bool
    detail: str


def _normalize_positions(positions: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for ticker, qty in positions.items():
        if not ticker:
            continue
        out[str(ticker).strip()] = int(qty)
    return out


def _broker_supports_live_trading(broker: Broker) -> bool:
    return hasattr(broker, "place_order") and hasattr(broker, "get_last_price")


def _classify_broker_error(exc: Exception) -> str:
    text = str(exc).lower()
    if "market closed" in text:
        return "market_closed"
    if "slippage" in text:
        return "slippage"
    if "login" in text or "connect" in text:
        return "connection"
    if "insufficient" in text or "cash" in text:
        return "funding"
    return "broker_error"


def build_trade_plan(cfg: dict, broker: Broker, target_weights: pd.Series, min_trade_weight_diff: float | None = None) -> TradePlan:
    positions = _normalize_positions(broker.get_positions())
    cash_available = float(broker.get_cash())
    portfolio_value = float(getattr(broker, "get_portfolio_value", lambda: cash_available)())
    if portfolio_value <= 0:
        portfolio_value = cash_available
    if portfolio_value <= 0:
        raise TradePlanError("Portfolio value must be positive to build a trade plan")

    threshold = float(min_trade_weight_diff if min_trade_weight_diff is not None else cfg["rebalance"]["min_trade_weight_diff"])
    round_shares = bool(cfg["rebalance"].get("round_shares", True))

    orders: list[OrderIntent] = []
    skipped: list[dict[str, Any]] = []
    warnings: list[str] = []
    tracked_tickers = set(target_weights.index) | set(positions.keys())

    total_buy_notional = 0.0
    total_sell_notional = 0.0

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
        est_notional = abs(qty) * last_price
        if side == "BUY":
            total_buy_notional += est_notional
        else:
            total_sell_notional += est_notional
        orders.append(
            OrderIntent(
                ticker=ticker,
                side=side,
                qty=abs(qty),
                target_weight=target_weight,
                last_price=last_price,
                est_notional=est_notional,
            )
        )

    if total_buy_notional > (cash_available + total_sell_notional) * 1.02:
        warnings.append("Estimated buy notional exceeds available cash plus expected sell proceeds")
    if not orders:
        warnings.append("No executable orders generated from current portfolio state")

    orders.sort(key=lambda x: (x.side != "SELL", -x.est_notional))
    return TradePlan(
        orders=orders,
        skipped=skipped,
        portfolio_value=portfolio_value,
        cash_available=cash_available,
        warnings=warnings,
    )


def review_live_trade(cfg: dict, broker: Broker, plan: TradePlan) -> LiveTradeReview:
    checks: list[dict[str, Any]] = []
    approved = True

    enable_kiwoom = bool(cfg.get("mode", {}).get("enable_kiwoom", False))
    dry_run = bool(cfg.get("mode", {}).get("dry_run", True))
    account_no = str(cfg.get("kiwoom", {}).get("account_no", "")).strip()

    checks.append({"name": "broker_support", "ok": _broker_supports_live_trading(broker), "detail": "Broker exposes required live-trading methods"})
    checks.append({"name": "kiwoom_enabled", "ok": enable_kiwoom, "detail": "mode.enable_kiwoom must be true for live trading"})
    checks.append({"name": "dry_run_disabled", "ok": not dry_run, "detail": "mode.dry_run must be false before real order execution"})
    checks.append({"name": "account_configured", "ok": bool(account_no), "detail": "kiwoom.account_no must be configured"})
    checks.append({"name": "no_tradeplan_warnings", "ok": len(plan.warnings) == 0, "detail": "; ".join(plan.warnings) if plan.warnings else "No warnings"})

    for check in checks:
        if not check["ok"]:
            approved = False

    return LiveTradeReview(plan=plan, checks=checks, approved=approved)


def execute_trade_plan(broker: Broker, plan: TradePlan, order_type: str = "MKT") -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    audit_conn = getattr(broker, "_db", None)
    for order in plan.orders:
        if audit_conn is not None:
            log_trade_audit(audit_conn, "order_intent", ticker=order.ticker, side=order.side, qty=order.qty, status="planned", detail=f"est_notional={order.est_notional:.2f}")
        try:
            result = broker.place_order(order.ticker, order.qty, order.side, order_type=order_type)
            executed_qty = order.qty
            status = "submitted"
            detail = ""
            if isinstance(result, dict):
                executed_qty = int(result.get("qty_executed", order.qty))
                status = str(result.get("status", status))
                detail = str(result.get("detail", ""))
            if executed_qty < order.qty:
                status = "partial_fill"
                detail = detail or "Executed quantity lower than requested"
            execution = OrderExecutionResult(
                ticker=order.ticker,
                side=order.side,
                qty_requested=order.qty,
                qty_executed=executed_qty,
                est_notional=order.est_notional,
                status=status,
                detail=detail,
            )
            executed.append(execution.__dict__)
            if audit_conn is not None:
                log_trade_audit(audit_conn, "order_execution", ticker=order.ticker, side=order.side, qty=executed_qty, status=status, detail=detail)
            if status == "partial_fill":
                break
        except Exception as exc:
            category = _classify_broker_error(exc)
            if audit_conn is not None:
                log_trade_audit(audit_conn, "order_execution", ticker=order.ticker, side=order.side, qty=order.qty, status=category, detail=str(exc))
            raise TradePlanError(f"Order execution failed for {order.ticker}: {exc}") from exc
    return executed


def run_rebalance_cycle(cfg: dict, broker: Broker, target_weights: pd.Series, max_replans: int = 1) -> RebalanceCycleResult:
    all_executions: list[dict[str, Any]] = []
    needs_replan = False
    detail = "completed"

    for attempt in range(max_replans + 1):
        plan = build_trade_plan(cfg, broker, target_weights)
        review = review_live_trade(cfg, broker, plan)
        if not review.approved:
            raise TradePlanError(f"Live trade review failed: {review.checks}")

        executions = execute_trade_plan(broker, plan)
        all_executions.extend(executions)
        if not executions:
            break

        if any(e["status"] == "partial_fill" for e in executions):
            needs_replan = True
            detail = "partial_fill_detected"
            if attempt >= max_replans:
                break
            continue
        break

    return RebalanceCycleResult(executions=all_executions, needs_replan=needs_replan, detail=detail)
