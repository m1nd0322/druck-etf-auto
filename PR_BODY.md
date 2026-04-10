## Summary

This PR improves the operational reliability and operator workflow of `druck-etf-auto` before further execution or live-trading expansion.

Key improvements:
- add stronger validated config loading
- harden notifier behavior and runtime guards
- make Kiwoom time checks explicitly KST-aware
- improve shared data fallback visibility
- expand CI to run automated test coverage on review branches
- add unit tests for indicators, config validation, macro regime logic, portfolio risk cuts, data loading, scheduler behavior, notifier behavior, and backtest scaffolding
- add a conservative backtest scaffold for future historical simulation work
- clarify the live trading path with order-plan generation, review checks, and dashboard visibility
- improve operator docs for install, dry-run workflow, and live transition

## Changes

### Config and startup safety
- added and expanded `druck/config.py`
- introduced shared `load_config()` + `validate_config()`
- fail fast on:
  - missing required keys
  - invalid macro thresholds
  - invalid weight and risk-cut ranges
  - invalid hour/minute schedule values
  - invalid provider and boolean config values
  - invalid Kiwoom execution settings

### Runtime hardening
- `run_report.py` now uses validated config loading
- `druck/scheduler.py` now uses validated config loading
- `druck/web/app.py` now uses the same config path and validation flow
- `druck/notifier.py` now handles Telegram delivery failures safely
- `druck/kiwoom_broker.py` now uses explicit `Asia/Seoul` timezone handling
- `druck/engine.py` now routes `do_trade=True` through a live-trade review before execution

### Data loading
- improved fallback logging when shared data loader import or load fails

### CI
- GitHub Actions now runs on `review/**` branches as well as normal push/PR flows
- CI now executes:
  - syntax checks
  - `pytest -q`
  - import smoke tests
  - Docker build validation

### Tests
Added or expanded:
- `tests/test_features.py`
- `tests/test_macro.py`
- `tests/test_portfolio.py`
- `tests/test_config.py`
- `tests/test_backtest.py`
- `tests/test_data.py`
- `tests/test_scheduler.py`
- `tests/test_notifier.py`
- `tests/test_trading.py`
- `tests/test_engine_trade.py`

Coverage includes:
- SMA / pct change / vol / drawdown / momentum helpers
- macro regime classification
- VIX spike detection
- config validation and failure cases
- data cache loading
- scheduler registration
- notifier behavior
- weight allocation normalization
- cash rotation after risk cuts
- backtest scaffold return shape
- order-plan generation and live-trade review blocking

### Backtest scaffold
Added:
- `druck/backtest.py`
- `run_backtest.py`

Current scope:
- wraps the live selection pipeline safely
- returns a minimal equity curve, rebalance log, and summary structure
- intentionally acts as scaffolding for future historical rebalancing, transaction costs, and analytics

### Live trading path clarification
Added:
- `druck/trading.py`

Current scope:
- generates order intents from target weights versus current holdings
- prioritizes sell orders before buys
- checks estimated buy notional against available cash plus expected sells
- blocks execution unless live-trading prerequisites pass
- surfaces warnings and review checks in the dashboard

### Dashboard and operator workflow
- dashboard now includes live workflow guidance
- dashboard now exposes a backtest trigger
- dashboard now shows order plan preview, warnings, and live review checks
- README now includes install, dry-run, and live transition guidance

## Verification

Executed in local project environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -q
```

Result:

```text
23 passed
```

## Notes

This PR focuses on reliability hardening, operator clarity, and safer execution workflow, not full execution expansion.
The backtest module introduced here is deliberately minimal scaffolding and should not be interpreted as a full historical simulation engine yet.
The live trading path is now more explicit, but still should be used only with deliberate operator review and staged rollout safeguards.
