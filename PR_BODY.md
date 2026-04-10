## Summary

This PR improves the operational reliability of `druck-etf-auto` before further execution or live-trading expansion.

Key improvements:
- add stronger validated config loading
- harden notifier behavior and runtime guards
- make Kiwoom time checks explicitly KST-aware
- improve shared data fallback visibility
- expand CI to run automated test coverage on review branches
- add unit tests for indicators, config validation, macro regime logic, portfolio risk cuts, and backtest scaffolding
- add a conservative backtest scaffold for future historical simulation work

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
- `druck/engine.py` now guards `do_trade=True` without a broker

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
Added:
- `tests/test_features.py`
- `tests/test_macro.py`
- `tests/test_portfolio.py`
- `tests/test_config.py`
- `tests/test_backtest.py`

Coverage includes:
- SMA / pct change / vol / drawdown / momentum helpers
- macro regime classification
- VIX spike detection
- config validation
- weight allocation normalization
- cash rotation after risk cuts
- backtest scaffold return shape

### Backtest scaffold
Added:
- `druck/backtest.py`
- `run_backtest.py`

Current scope:
- wraps the live selection pipeline safely
- returns a minimal equity curve, rebalance log, and summary structure
- intentionally acts as scaffolding for future historical rebalancing, transaction costs, and analytics

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
14 passed in 0.35s
```

## Notes

This PR focuses on reliability hardening and developer workflow improvement, not full execution expansion.
The backtest module introduced here is deliberately minimal scaffolding and should not be interpreted as a full historical simulation engine yet.
Live trading flow is still only partially wired and should remain behind explicit safeguards.
