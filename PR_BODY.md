## Summary

This PR improves the operational reliability of `druck-etf-auto` before further execution or live-trading expansion.

Key improvements:
- add validated config loading
- harden notifier behavior
- make Kiwoom time checks explicitly KST-aware
- improve shared data fallback visibility
- add unit tests for core indicators, macro regime logic, and portfolio risk cuts

## Changes

### Config and startup safety
- added `druck/config.py`
- introduced shared `load_config()` + `validate_config()`
- fail fast on:
  - missing required keys
  - invalid macro thresholds
  - invalid weight and risk-cut ranges
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

### Tests
Added:
- `tests/test_features.py`
- `tests/test_macro.py`
- `tests/test_portfolio.py`

Coverage includes:
- SMA / pct change / vol / drawdown / momentum helpers
- macro regime classification
- VIX spike detection
- weight allocation normalization
- cash rotation after risk cuts

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
10 passed in 0.28s
```

## Notes

This PR focuses on reliability hardening, not execution expansion.
Live trading flow is still only partially wired and should remain behind explicit safeguards.
