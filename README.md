# druck-etf-auto

Druckenmiller-style **macro + momentum** ETF selection & rebalancing engine (KR/US), with **Kiwoom OpenAPI+ (Windows)** execution.

Safe-by-default:
- `dry_run: true` by default (no orders)
- keys/accounts are **never** committed (see `.gitignore`)
- guardrails: market-open only, block near close, VIX spike halt, TR rate-limit, slippage abort, unfilled cancel/reorder

## Features

### Strategy engine
- **Macro regime** (Risk-On / Neutral / Risk-Off): SPY trend(200SMA), USD(UUP), credit(HYG vs IEF), rates(TLT), volatility(^VIX)
- **Universe scoring**: momentum(3/6/12M), trend(50/200 + 200SMA), volatility penalty, drawdown penalty
- **Risk-cuts**: below 200SMA / trailing DD / hard stop → move weight to cash ETF

### Automation
- Weekly rebalance + daily risk checks via APScheduler
- Reports: CSV + Markdown in `output/`

### Kiwoom (Windows only)
- Market orders, intraday-only + block new orders near close
- **3-slice execution**: next slice after previous slice fills (Chejan)
- Unfilled remainder: query → cancel → re-order
- Slippage/dislocation guard: abort if avg fill deviates too much
- Fill logs: SQLite (`trade_log.db`) (ignored)

## Quick start (report-only)

```bash
pip install -r requirements.txt
python run_report.py
```

## Scheduler (auto)

```bash
python run_auto.py
```

## Kiwoom (Windows)

1) Install Kiwoom OpenAPI+  
2) `pip install PyQt5`  
3) Set `config.yaml`:

```yaml
mode:
  enable_kiwoom: true
  dry_run: true   # start with true
kiwoom:
  account_no: "YOUR_ACCOUNT_NO"
```

Run:

```bash
python run_auto.py
```

## Security

Do **not** put real keys in `config.yaml`.  
Use env vars + `config.local.yaml` (ignored).

## Disclaimer

Not investment advice. Use at your own risk.
