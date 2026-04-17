# druck-etf-auto

[![CI](https://github.com/m1nd0322/druck-etf-auto/actions/workflows/ci.yml/badge.svg)](https://github.com/m1nd0322/druck-etf-auto/actions/workflows/ci.yml)

Macro-regime and momentum ETF allocation engine for KR/US universes, hardened for safer personal operation.

> Korean version: see [README.ko.md](README.ko.md)

## What this project does

`druck-etf-auto` is an automated portfolio workflow that:
- reads market data
- classifies the market regime (`RISK_ON`, `NEUTRAL`, `RISK_OFF`)
- scores ETFs using momentum, trend, volatility, and drawdown
- builds target portfolio weights
- applies risk-cut rules
- generates a report
- optionally prepares or executes trade plans

This repository is designed to be **safe by default**.
Live trading is not the default mode.

## Who this is for

This README is written for someone using the system for the first time.
If you want to understand the project quickly, follow this order:

1. Install the environment
2. Run a report in dry-run mode
3. Open the dashboard
4. Run a backtest snapshot
5. Review safety signals and logs
6. Only then consider live broker integration

## Safety model

The system separates **system errors** from **strategy danger signals**.

### System errors
Examples:
- data fetch failure
- notifier failure
- transient runtime error
- scheduler job error

Expected behavior:
- the system should report the error
- the system should not unnecessarily kill the whole operating loop

### Strategy danger signals
Examples:
- very weak macro regime
- too many negative-momentum selections
- excessive shift to cash
- performance-degradation signals
- benchmark-relative weakness

Expected behavior:
- the system can keep running
- trading can be halted for safety
- the operator reviews the dashboard, runtime events, and logs

## Main features

- validated config loading (`druck/config.py`)
- dry-run-first workflow
- dashboard with report, order preview, audit, runtime events, and halt visibility
- trade audit logging
- operator acknowledgement flow for replan events
- bounded replan loop after partial fills
- runtime guard for reporting errors without collapsing the full loop
- strategy halt rules for dangerous trade conditions
- backtest scaffold for future historical expansion
- CI and test coverage for core logic and operator workflow

## Repository structure

- `druck/engine.py` - end-to-end run pipeline
- `druck/trading.py` - trade plan generation, review, execution, replan loop
- `druck/runtime.py` - runtime guard and runtime event reporting
- `druck/db.py` - fills, trade audit, operator acknowledgement, runtime event persistence
- `druck/web/app.py` - dashboard and APIs
- `druck/backtest.py` - current backtest scaffold
- `run_report.py` - run a report once
- `run_backtest.py` - run a backtest snapshot
- `run_auto.py` - scheduler entry point
- `run_web.py` - web dashboard entry point

## 1. Installation

### Requirements
- Python 3.11+ recommended
- Linux/macOS for report, dashboard, testing, and backtest scaffold
- Windows required for Kiwoom live broker path

### Install steps

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 2. First run, safest path

The safest first experience is **report only**.

### Check config
Main config file:
- `config.yaml`

Default safety values already include:
- `mode.dry_run: true`
- `mode.enable_kiwoom: false`

That means no real live order submission by default.

### Run one report

```bash
. .venv/bin/activate
python run_report.py
```

Expected outputs:
- `output/report_YYYYMMDD_HHMMSS.md`
- `output/selection_YYYYMMDD_HHMMSS.csv`

## 3. Open the dashboard

```bash
. .venv/bin/activate
python run_web.py
```

Then open the local dashboard in your browser.

What to look at first:
- latest regime state
- risk score
- selected ETFs
- order plan preview
- warnings
- runtime events
- strategy halt status
- recent trade audit events
- recent operator acknowledgements

## 4. Run a backtest snapshot

```bash
. .venv/bin/activate
python run_backtest.py
```

Important note:
- the backtest engine is now materially stronger than the original scaffold
- it supports periodic rebalancing, turnover, transaction cost impact, slippage and market-impact cost modeling, benchmark comparison, point-in-time defensive handling, historical universe timeline hooks, volume-data hooks for ADV-style liquidity estimates, walk-forward evaluation, scenario summaries, and core analytics
- it is still not a full multi-asset institutional research platform with corporate actions research controls, survivorship-bias-free vendor history, and exchange-microstructure-perfect execution modeling

## 5. Understand key safety controls

### Dry run
- `mode.dry_run: true`
- safest mode for first-time use
- generates trade intent and review information without real order submission

### Live trade review gate
Before live execution, the system checks:
- broker support
- Kiwoom enabled state
- dry-run disabled state
- account configuration
- trade-plan warnings

### Partial fill handling
If a partial fill happens:
- the system marks the cycle as replan-required
- it does not treat the cycle as clean completion
- the dashboard shows acknowledgement requirements
- the operator can record acknowledgement and note

### Runtime events
Runtime events capture operational issues such as:
- `system_error`
- `strategy_halt`

These events are visible in the dashboard and API.
They can also be resolved by the operator with a note.

### Strategy halt
Trading may stop when signals suggest the strategy is going in the wrong direction.
Current halt families include:
- macro risk weakness
- too many negative momentum assets
- excessive cash concentration after cuts
- clustered risk cuts
- average score degradation
- average momentum degradation
- recent return weakness proxy
- benchmark-relative weakness

## 6. Configuration guide for beginners

Important config sections:
- `mode` - dry run and Kiwoom enable flag
- `data` - lookback, provider, cache settings
- `macro_filter` - regime thresholds and components
- `selection` - ETF scoring and concentration
- `risk_cut` - defensive risk controls
- `rebalance` - minimum trade thresholds
- `backtest` - rebalance cadence, transaction costs, historical universe timeline path, volume/ADV hooks, scenarios, and walk-forward settings
- `strategy_halt` - trade-stop rules when signals degrade
- `schedule` - report and risk-check timing
- `notifier` - Telegram notifications
- `kiwoom` - broker execution settings

### Example beginner-safe mode

```yaml
mode:
  dry_run: true
  enable_kiwoom: false
```

Stay here until you understand the generated reports and dashboard behavior.

### Backtest research extensions

Example backtest section:

```yaml
backtest:
  universe_timeline_path: "data/universe_timeline.csv"
  volume_data_path: "data/volume.csv"
  adv_window_days: 20
  max_participation_rate: 0.10
  capacity_safety_factor: 0.25
  scenarios:
    enabled: true
    stress_return_shock: -0.05
    vol_multiplier: 1.5
    presets:
      - name: return_shock_and_vol_up
        severity: high
        tags: [stress, drawdown, volatility]
        operator_action: reduce risk and review exposures
        review_required: true
        note_template: "Review drawdown-sensitive positions and confirm defensive posture."
        return_shock: -0.05
        vol_multiplier: 1.5
        benchmark_shock: 0.0
      - name: benchmark_gap_down
        severity: high
        tags: [stress, benchmark, gap]
        operator_action: compare active risk versus benchmark and review hedge stance
        review_required: true
        note_template: "Check benchmark-relative weakness and decide whether to de-risk or hold."
        return_shock: -0.02
        vol_multiplier: 1.2
        benchmark_shock: -0.04
```

Expected timeline schema:
- `ticker`
- `start_date`
- `end_date`

`end_date` can be blank for currently active members.

When present, the backtest CLI now prints:
- multi-scenario stress summary
- walk-forward summary
- rebalance log including ADV-style liquidity fields

The dashboard backtest view now also shows:
- formatted summary metrics
- capacity warning banner when portfolio size exceeds estimated safe capacity
- scenario warning banner for high-severity presets
- scenario operator guidance (`operator_action`, `review_required`, `note_template`)
- scenario acknowledgement action that writes to `/api/ack`
- scenario results as a table instead of raw JSON
- scenario severity and tag metadata
- scenario tag filter buttons for operator slicing
- recent rebalance rows with formatted turnover, cost, ADV, and capacity values

The status API now surfaces top-level warnings via:
- `/api/status -> warnings.backtest_capacity_warning`
- `/api/status -> warnings.backtest_scenario_warning`
- both warnings now use a consistent `message` + `priority` model for operator-facing consumption

The default scenario table includes preset rows such as:
- return shock plus volatility expansion
- benchmark gap-down stress
- volatility compression comparison

Severity and tags are intended for operator-facing routing and readability.
For example, `high` severity scenarios can be surfaced as dashboard/status warnings.

The operator-facing preset fields are:
- `operator_action` - short recommended action for the operator
- `review_required` - whether the scenario should force explicit human review
- `note_template` - starter language for operator notes or acknowledgements

When `review_required` is true, the dashboard can raise an acknowledgement action and send the operator note through `/api/ack`.

The selection score is no longer just momentum plus trend. It now also includes:
- `persistence` - how consistently returns have stayed positive
- `recovery` - how much an asset has recovered from prior drawdown
- `downside_efficiency` - total return earned per unit of downside burden

The US ETF universe is also structured in sleeves:
- core tickers
- factor ETFs
- sector ETFs
- country ETFs

This widens the cross-sectional opportunity set instead of relying only on a small SPY/TLT-style universe.

The backtest now includes two comparison layers:
- `selection_score_comparison` - how the enhanced scoring changes picks versus the legacy score
- `strategy_comparison` - how enhanced vs legacy strategy behavior differs on return, drawdown, active return, turnover, and halts

For storage-constrained environments, provider validation outputs are designed to be parquet-first rather than CSV-first.
This keeps research artifacts compact and analytics-friendly on smaller SSDs.

Recommended storage model:
- parquet for raw prices, provider validation outputs, and research artifacts
- duckdb as an optional query layer on top of parquet, not as the primary raw storage layer

In other words:
- store first in parquet
- analyze many parquet files together with duckdb when comparison/aggregation becomes complex

## 7. Moving toward live trading

Live trading should be staged.

### Stage 1, report only
- run `run_report.py`
- inspect outputs

### Stage 2, dashboard and backtest scaffold
- run dashboard
- run backtest snapshot
- inspect runtime events and halt states

### Stage 3, Kiwoom dry-run review
Set:

```yaml
mode:
  dry_run: true
  enable_kiwoom: true
kiwoom:
  account_no: "YOUR_ACCOUNT_NO"
```

Then review:
- order plan preview
- live review checks
- audit logs
- runtime events

### Stage 4, first live rollout
Only after prior stages are stable:
- disable dry run
- start with small exposure
- actively monitor fills, audits, runtime events, and strategy halt signals

## 8. Commands you will actually use

### Run one report
```bash
python run_report.py
```

### Run scheduler
```bash
python run_auto.py
```

### Run dashboard
```bash
python run_web.py
```

### Run tests
```bash
pytest -q
```

### Run backtest scaffold
```bash
python run_backtest.py
```

## 9. APIs and dashboard visibility

Useful API endpoints:
- `/api/status`
- `/api/status -> warnings.backtest_capacity_warning`
- `/api/status -> warnings.backtest_scenario_warning`
- `/api/audit`
- `/api/ack`
- `/api/runtime`
- `/api/backtest`

The dashboard is the recommended first interface for operators.

## 10. Testing and quality

```bash
pytest -q
```

Coverage includes:
- config validation
- macro regime logic
- portfolio scoring and cuts
- notifier and scheduler behavior
- trade review and execution safety
- partial fill and replan flow
- operator acknowledgement flow
- runtime event persistence and resolution
- strategy halt behavior
- dashboard API behavior

## 11. Shared data integration

This project can load shared market data from [`m1nd0322/data`](https://github.com/m1nd0322/data).

Current integration behavior:
- shared parquet data is preferred when available
- missing tickers fall back to yfinance/FDR loaders
- cache support reduces repeated downloads

## 12. Limitations

- the current backtest is still minimal scaffolding
- strategy halt logic is safety-oriented, not a complete portfolio risk research layer
- Kiwoom live trading requires Windows-specific environment readiness

## 13. Recommended reading order for deeper understanding

1. `config.yaml`
2. `druck/engine.py`
3. `druck/trading.py`
4. `druck/runtime.py`
5. `druck/web/app.py`
6. dashboard while running locally

## Disclaimer

This repository is for research and engineering demonstration purposes only.
It is not investment advice.
