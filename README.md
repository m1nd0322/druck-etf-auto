# druck-etf-auto

Macro regime + momentum ETF allocation engine for KR/US universes, designed as a production-minded personal quant portfolio project.

## Investment Thesis

- Cross-asset macro signals define market regime (`RISK_ON` / `NEUTRAL` / `RISK_OFF`).
- Within regime, ETF selection uses momentum and trend, penalized by volatility and drawdown.
- Portfolio risk is cut mechanically (200SMA break, trailing drawdown, hard stop), with capital rotated to cash ETF.

This structure aims to improve consistency across changing market states instead of relying on a single static allocation rule.

## Strategy Stack

- Macro filter components:
- `SPY` trend, `UUP` momentum, `HYG vs IEF` credit spread proxy, `TLT` rates, `^VIX` volatility regime
- Selection/scoring:
- Momentum + trend alpha terms
- Volatility and max drawdown penalty terms
- Regime-dependent concentration:
- `top_n_risk_on` and `top_n_risk_off` are separated in config
- Position sizing:
- Inverse-vol weighting with single-position cap (`max_weight`)
- Risk cut layer:
- Below 200SMA cut
- Trailing drawdown cut
- Hard stop cut
- Cut weight moves to configured cash ETF (`SHY` by default for US)

## Execution and Safety

- Default `dry_run: true` (safe by default, no live order submission).
- Scheduler-driven operation via APScheduler:
- Weekly report run
- Daily risk-check run
- Kiwoom OpenAPI+ execution path (Windows):
- Market-hours gate and near-close block
- Split execution (`split_n`)
- Unfilled cancel/reorder flow
- Slippage guard (`slippage_limit_bps`)
- Fill logging to SQLite (`trade_log.db`, ignored by git)

## Architecture

- `druck/engine.py`: end-to-end pipeline (`data -> regime -> scoring -> weights -> cuts -> report`)
- `druck/macro.py`: macro regime scoring and VIX spike halt signal
- `druck/portfolio.py`: cross-sectional scoring, sizing, risk cuts
- `druck/report.py`: Markdown/CSV report generation
- `druck/scheduler.py`: cron-based automation
- `druck/kiwoom_broker.py`: broker integration (Windows only)

## Quick Start

```bash
pip install -r requirements.txt
python run_report.py
```

Generated files:
- `output/selection_YYYYMMDD_HHMMSS.csv`
- `output/report_YYYYMMDD_HHMMSS.md`

## Automated Run

```bash
python run_auto.py
```

Schedule values are defined in `config.yaml` under `schedule`.

## Live Trading Setup (Kiwoom, Windows)

1. Install Kiwoom OpenAPI+
2. Ensure `PyQt5` is available (Windows marker is already in `requirements.txt`)
3. Update `config.yaml`:

```yaml
mode:
  enable_kiwoom: true
  dry_run: true
kiwoom:
  account_no: "YOUR_ACCOUNT_NO"
```

Start in dry-run mode, verify logs/reports, then switch to live only after controls are validated.

## Reproducibility and Config

- Main config: `config.yaml`
- Optional local override pattern: `config.local.yaml` (ignored)
- Data cache: `.cache/` via `data.cache_csv: true`
- Secrets: use environment variables (Telegram bot token/chat id)

## Roadmap

- Add deterministic backtest module with transaction cost model
- Add portfolio analytics (CAGR, max DD, turnover, exposure by sleeve)
- Add test coverage for scoring/risk-cut logic and config schema validation

## Disclaimer

This repository is for research and engineering demonstration purposes only. It is not investment advice.
