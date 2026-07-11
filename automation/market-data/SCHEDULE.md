# Market Data Collection Schedule

Daily market data collection configuration for the built-in scheduler and OpenClaw cron.

## Daily jobs

- KR market data collection: 06:00 Asia/Seoul
- US market data collection: 17:00 Asia/Seoul

## Commands

- `automation/market-data/run_kr_collect.sh`
- `automation/market-data/run_us_collect.sh`

## Config source

Configured in `config.yaml` under:

```yaml
schedule:
  market_data_collection:
    enabled: true
    kr_daily:
      hour: 6
      minute: 0
      command: "automation/market-data/run_kr_collect.sh"
    us_daily:
      hour: 17
      minute: 0
      command: "automation/market-data/run_us_collect.sh"
```

## Notes

- The current active production schedule is attached through OpenClaw built-in cron, not system crontab.
- The wrappers derive the project root from their own location, so the checkout can move without editing commands.
- Set `DRUCK_PYTHON` to override the default `.venv/bin/python` interpreter and `DRUCK_LOG_DIR` to redirect wrapper logs.
- Scheduler timezone is `Asia/Seoul`.
- Cron delivery is set to `none` so Telegram `@heartbeat` delivery resolution failures do not mark otherwise-useful collection runs as delivery errors.
- Logs are written by each shell wrapper into `automation/market-data/*.log`.
- Current tuned operational chunk sizes:
  - KR: `1`
  - US: `50`
  - indexes: `10`
- Important operational note: KR collection currently behaves reliably only with effectively single-ticker chunks. In testing, KR sample collection succeeded with chunk size `1` but failed again with larger chunks, so KR remains intentionally conservative.
- Summary JSON now includes per-group `missing_ticker_examples` and `warning_summary` so the first daily cron runs can be reviewed quickly without opening every parquet artifact.
