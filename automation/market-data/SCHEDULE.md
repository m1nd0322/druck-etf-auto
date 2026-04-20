# Market Data Collection Schedule

OpenClaw scheduler configuration for daily market data collection.

## Daily jobs

- KR market data collection: 06:00 Asia/Seoul
- US market data collection: 17:00 Asia/Seoul

## Commands

- `/home/node/.openclaw/workspace/tmp/druck-etf-auto/automation/market-data/run_kr_collect.sh`
- `/home/node/.openclaw/workspace/tmp/druck-etf-auto/automation/market-data/run_us_collect.sh`

## Config source

Configured in `config.yaml` under:

```yaml
schedule:
  market_data_collection:
    enabled: true
    kr_daily:
      hour: 6
      minute: 0
      command: "/home/node/.openclaw/workspace/tmp/druck-etf-auto/automation/market-data/run_kr_collect.sh"
    us_daily:
      hour: 17
      minute: 0
      command: "/home/node/.openclaw/workspace/tmp/druck-etf-auto/automation/market-data/run_us_collect.sh"
```

## Notes

- This uses the project scheduler in `druck/scheduler.py`, not system crontab.
- Scheduler timezone is `Asia/Seoul`.
- Logs are written by each shell wrapper into `automation/market-data/*.log`.
