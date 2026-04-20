from __future__ import annotations
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import load_config
import subprocess

from .engine import run_once
from .notifier import send_telegram
from .runtime import RuntimeEvent, db_runtime_reporter, run_guarded


def _make_reporter(cfg: dict):
    db_reporter = db_runtime_reporter()

    def reporter(event: RuntimeEvent):
        prefix = "[Druck Runtime]"
        msg = f"{prefix} {event.category}: {event.message}"
        if event.detail:
            msg += f" | {event.detail}"
        print(msg)
        db_reporter(event)
        send_telegram(cfg, msg)
    return reporter


def _run_market_data_command(command: str):
    subprocess.run(command, shell=True, check=True)


def start_scheduler():
    cfg = load_config("config.yaml")
    tz = cfg['schedule'].get('timezone','Asia/Seoul')
    sched = BlockingScheduler(timezone=tz)
    reporter = _make_reporter(cfg)

    w = cfg['schedule']['report_weekly']
    d = cfg['schedule']['risk_check_daily']

    sched.add_job(lambda: run_guarded(lambda: run_once(cfg, do_trade=False), reporter=reporter), CronTrigger(day_of_week=w['day_of_week'], hour=w['hour'], minute=w['minute']), name="weekly_report")
    sched.add_job(lambda: run_guarded(lambda: run_once(cfg, do_trade=False), reporter=reporter), CronTrigger(hour=d['hour'], minute=d['minute']), name="daily_risk_check")

    market_data = cfg.get('schedule', {}).get('market_data_collection', {})
    if market_data.get('enabled', False):
        kr = market_data.get('kr_daily', {}) or {}
        us = market_data.get('us_daily', {}) or {}
        kr_cmd = str(kr.get('command', '')).strip()
        us_cmd = str(us.get('command', '')).strip()
        if kr_cmd:
            sched.add_job(
                lambda: run_guarded(lambda: _run_market_data_command(kr_cmd), reporter=reporter),
                CronTrigger(hour=int(kr.get('hour', 6)), minute=int(kr.get('minute', 0))),
                name="kr_market_data_daily",
            )
        if us_cmd:
            sched.add_job(
                lambda: run_guarded(lambda: _run_market_data_command(us_cmd), reporter=reporter),
                CronTrigger(hour=int(us.get('hour', 17)), minute=int(us.get('minute', 0))),
                name="us_market_data_daily",
            )

    print("[scheduler] started")
    sched.start()
