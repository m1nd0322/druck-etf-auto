from __future__ import annotations
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import load_config
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


def start_scheduler():
    cfg = load_config("config.yaml")
    tz = cfg['schedule'].get('timezone','Asia/Seoul')
    sched = BlockingScheduler(timezone=tz)
    reporter = _make_reporter(cfg)

    w = cfg['schedule']['report_weekly']
    d = cfg['schedule']['risk_check_daily']

    sched.add_job(lambda: run_guarded(lambda: run_once(cfg, do_trade=False), reporter=reporter), CronTrigger(day_of_week=w['day_of_week'], hour=w['hour'], minute=w['minute']), name="weekly_report")
    sched.add_job(lambda: run_guarded(lambda: run_once(cfg, do_trade=False), reporter=reporter), CronTrigger(hour=d['hour'], minute=d['minute']), name="daily_risk_check")

    print("[scheduler] started")
    sched.start()
