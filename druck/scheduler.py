from __future__ import annotations
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .engine import run_once
from .macro import is_vix_spike

def start_scheduler():
    cfg = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
    tz = cfg['schedule'].get('timezone','Asia/Seoul')
    sched = BlockingScheduler(timezone=tz)

    w = cfg['schedule']['report_weekly']
    d = cfg['schedule']['risk_check_daily']

    sched.add_job(lambda: run_once(cfg, do_trade=False), CronTrigger(day_of_week=w['day_of_week'], hour=w['hour'], minute=w['minute']), name="weekly_report")
    sched.add_job(lambda: run_once(cfg, do_trade=False), CronTrigger(hour=d['hour'], minute=d['minute']), name="daily_risk_check")

    print("[scheduler] started")
    sched.start()
