from druck.scheduler import start_scheduler


class DummyScheduler:
    def __init__(self, timezone):
        self.timezone = timezone
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, name=None):
        self.jobs.append({"func": func, "trigger": trigger, "name": name})

    def start(self):
        self.started = True


def test_start_scheduler_registers_jobs(monkeypatch):
    scheduler = DummyScheduler("Asia/Seoul")
    cfg = {
        "schedule": {
            "timezone": "Asia/Seoul",
            "report_weekly": {"day_of_week": "sat", "hour": 9, "minute": 5},
            "risk_check_daily": {"hour": 16, "minute": 5},
            "market_data_collection": {
                "enabled": True,
                "kr_daily": {"hour": 6, "minute": 0, "command": "/tmp/run_kr.sh"},
                "us_daily": {"hour": 17, "minute": 0, "command": "/tmp/run_us.sh"},
            },
        }
    }

    monkeypatch.setattr("druck.scheduler.load_config", lambda *_args, **_kwargs: cfg)
    monkeypatch.setattr("druck.scheduler.BlockingScheduler", lambda timezone: scheduler)
    monkeypatch.setattr("druck.scheduler.CronTrigger", lambda **kwargs: kwargs)
    monkeypatch.setattr("druck.scheduler.send_telegram", lambda cfg, msg: None)
    monkeypatch.setattr("druck.scheduler._run_market_data_command", lambda command: None)
    start_scheduler()

    for job in scheduler.jobs:
        job["func"]()

    assert scheduler.started is True
    assert len(scheduler.jobs) == 4
    assert {job["name"] for job in scheduler.jobs} == {"weekly_report", "daily_risk_check", "kr_market_data_daily", "us_market_data_daily"}
