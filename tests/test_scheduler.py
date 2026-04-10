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
        }
    }

    monkeypatch.setattr("druck.scheduler.load_config", lambda *_args, **_kwargs: cfg)
    monkeypatch.setattr("druck.scheduler.BlockingScheduler", lambda timezone: scheduler)
    monkeypatch.setattr("druck.scheduler.CronTrigger", lambda **kwargs: kwargs)
    start_scheduler()

    assert scheduler.started is True
    assert len(scheduler.jobs) == 2
    assert {job["name"] for job in scheduler.jobs} == {"weekly_report", "daily_risk_check"}
