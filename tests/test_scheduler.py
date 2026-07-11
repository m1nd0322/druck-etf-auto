import os
from pathlib import Path
import shlex
import subprocess

from druck.config import load_config
from druck.scheduler import _run_market_data_command, start_scheduler


class DummyScheduler:
    def __init__(self, timezone):
        self.timezone = timezone
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, name=None):
        self.jobs.append({"func": func, "trigger": trigger, "name": name})

    def start(self):
        self.started = True


def test_market_data_command_uses_argv_without_shell(monkeypatch):
    calls = []

    def fake_run(args, *, check, cwd):
        calls.append((args, check, cwd))

    monkeypatch.setattr("druck.scheduler.subprocess.run", fake_run)

    _run_market_data_command("/tmp/run_collect.sh --full")

    root = Path(__file__).resolve().parents[1]
    assert calls == [(["/tmp/run_collect.sh", "--full"], True, root)]


def test_scheduled_market_data_commands_resolve_inside_repository():
    root = Path(__file__).resolve().parents[1]

    for config_path in root.glob("config*.yaml"):
        cfg = load_config(config_path)
        market_data = cfg.get("schedule", {}).get("market_data_collection", {})
        for job_name in ("kr_daily", "us_daily"):
            command = shlex.split(str((market_data.get(job_name) or {}).get("command", "")))
            if not command:
                continue
            assert not Path(command[0]).is_absolute()
            assert (root / command[0]).is_file()


def test_market_data_wrappers_are_cwd_independent(tmp_path):
    root = Path(__file__).resolve().parents[1]
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    env = {
        **os.environ,
        "DRUCK_PYTHON": str(fake_python),
        "DRUCK_LOG_DIR": str(tmp_path),
    }

    for name in ("run_kr_collect.sh", "run_us_collect.sh"):
        script = root / "automation" / "market-data" / name
        subprocess.run([script], cwd=tmp_path, env=env, check=True)


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
