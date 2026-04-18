from druck.runtime import RuntimeEvent, report_event, run_guarded


def test_run_guarded_captures_system_error():
    events = []

    def reporter(event: RuntimeEvent):
        events.append(event)

    result = run_guarded(lambda: (_ for _ in ()).throw(RuntimeError("boom")), reporter=reporter)
    assert result.ok is False
    assert result.halted is False
    assert result.category == "system_error"
    assert events[0].category == "system_error"


def test_run_guarded_reports_strategy_halt_payload():
    events = []

    def reporter(event: RuntimeEvent):
        events.append(event)

    result = run_guarded(lambda: {"strategy_halt": True, "halt_reason": "negative_momentum_halt", "halt_detail": "3 assets negative"}, reporter=reporter)
    assert result.ok is False
    assert result.halted is True
    assert result.category == "strategy_halt"
    assert events[0].message == "negative_momentum_halt"


def test_report_event_swallows_reporter_failure_without_raising():
    called = {"count": 0}

    def reporter(event: RuntimeEvent):
        called["count"] += 1
        raise RuntimeError("reporter failed")

    report_event(reporter, RuntimeEvent(category="system_error", message="boom"))
    assert called["count"] == 1


def test_run_guarded_returns_payload_even_if_reporter_fails():
    def reporter(event: RuntimeEvent):
        raise RuntimeError("reporter failed")

    result = run_guarded(lambda: {"strategy_halt": True, "halt_reason": "weakness", "halt_detail": "detail"}, reporter=reporter)
    assert result.ok is False
    assert result.halted is True
    assert result.category == "strategy_halt"
    assert result.payload["halt_reason"] == "weakness"
