from druck.notifier import send_telegram


class DummyResponse:
    def raise_for_status(self):
        return None


def test_send_telegram_skips_when_disabled(monkeypatch):
    called = {"value": False}

    def fake_post(*args, **kwargs):
        called["value"] = True
        return DummyResponse()

    monkeypatch.setattr("requests.post", fake_post)
    send_telegram({"notifier": {"telegram": {"enabled": False}}}, "hello")
    assert called["value"] is False
