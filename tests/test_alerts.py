import logging

import httpx

from pipeline import alerts
from pipeline.config import Settings


def test_slack_payload():
    payload = alerts.build_payload("slack", "Pipeline failed", "boom", "error")
    assert payload == {"text": "*[ERROR] Pipeline failed*\nboom"}


def test_teams_payload_is_adaptive_card():
    payload = alerts.build_payload("teams", "Pipeline failed", "boom", "warning")
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"
    assert card["body"][0]["text"] == "Pipeline failed"
    assert card["body"][0]["color"] == "Warning"
    assert card["body"][1]["text"] == "boom"


def test_without_webhook_alert_goes_to_log(caplog):
    settings = Settings.from_env({})
    with caplog.at_level(logging.WARNING, logger="pipeline.alerts"):
        delivered = alerts.send_alert(settings, "Pipeline failed", "boom")
    assert delivered is False
    assert "ALERT [ERROR] Pipeline failed | boom" in caplog.text


def test_webhook_is_called(monkeypatch):
    calls = []

    def fake_post(url, json, timeout):
        calls.append((url, json))
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(alerts.httpx, "post", fake_post)
    settings = Settings.from_env({"ALERT_WEBHOOK_URL": "https://hooks.example.com/abc"})
    assert alerts.send_alert(settings, "Pipeline failed", "boom") is True
    assert calls == [("https://hooks.example.com/abc", {"text": "*[ERROR] Pipeline failed*\nboom"})]


def test_webhook_error_does_not_raise_or_leak_url(monkeypatch, caplog):
    def fake_post(url, json, timeout):
        return httpx.Response(500, request=httpx.Request("POST", url))

    monkeypatch.setattr(alerts.httpx, "post", fake_post)
    settings = Settings.from_env({"ALERT_WEBHOOK_URL": "https://hooks.example.com/secret-token"})
    with caplog.at_level(logging.ERROR, logger="pipeline.alerts"):
        assert alerts.send_alert(settings, "Pipeline failed", "boom") is False
    assert "secret-token" not in caplog.text
