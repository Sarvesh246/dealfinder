import importlib


def test_send_alerts_returns_false_without_config(monkeypatch):
    import alerts

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
    monkeypatch.setenv("GMAIL_USER", "")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "")
    monkeypatch.setenv("ALERT_EMAIL", "")
    importlib.reload(alerts)

    assert alerts.send_alerts("AirPods Pro 3", 199.0, 199.0, "https://example.com") is False


def test_send_alerts_returns_true_when_discord_succeeds(monkeypatch):
    import alerts

    class _Resp:
        status_code = 204

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("GMAIL_USER", "")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "")
    monkeypatch.setenv("ALERT_EMAIL", "")
    monkeypatch.setattr(alerts.requests, "post", lambda *args, **kwargs: _Resp())
    importlib.reload(alerts)
    monkeypatch.setattr(alerts.requests, "post", lambda *args, **kwargs: _Resp())

    assert alerts.send_alerts("AirPods Pro 3", 199.0, 199.0, "https://example.com") is True


def test_send_alerts_any_drop_payload_mentions_previous_price(monkeypatch):
    import alerts

    class _Resp:
        status_code = 204

    payloads = []
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("GMAIL_USER", "")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "")
    monkeypatch.setenv("ALERT_EMAIL", "")
    importlib.reload(alerts)

    def _post(*args, **kwargs):
        payloads.append(kwargs["json"])
        return _Resp()

    monkeypatch.setattr(alerts.requests, "post", _post)

    sent = alerts.send_alerts(
        "Instant Pot Duo 7-in-1",
        89.99,
        alert_mode="any_drop",
        previous_price=99.99,
        url="https://example.com/instant-pot",
    )

    assert sent is True
    assert payloads
    fields = payloads[0]["embeds"][0]["fields"]
    assert any(field["name"] == "Previous Price" and "$99.99" in field["value"] for field in fields)
    assert any(field["name"] == "Drop Amount" and "$10.00" in field["value"] for field in fields)


def test_notification_status_reports_missing_and_configured_channels(monkeypatch):
    import alerts

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("GMAIL_USER", "sender@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "secret")
    monkeypatch.setenv("ALERT_EMAIL", "alerts@example.com")
    importlib.reload(alerts)

    status = alerts.get_notification_status()

    assert status["discord_configured"] is True
    assert status["gmail_configured"] is True
    assert status["gmail_target"] == "alerts@example.com"
