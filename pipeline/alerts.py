"""Send alerts to a Slack or Microsoft Teams incoming webhook, or to the log if none is set."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pipeline.config import Settings

logger = logging.getLogger("pipeline.alerts")


def build_payload(alert_format: str, title: str, message: str, level: str) -> dict[str, Any]:
    if alert_format == "teams":
        # Adaptive Card message, accepted by Teams "Workflows" webhooks.
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": title,
                                "weight": "Bolder",
                                "size": "Medium",
                                "color": "Attention" if level == "error" else "Warning",
                            },
                            {"type": "TextBlock", "text": message, "wrap": True},
                        ],
                    },
                }
            ],
        }
    # Slack incoming webhooks accept a plain "text" field.
    return {"text": f"*[{level.upper()}] {title}*\n{message}"}


def send_alert(settings: Settings, title: str, message: str, level: str = "error") -> bool:
    """Return True if the alert was delivered to the webhook."""
    if not settings.alert_webhook_url:
        logger.warning(
            "ALERT [%s] %s | %s (ALERT_WEBHOOK_URL not set, logged only)",
            level.upper(),
            title,
            message,
        )
        return False

    payload = build_payload(settings.alert_format, title, message, level)
    try:
        response = httpx.post(settings.alert_webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        # Never log the webhook URL itself: it is a secret.
        logger.error(
            "Could not deliver alert to webhook (%s). Alert was: %s | %s",
            type(exc).__name__,
            title,
            message,
        )
        return False
    logger.info("Alert sent to %s webhook: %s", settings.alert_format, title)
    return True
