"""Settings read from environment variables, with defaults that work out of the box."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

FAILURE_MODES = ("none", "once", "always")
ALERT_FORMATS = ("slack", "teams")


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"Expected a true/false value, got {value!r}")


def _parse_failure_mode(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {"": "none", "false": "none", "off": "none", "true": "always", "on": "always"}
    normalized = aliases.get(normalized, normalized)
    if normalized not in FAILURE_MODES:
        raise ValueError(f"SIMULATE_FAILURE must be one of {FAILURE_MODES}, got {value!r}")
    return normalized


@dataclass(frozen=True)
class Settings:
    database_url: str
    cities_file: Path
    open_meteo_url: str
    past_days: int
    http_timeout_seconds: float
    extract_retries: int
    extract_retry_delay_seconds: int
    simulate_failure: str
    schedule_cron: str
    run_on_startup: bool
    alert_webhook_url: str | None
    alert_format: str
    prefect_ui_url: str | None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env

        def get(name: str, default: str) -> str:
            value = env.get(name)
            return default if value is None or value.strip() == "" else value.strip()

        default_db_url = "postgresql+psycopg://{user}:{password}@localhost:{port}/{db}".format(
            user=get("POSTGRES_USER", "etl"),
            password=get("POSTGRES_PASSWORD", "etl"),
            port=get("POSTGRES_HOST_PORT", "5433"),
            db=get("POSTGRES_DB", "weather"),
        )

        alert_format = get("ALERT_FORMAT", "slack").lower()
        if alert_format not in ALERT_FORMATS:
            raise ValueError(f"ALERT_FORMAT must be one of {ALERT_FORMATS}, got {alert_format!r}")

        past_days = int(get("PAST_DAYS", "1"))
        if not 0 <= past_days <= 92:
            raise ValueError("PAST_DAYS must be between 0 and 92")

        return cls(
            database_url=get("DATABASE_URL", default_db_url),
            cities_file=Path(get("CITIES_FILE", str(PROJECT_ROOT / "data" / "cities.csv"))),
            open_meteo_url=get("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"),
            past_days=past_days,
            http_timeout_seconds=float(get("HTTP_TIMEOUT_SECONDS", "20")),
            extract_retries=int(get("EXTRACT_RETRIES", "3")),
            extract_retry_delay_seconds=int(get("EXTRACT_RETRY_DELAY_SECONDS", "10")),
            simulate_failure=_parse_failure_mode(get("SIMULATE_FAILURE", "none")),
            schedule_cron=get("SCHEDULE_CRON", "0 * * * *"),
            run_on_startup=_parse_bool(get("RUN_ON_STARTUP", "true")),
            alert_webhook_url=env.get("ALERT_WEBHOOK_URL", "").strip() or None,
            alert_format=alert_format,
            prefect_ui_url=env.get("PREFECT_UI_URL", "").strip().rstrip("/") or None,
        )
