"""Pure transform and validation functions. No network or database access, so easy to test."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pipeline.cities import City

# Open-Meteo hourly variable -> column name in our database
HOURLY_FIELDS = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "relative_humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
}

REQUIRED_FIELDS = ("temperature_c",)

# Allowed ranges. Anything outside these is treated as a sensor or API error.
VALID_RANGES = {
    "temperature_c": (-90.0, 60.0),
    "relative_humidity_pct": (0.0, 100.0),
    "precipitation_mm": (0.0, 500.0),
    "wind_speed_kmh": (0.0, 400.0),
}


@dataclass
class TransformResult:
    rows_extracted: int = 0
    valid: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an Open-Meteo timestamp such as '2026-11-10T13:00' (UTC). Returns None if invalid."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def pair_payloads(cities: list[City], payloads: list[dict]) -> list[tuple[City, dict]]:
    """Open-Meteo returns one result per coordinate, in request order."""
    if len(cities) != len(payloads):
        raise ValueError(f"Expected {len(cities)} forecasts from the API, got {len(payloads)}")
    return list(zip(cities, payloads, strict=True))


def flatten_forecast(city_name: str, payload: dict, now: datetime) -> list[dict[str, Any]]:
    """Turn Open-Meteo's column-oriented 'hourly' block into one row per hour.

    Hours later than `now` are forecasts, not observations, so they are skipped.
    Rows with an unparseable timestamp are kept so validation can reject them with a reason.
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    rows = []
    for i, raw_time in enumerate(times):
        observed_at = parse_timestamp(raw_time)
        if observed_at is not None and observed_at > now:
            continue
        row: dict[str, Any] = {"city": city_name, "observed_at": observed_at, "raw_time": raw_time}
        for source, column in HOURLY_FIELDS.items():
            values = hourly.get(source) or []
            row[column] = values[i] if i < len(values) else None
        rows.append(row)
    return rows


def check_row(row: dict[str, Any]) -> list[str]:
    """Return the list of data quality problems for one row (empty list means the row is valid)."""
    problems = []
    if row.get("observed_at") is None:
        problems.append(f"invalid timestamp {row.get('raw_time')!r}")
    for column in REQUIRED_FIELDS:
        if row.get(column) is None:
            problems.append(f"missing {column}")
    for column, (low, high) in VALID_RANGES.items():
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            problems.append(f"{column} is not a number ({value!r})")
        elif not low <= value <= high:
            problems.append(f"{column}={value} outside [{low:g}, {high:g}]")
    return problems


def to_json_safe(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def validate_rows(rows: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Split rows into (valid, rejected). Each rejected row carries a human-readable reason."""
    valid, rejected = [], []
    seen: set[tuple[str, datetime]] = set()
    for row in rows:
        problems = check_row(row)
        key = (row["city"], row.get("observed_at"))
        if not problems and key in seen:
            problems.append("duplicate city and hour in the same batch")
        if problems:
            rejected.append(
                {
                    "city": row["city"],
                    "observed_at": row.get("observed_at"),
                    "reason": "; ".join(problems),
                    "payload": to_json_safe(row),
                }
            )
            continue
        seen.add(key)
        valid.append({k: v for k, v in row.items() if k != "raw_time"})
    return valid, rejected


def transform_forecasts(
    cities: list[City], payloads: list[dict], now: datetime | None = None
) -> TransformResult:
    now = now or datetime.now(UTC)
    result = TransformResult()
    rows = []
    for city, payload in pair_payloads(cities, payloads):
        rows.extend(flatten_forecast(city.name, payload, now))
    result.rows_extracted = len(rows)
    result.valid, result.rejected = validate_rows(rows)
    return result
