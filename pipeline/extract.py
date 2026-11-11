"""Fetch hourly weather from the free Open-Meteo API (no API key needed)."""

from __future__ import annotations

import httpx

from pipeline.cities import City
from pipeline.transform import HOURLY_FIELDS


def build_params(cities: list[City], past_days: int) -> dict[str, str | int]:
    """One request covers all cities: Open-Meteo accepts comma-separated coordinates."""
    return {
        "latitude": ",".join(f"{c.latitude:.4f}" for c in cities),
        "longitude": ",".join(f"{c.longitude:.4f}" for c in cities),
        "hourly": ",".join(HOURLY_FIELDS),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "GMT",
        "wind_speed_unit": "kmh",
    }


def fetch_forecasts(
    cities: list[City], *, base_url: str, past_days: int, timeout: float
) -> list[dict]:
    response = httpx.get(base_url, params=build_params(cities, past_days), timeout=timeout)
    response.raise_for_status()
    data = response.json()
    # A single coordinate returns an object, several return a list.
    return data if isinstance(data, list) else [data]
