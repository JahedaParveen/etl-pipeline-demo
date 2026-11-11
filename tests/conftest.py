import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.cities import City

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def two_cities() -> list[City]:
    return [
        City("London", "United Kingdom", 51.5074, -0.1278, "Europe/London", 8866000),
        City("Tokyo", "Japan", 35.6762, 139.6503, "Asia/Tokyo", 14090000),
    ]


@pytest.fixture
def two_city_payloads() -> list[dict]:
    return json.loads((FIXTURES / "open_meteo_two_cities.json").read_text(encoding="utf-8"))


@pytest.fixture
def now() -> datetime:
    """Fixed 'current time': the 03:00 hour in the fixture is still in the future."""
    return datetime(2026, 11, 10, 2, 30, tzinfo=UTC)
