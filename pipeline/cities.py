"""Load and validate the city metadata CSV (the pipeline's second data source)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

REQUIRED_COLUMNS = {"city", "country", "latitude", "longitude", "timezone"}


@dataclass(frozen=True)
class City:
    name: str
    country: str
    latitude: float
    longitude: float
    timezone: str
    population: int | None = None


def load_cities(path: Path | str) -> list[City]:
    """Read cities from a CSV file. Raises ValueError with the line number on bad input."""
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path}: missing columns {sorted(missing)}")

        cities: list[City] = []
        seen: set[str] = set()
        for line_no, record in enumerate(reader, start=2):
            name = (record.get("city") or "").strip()
            if not name:
                raise ValueError(f"{path} line {line_no}: city name is empty")
            if name in seen:
                raise ValueError(f"{path} line {line_no}: duplicate city {name!r}")
            seen.add(name)

            try:
                latitude = float(record["latitude"])
                longitude = float(record["longitude"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{path} line {line_no}: invalid coordinates") from exc
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError(f"{path} line {line_no}: coordinates out of range")

            population_raw = (record.get("population") or "").strip()
            cities.append(
                City(
                    name=name,
                    country=(record.get("country") or "").strip(),
                    latitude=latitude,
                    longitude=longitude,
                    timezone=(record.get("timezone") or "").strip() or "UTC",
                    population=int(population_raw) if population_raw else None,
                )
            )

    if not cities:
        raise ValueError(f"{path}: no cities found")
    return cities
