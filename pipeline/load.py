"""Write to PostgreSQL. Loads are idempotent: re-running the same hour never creates duplicates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Connection, Engine, func, insert, literal_column, or_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from pipeline import db
from pipeline.cities import City

WEATHER_VALUE_COLUMNS = (
    "temperature_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
)


@dataclass
class LoadResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0


def start_run(engine: Engine, flow_run_id: str | None) -> int:
    with engine.begin() as conn:
        return conn.execute(
            insert(db.pipeline_runs)
            .values(flow_run_id=flow_run_id, status="running")
            .returning(db.pipeline_runs.c.id)
        ).scalar_one()


def finish_run(
    engine: Engine,
    run_id: int,
    *,
    status: str,
    rows_extracted: int = 0,
    result: LoadResult | None = None,
    error_message: str | None = None,
) -> None:
    result = result or LoadResult()
    with engine.begin() as conn:
        conn.execute(
            update(db.pipeline_runs)
            .where(db.pipeline_runs.c.id == run_id)
            .values(
                status=status,
                finished_at=func.now(),
                rows_extracted=rows_extracted,
                rows_inserted=result.inserted,
                rows_updated=result.updated,
                rows_unchanged=result.unchanged,
                rows_rejected=result.rejected,
                error_message=error_message[:2000] if error_message else None,
            )
        )


def upsert_cities(conn: Connection, cities: list[City]) -> dict[str, int]:
    """Insert or update city metadata and return a name -> id map."""
    stmt = pg_insert(db.cities).values(
        [
            {
                "name": c.name,
                "country": c.country,
                "latitude": c.latitude,
                "longitude": c.longitude,
                "timezone": c.timezone,
                "population": c.population,
            }
            for c in cities
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["name"],
        set_={
            "country": stmt.excluded.country,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "timezone": stmt.excluded.timezone,
            "population": stmt.excluded.population,
            "updated_at": func.now(),
        },
    ).returning(db.cities.c.name, db.cities.c.id)
    return dict(conn.execute(stmt).tuples().all())


def upsert_weather(
    conn: Connection, rows: list[dict[str, Any]], city_ids: dict[str, int], run_id: int
) -> tuple[int, int]:
    """Upsert on the natural key (city_id, observed_at). Returns (inserted, updated).

    Rows whose values did not change are skipped by the WHERE clause, so they count
    as neither inserted nor updated.
    """
    if not rows:
        return 0, 0
    table = db.weather_hourly
    stmt = pg_insert(table).values(
        [
            {
                "city_id": city_ids[row["city"]],
                "observed_at": row["observed_at"],
                "run_id": run_id,
                **{col: row[col] for col in WEATHER_VALUE_COLUMNS},
            }
            for row in rows
        ]
    )
    changed = or_(
        *(table.c[col].is_distinct_from(stmt.excluded[col]) for col in WEATHER_VALUE_COLUMNS)
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["city_id", "observed_at"],
        set_={
            **{col: stmt.excluded[col] for col in WEATHER_VALUE_COLUMNS},
            "run_id": stmt.excluded.run_id,
            "updated_at": func.now(),
        },
        where=changed,
    ).returning(literal_column("(xmax = 0)").label("inserted"))

    flags = conn.execute(stmt).scalars().all()
    inserted = sum(1 for flag in flags if flag)
    return inserted, len(flags) - inserted


def insert_rejected(conn: Connection, rejected: list[dict[str, Any]], run_id: int) -> None:
    if not rejected:
        return
    conn.execute(
        insert(db.rejected_rows),
        [
            {
                "run_id": run_id,
                "city": r["city"],
                "observed_at": r["observed_at"],
                "reason": r["reason"],
                "payload": r["payload"],
            }
            for r in rejected
        ],
    )


def load_batch(
    engine: Engine,
    run_id: int,
    cities: list[City],
    valid: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
) -> LoadResult:
    """Load everything in one transaction, so a failure leaves the database unchanged."""
    with engine.begin() as conn:
        city_ids = upsert_cities(conn, cities)
        inserted, updated = upsert_weather(conn, valid, city_ids, run_id)
        insert_rejected(conn, rejected, run_id)
    return LoadResult(
        inserted=inserted,
        updated=updated,
        unchanged=len(valid) - inserted - updated,
        rejected=len(rejected),
    )
