"""Database schema (SQLAlchemy Core) and connection helpers."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Engine,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

from pipeline.config import PROJECT_ROOT

metadata = MetaData()

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("flow_run_id", Text),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", Text, nullable=False),  # running | success | failed
    Column("rows_extracted", Integer, nullable=False, server_default="0"),
    Column("rows_inserted", Integer, nullable=False, server_default="0"),
    Column("rows_updated", Integer, nullable=False, server_default="0"),
    Column("rows_unchanged", Integer, nullable=False, server_default="0"),
    Column("rows_rejected", Integer, nullable=False, server_default="0"),
    Column("error_message", Text),
)

cities = Table(
    "cities",
    metadata,
    Column("id", Integer, Identity(), primary_key=True),
    Column("name", Text, nullable=False, unique=True),
    Column("country", Text, nullable=False),
    Column("latitude", Numeric(8, 4), nullable=False),
    Column("longitude", Numeric(8, 4), nullable=False),
    Column("timezone", Text, nullable=False),
    Column("population", Integer),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

weather_hourly = Table(
    "weather_hourly",
    metadata,
    Column("city_id", Integer, ForeignKey("cities.id"), primary_key=True),
    Column("observed_at", DateTime(timezone=True), primary_key=True),
    Column("temperature_c", Numeric(5, 2), nullable=False),
    Column("relative_humidity_pct", Numeric(5, 2)),
    Column("precipitation_mm", Numeric(6, 2)),
    Column("wind_speed_kmh", Numeric(6, 2)),
    Column("run_id", BigInteger, ForeignKey("pipeline_runs.id")),
    Column("loaded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_weather_hourly_observed_at", "observed_at"),
)

rejected_rows = Table(
    "rejected_rows",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("run_id", BigInteger, ForeignKey("pipeline_runs.id"), nullable=False),
    Column("city", Text),
    Column("observed_at", DateTime(timezone=True)),
    Column("reason", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("rejected_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

VIEWS_FILE = PROJECT_ROOT / "sql" / "views.sql"


@lru_cache(maxsize=4)
def get_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def split_sql(sql: str) -> list[str]:
    """Split a simple SQL file into statements, dropping '--' comment lines."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def init_db(engine: Engine) -> None:
    """Create tables and views if they do not exist. Safe to call on every run."""
    metadata.create_all(engine)
    with engine.begin() as conn:
        for statement in split_sql(VIEWS_FILE.read_text(encoding="utf-8")):
            conn.exec_driver_sql(statement)
