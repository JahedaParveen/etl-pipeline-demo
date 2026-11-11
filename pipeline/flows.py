"""Prefect flow: extract -> transform -> load, with retries, run tracking and alerts."""

from __future__ import annotations

from datetime import UTC, datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run, task_run

from pipeline.alerts import send_alert
from pipeline.cities import City, load_cities
from pipeline.config import Settings
from pipeline.db import get_engine, init_db
from pipeline.extract import fetch_forecasts
from pipeline.load import LoadResult, finish_run, load_batch, start_run
from pipeline.transform import TransformResult, transform_forecasts


class SimulatedFailure(RuntimeError):
    """Raised on purpose when SIMULATE_FAILURE is set, to demo retries and alerts."""


@task(name="extract", timeout_seconds=60, cache_policy=NO_CACHE)
def extract(cities: list[City], settings: Settings) -> list[dict]:
    logger = get_run_logger()
    attempt = task_run.run_count
    mode = settings.simulate_failure
    if mode == "always" or (mode == "once" and attempt <= 1):
        raise SimulatedFailure(
            f"SIMULATE_FAILURE={mode}: Open-Meteo request failed on purpose (attempt {attempt})"
        )
    payloads = fetch_forecasts(
        cities,
        base_url=settings.open_meteo_url,
        past_days=settings.past_days,
        timeout=settings.http_timeout_seconds,
    )
    logger.info("Fetched hourly forecasts for %d cities (attempt %d)", len(payloads), attempt)
    return payloads


@task(name="transform", timeout_seconds=60, cache_policy=NO_CACHE)
def transform(cities: list[City], payloads: list[dict]) -> TransformResult:
    logger = get_run_logger()
    result = transform_forecasts(cities, payloads, now=datetime.now(UTC))
    logger.info(
        "Transformed %d rows: %d valid, %d rejected",
        result.rows_extracted,
        len(result.valid),
        len(result.rejected),
    )
    for row in result.rejected[:10]:
        logger.warning("Rejected %s @ %s: %s", row["city"], row["observed_at"], row["reason"])
    return result


@task(name="load", timeout_seconds=120, retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def load(database_url: str, run_id: int, cities: list[City], data: TransformResult) -> LoadResult:
    logger = get_run_logger()
    result = load_batch(get_engine(database_url), run_id, cities, data.valid, data.rejected)
    logger.info(
        "Loaded into Postgres: %d inserted, %d updated, %d unchanged, %d rejected",
        result.inserted,
        result.updated,
        result.unchanged,
        result.rejected,
    )
    return result


def _run_link(settings: Settings) -> str:
    if settings.prefect_ui_url and flow_run.id:
        return f"\nRun details: {settings.prefect_ui_url}/runs/flow-run/{flow_run.id}"
    return ""


@flow(name="weather-etl", description="Hourly weather from Open-Meteo into PostgreSQL.")
def weather_etl() -> dict:
    settings = Settings.from_env()
    logger = get_run_logger()
    engine = get_engine(settings.database_url)
    init_db(engine)
    run_id = start_run(engine, flow_run_id=flow_run.id)
    logger.info("Started pipeline run #%d", run_id)

    rows_extracted = 0
    try:
        cities = load_cities(settings.cities_file)
        payloads = extract.with_options(
            retries=settings.extract_retries,
            retry_delay_seconds=settings.extract_retry_delay_seconds,
        )(cities, settings)
        data = transform(cities, payloads)
        rows_extracted = data.rows_extracted
        result = load(settings.database_url, run_id, cities, data)
    except Exception as exc:
        finish_run(
            engine,
            run_id,
            status="failed",
            rows_extracted=rows_extracted,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        send_alert(
            settings,
            title="Weather ETL failed",
            message=f"Pipeline run #{run_id} failed: {type(exc).__name__}: {exc}"
            + _run_link(settings),
            level="error",
        )
        raise

    finish_run(engine, run_id, status="success", rows_extracted=rows_extracted, result=result)
    if result.rejected:
        send_alert(
            settings,
            title="Weather ETL data quality warning",
            message=f"Pipeline run #{run_id} rejected {result.rejected} of {rows_extracted} rows. "
            "See the rejected_rows table for reasons." + _run_link(settings),
            level="warning",
        )

    summary = {
        "run_id": run_id,
        "rows_extracted": rows_extracted,
        "inserted": result.inserted,
        "updated": result.updated,
        "unchanged": result.unchanged,
        "rejected": result.rejected,
    }
    logger.info("Run #%d finished: %s", run_id, summary)
    return summary
