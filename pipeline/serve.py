"""Worker entrypoint: optionally run once at startup, then serve the flow on a schedule."""

from __future__ import annotations

import logging

from pipeline.config import Settings
from pipeline.flows import weather_etl

logger = logging.getLogger("pipeline.serve")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()

    if settings.run_on_startup:
        logger.info("RUN_ON_STARTUP is on: running the pipeline once now")
        try:
            weather_etl()
        except Exception:
            logger.exception("Startup run failed. The schedule will keep running.")

    logger.info("Serving deployment 'weather-etl/hourly' on cron '%s'", settings.schedule_cron)
    weather_etl.serve(
        name="hourly",
        cron=settings.schedule_cron,
        tags=["weather", "etl"],
        description="Pull hourly weather for the cities in data/cities.csv into Postgres.",
    )


if __name__ == "__main__":
    main()
