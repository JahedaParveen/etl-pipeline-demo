from pathlib import Path

import pytest

from pipeline.cities import load_cities
from pipeline.config import PROJECT_ROOT, Settings
from pipeline.db import split_sql
from pipeline.extract import build_params


class TestLoadCities:
    def test_bundled_csv_is_valid(self):
        cities = load_cities(PROJECT_ROOT / "data" / "cities.csv")
        assert len(cities) == 8
        assert cities[0].name == "London"
        assert cities[0].timezone == "Europe/London"

    def test_missing_column(self, tmp_path: Path):
        path = tmp_path / "cities.csv"
        path.write_text("city,country\nLondon,UK\n", encoding="utf-8")
        with pytest.raises(ValueError, match="missing columns"):
            load_cities(path)

    def test_duplicate_city(self, tmp_path: Path):
        path = tmp_path / "cities.csv"
        path.write_text(
            "city,country,latitude,longitude,timezone\n"
            "London,UK,51.5,-0.1,Europe/London\n"
            "London,UK,51.5,-0.1,Europe/London\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="line 3: duplicate city"):
            load_cities(path)

    def test_bad_coordinates(self, tmp_path: Path):
        path = tmp_path / "cities.csv"
        path.write_text(
            "city,country,latitude,longitude,timezone\nNowhere,XX,95,0,UTC\n", encoding="utf-8"
        )
        with pytest.raises(ValueError, match="out of range"):
            load_cities(path)


class TestSettings:
    def test_defaults(self):
        s = Settings.from_env({})
        assert s.database_url == "postgresql+psycopg://etl:etl@localhost:5433/weather"
        assert s.simulate_failure == "none"
        assert s.alert_webhook_url is None
        assert s.run_on_startup is True

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("true", "always"), ("false", "none"), ("once", "once"), ("ALWAYS", "always")],
    )
    def test_simulate_failure_values(self, raw, expected):
        assert Settings.from_env({"SIMULATE_FAILURE": raw}).simulate_failure == expected

    def test_invalid_simulate_failure(self):
        with pytest.raises(ValueError, match="SIMULATE_FAILURE"):
            Settings.from_env({"SIMULATE_FAILURE": "sometimes"})

    def test_invalid_alert_format(self):
        with pytest.raises(ValueError, match="ALERT_FORMAT"):
            Settings.from_env({"ALERT_FORMAT": "email"})


def test_build_params_batches_all_cities(two_cities):
    params = build_params(two_cities, past_days=1)
    assert params["latitude"] == "51.5074,35.6762"
    assert params["longitude"] == "-0.1278,139.6503"
    assert params["hourly"] == "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"


def test_views_file_splits_into_statements():
    statements = split_sql((PROJECT_ROOT / "sql" / "views.sql").read_text(encoding="utf-8"))
    assert len(statements) == 2
    assert all(s.startswith("CREATE OR REPLACE VIEW") for s in statements)
