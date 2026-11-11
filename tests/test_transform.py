from datetime import UTC, datetime

import pytest

from pipeline.transform import (
    check_row,
    flatten_forecast,
    pair_payloads,
    parse_timestamp,
    transform_forecasts,
    validate_rows,
)


def make_row(**overrides):
    row = {
        "city": "London",
        "observed_at": datetime(2026, 11, 10, 1, tzinfo=UTC),
        "raw_time": "2026-11-10T01:00",
        "temperature_c": 10.0,
        "relative_humidity_pct": 80,
        "precipitation_mm": 0.0,
        "wind_speed_kmh": 12.0,
    }
    row.update(overrides)
    return row


class TestParseTimestamp:
    def test_naive_open_meteo_time_is_utc(self):
        assert parse_timestamp("2026-11-10T13:00") == datetime(2026, 11, 10, 13, tzinfo=UTC)

    def test_offset_is_converted_to_utc(self):
        assert parse_timestamp("2026-11-10T13:00+02:00") == datetime(2026, 11, 10, 11, tzinfo=UTC)

    @pytest.mark.parametrize("value", ["not-a-date", "", None, 12345])
    def test_invalid_values_return_none(self, value):
        assert parse_timestamp(value) is None


class TestFlattenForecast:
    def test_one_row_per_past_hour(self, two_city_payloads, now):
        rows = flatten_forecast("London", two_city_payloads[0], now)
        assert [r["observed_at"].hour for r in rows] == [0, 1, 2]  # 03:00 is in the future
        assert rows[0] == {
            "city": "London",
            "observed_at": datetime(2026, 11, 10, 0, tzinfo=UTC),
            "raw_time": "2026-11-10T00:00",
            "temperature_c": 9.1,
            "relative_humidity_pct": 86,
            "precipitation_mm": 0.0,
            "wind_speed_kmh": 14.2,
        }

    def test_short_value_arrays_become_none(self, now):
        payload = {"hourly": {"time": ["2026-11-10T00:00"], "temperature_2m": []}}
        rows = flatten_forecast("London", payload, now)
        assert rows[0]["temperature_c"] is None

    def test_missing_hourly_block_gives_no_rows(self, now):
        assert flatten_forecast("London", {}, now) == []

    def test_bad_timestamp_is_kept_for_validation(self, now):
        payload = {"hourly": {"time": ["garbage"], "temperature_2m": [10.0]}}
        rows = flatten_forecast("London", payload, now)
        assert rows[0]["observed_at"] is None


class TestCheckRow:
    def test_valid_row_has_no_problems(self):
        assert check_row(make_row()) == []

    def test_missing_temperature(self):
        assert check_row(make_row(temperature_c=None)) == ["missing temperature_c"]

    def test_optional_fields_may_be_null(self):
        assert check_row(make_row(relative_humidity_pct=None, wind_speed_kmh=None)) == []

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("temperature_c", 75.0),
            ("temperature_c", -120.0),
            ("relative_humidity_pct", 140),
            ("precipitation_mm", -1.0),
            ("wind_speed_kmh", 999.0),
        ],
    )
    def test_out_of_range_values(self, column, value):
        problems = check_row(make_row(**{column: value}))
        assert len(problems) == 1
        assert problems[0].startswith(f"{column}={value} outside")

    def test_non_numeric_value(self):
        problems = check_row(make_row(temperature_c="warm"))
        assert problems == ["temperature_c is not a number ('warm')"]

    def test_boolean_is_not_a_number(self):
        assert check_row(make_row(wind_speed_kmh=True)) == ["wind_speed_kmh is not a number (True)"]

    def test_multiple_problems_are_all_reported(self):
        problems = check_row(make_row(observed_at=None, raw_time="x", temperature_c=None))
        assert problems == ["invalid timestamp 'x'", "missing temperature_c"]


class TestValidateRows:
    def test_rejected_rows_carry_reason_and_json_payload(self):
        valid, rejected = validate_rows([make_row(), make_row(city="Paris", temperature_c=None)])
        assert len(valid) == 1
        assert "raw_time" not in valid[0]
        assert rejected[0]["city"] == "Paris"
        assert rejected[0]["reason"] == "missing temperature_c"
        assert rejected[0]["payload"]["observed_at"] == "2026-11-10T01:00:00+00:00"

    def test_duplicates_in_batch_are_rejected(self):
        valid, rejected = validate_rows([make_row(), make_row(temperature_c=11.0)])
        assert len(valid) == 1
        assert valid[0]["temperature_c"] == 10.0
        assert rejected[0]["reason"] == "duplicate city and hour in the same batch"


class TestTransformForecasts:
    def test_end_to_end_with_fixture(self, two_cities, two_city_payloads, now):
        result = transform_forecasts(two_cities, two_city_payloads, now=now)

        assert result.rows_extracted == 6
        assert [(r["city"], r["observed_at"].hour) for r in result.valid] == [
            ("London", 0),
            ("Tokyo", 0),
            ("Tokyo", 1),
        ]
        reasons = {(r["city"], r["observed_at"].hour): r["reason"] for r in result.rejected}
        assert reasons == {
            ("London", 1): "missing temperature_c",
            ("London", 2): "relative_humidity_pct=140 outside [0, 100]",
            ("Tokyo", 2): "temperature_c=75.0 outside [-90, 60]",
        }

    def test_payload_count_mismatch_raises(self, two_cities, two_city_payloads):
        with pytest.raises(ValueError, match="Expected 2 forecasts"):
            pair_payloads(two_cities, two_city_payloads[:1])
