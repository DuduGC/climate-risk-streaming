from __future__ import annotations

import importlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.unit


def _models():
    try:
        return importlib.import_module("climate_logistics.models")
    except ModuleNotFoundError:
        pytest.fail("climate_logistics.models ainda não foi implementado", pytrace=False)


def _event(**changes):
    fields = {
        "location_id": "RJ01", "city": "Rio de Janeiro", "latitude": -22.9,
        "longitude": -43.17, "temperature": 27.4, "precipitation": 18.2,
        "wind_speed": 42.1, "wind_gust": 58.3, "visibility": 7000.0,
        "weather_code": 61,
        "event_timestamp": datetime(2026, 10, 7, 13, 20, tzinfo=UTC),
        "ingestion_timestamp": datetime(2026, 10, 7, 13, 20, 2, tzinfo=UTC),
        "measurement_interval_seconds": 900,
    }
    fields.update(changes)
    return _models().WeatherEvent(**fields)


def test_weather_event_serializes_all_fields_as_json_safe_utc():
    result = _event().to_dict()
    assert result == {
        "location_id": "RJ01", "city": "Rio de Janeiro", "latitude": -22.9,
        "longitude": -43.17, "temperature": 27.4, "precipitation": 18.2,
        "wind_speed": 42.1, "wind_gust": 58.3, "visibility": 7000.0,
        "weather_code": 61, "event_timestamp": "2026-10-07T13:20:00Z",
        "ingestion_timestamp": "2026-10-07T13:20:02Z", "source": "open-meteo",
        "schema_version": 1, "measurement_interval_seconds": 900,
    }
    json.dumps(result, allow_nan=False)


def test_weather_event_converts_aware_offsets_to_utc_z():
    event = _event(
        event_timestamp=datetime(2026, 10, 7, 15, 20, tzinfo=timezone(timedelta(hours=2))),
        ingestion_timestamp=datetime(2026, 10, 7, 8, 20, 2, tzinfo=timezone(timedelta(hours=-5))),
    )
    assert event.to_dict()["event_timestamp"] == "2026-10-07T13:20:00Z"
    assert event.to_dict()["ingestion_timestamp"] == "2026-10-07T13:20:02Z"


@pytest.mark.parametrize("field", ["event_timestamp", "ingestion_timestamp"])
def test_weather_event_rejects_naive_datetimes(field):
    models = _models()
    with pytest.raises(models.WeatherValidationError):
        _event(**{field: datetime(2026, 10, 7, 13, 20)})


def test_weather_event_is_frozen():
    event = _event()
    with pytest.raises(FrozenInstanceError):
        event.temperature = 0


@pytest.mark.parametrize(("field", "value"), [
    ("location_id", ""), ("location_id", " "), ("location_id", 1),
    ("city", ""), ("city", " "), ("city", None), ("city", 1),
    ("latitude", True), ("latitude", "-22.9"), ("latitude", float("nan")),
    ("latitude", float("inf")), ("latitude", -90.1), ("latitude", 90.1),
    ("longitude", False), ("longitude", "-43.17"), ("longitude", float("nan")),
    ("longitude", float("-inf")), ("longitude", -180.1), ("longitude", 180.1),
    ("temperature", True), ("temperature", "27.4"), ("temperature", float("nan")),
    ("temperature", float("inf")), ("temperature", -100.1), ("temperature", 100.1),
    ("precipitation", False), ("precipitation", "18.2"), ("precipitation", float("nan")),
    ("precipitation", float("-inf")), ("precipitation", -0.1), ("precipitation", 1000.1),
    ("wind_speed", True), ("wind_speed", "42.1"), ("wind_speed", float("nan")),
    ("wind_speed", float("inf")), ("wind_speed", -0.1), ("wind_speed", 500.1),
    ("wind_gust", False), ("wind_gust", "58.3"), ("wind_gust", float("nan")),
    ("wind_gust", float("-inf")), ("wind_gust", -0.1), ("wind_gust", 500.1),
    ("visibility", True), ("visibility", "7000"), ("visibility", float("nan")),
    ("visibility", float("inf")), ("visibility", -0.1), ("visibility", 1_000_000.1),
    ("weather_code", True), ("weather_code", 4), ("weather_code", 61.5),
    ("weather_code", "61"), ("measurement_interval_seconds", True),
    ("measurement_interval_seconds", 0), ("measurement_interval_seconds", 3601),
    ("measurement_interval_seconds", 900.5), ("source", "other-source"),
    ("schema_version", True), ("schema_version", 2),
])
def test_weather_event_constructor_rejects_invalid_values(field, value):
    models = _models()
    with pytest.raises(models.WeatherValidationError):
        _event(**{field: value})


@pytest.mark.parametrize(("field", "value"), [
    ("temperature", -100), ("temperature", 100), ("precipitation", 0),
    ("precipitation", 1000), ("wind_speed", 0), ("wind_speed", 500),
    ("wind_gust", 0), ("wind_gust", 500), ("visibility", 0),
    ("visibility", 1_000_000), ("measurement_interval_seconds", 1),
    ("measurement_interval_seconds", 3600),
])
def test_weather_event_accepts_inclusive_bounds(field, value):
    assert getattr(_event(**{field: value}), field) == value


@pytest.mark.parametrize("weather_code", [
    0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
    71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99,
])
def test_weather_event_accepts_documented_wmo_codes(weather_code):
    assert _event(weather_code=weather_code).weather_code == weather_code


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "event_timestamp",
            datetime.min.replace(tzinfo=timezone(timedelta(hours=1))),
        ),
        (
            "event_timestamp",
            datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))),
        ),
        (
            "ingestion_timestamp",
            datetime.min.replace(tzinfo=timezone(timedelta(hours=1))),
        ),
        (
            "ingestion_timestamp",
            datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))),
        ),
    ],
)
def test_weather_event_rejects_aware_datetimes_outside_utc_domain(field, value):
    models = _models()
    with pytest.raises(models.WeatherValidationError):
        _event(**{field: value})


def test_weather_event_accepts_datetime_bounds_in_utc():
    event = _event(
        event_timestamp=datetime.min.replace(tzinfo=UTC),
        ingestion_timestamp=datetime.max.replace(tzinfo=UTC),
    )

    result = event.to_dict()

    assert result["event_timestamp"] == "0001-01-01T00:00:00Z"
    assert result["ingestion_timestamp"] == "9999-12-31T23:59:59.999999Z"
