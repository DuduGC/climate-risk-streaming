from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parents[1] / "fixtures" / "open_meteo_current.json"
INGESTION = datetime(2026, 10, 7, 13, 20, 2, tzinfo=UTC)


def _api(name):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"{name} ainda não foi implementado", pytrace=False)


def _payload():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _normalize(payload, ingestion=INGESTION):
    locations = _api("climate_logistics.locations")
    normalizer = _api("climate_logistics.open_meteo")
    return normalizer.normalize_current(
        payload, locations.LOCATIONS[0], ingestion_timestamp=ingestion
    )


def test_normalize_current_maps_fields_and_uses_configured_coordinates():
    event = _normalize(_payload())
    assert (event.location_id, event.city) == ("RJ01", "Rio de Janeiro")
    assert (event.latitude, event.longitude) == (-22.9, -43.17)
    assert (event.temperature, event.precipitation) == (27.4, 18.2)
    assert (event.wind_speed, event.wind_gust, event.visibility) == (42.1, 58.3, 7000.0)
    assert event.weather_code == 61
    assert event.measurement_interval_seconds == 900
    assert event.event_timestamp == datetime(2026, 10, 7, 13, 20, tzinfo=UTC)
    assert event.ingestion_timestamp == INGESTION
    assert event.event_timestamp != event.ingestion_timestamp
    assert (event.source, event.schema_version) == ("open-meteo", 1)


@pytest.mark.parametrize(("section", "key"), [
    ("current", "time"), ("current", "interval"),
    ("current", "temperature_2m"), ("current", "precipitation"),
    ("current", "wind_speed_10m"), ("current", "wind_gusts_10m"),
    ("current", "visibility"), ("current", "weather_code"),
    ("current_units", "time"), ("current_units", "interval"),
    ("current_units", "temperature_2m"), ("current_units", "precipitation"),
    ("current_units", "wind_speed_10m"), ("current_units", "wind_gusts_10m"),
    ("current_units", "visibility"), ("current_units", "weather_code"),
])
def test_normalize_current_rejects_missing_fields(section, key):
    models = _api("climate_logistics.models")
    payload = _payload()
    del payload[section][key]
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


@pytest.mark.parametrize(("field", "value"), [
    ("temperature_2m", None), ("temperature_2m", True),
    ("temperature_2m", "27.4"), ("temperature_2m", float("nan")),
    ("temperature_2m", float("inf")), ("precipitation", None),
    ("precipitation", False), ("precipitation", "18.2"),
    ("precipitation", float("nan")), ("precipitation", float("inf")),
    ("wind_speed_10m", None), ("wind_speed_10m", True),
    ("wind_speed_10m", "42.1"), ("wind_speed_10m", float("nan")),
    ("wind_speed_10m", float("inf")), ("wind_gusts_10m", None),
    ("wind_gusts_10m", False), ("wind_gusts_10m", "58.3"),
    ("wind_gusts_10m", float("nan")), ("wind_gusts_10m", float("inf")),
    ("visibility", None), ("visibility", True), ("visibility", "7000"),
    ("visibility", float("nan")), ("visibility", float("inf")),
    ("weather_code", None), ("weather_code", True), ("weather_code", "61"),
    ("weather_code", float("nan")), ("weather_code", float("inf")),
])
def test_normalize_current_rejects_non_numeric_or_non_finite_values(field, value):
    models = _api("climate_logistics.models")
    payload = _payload()
    payload["current"][field] = value
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


@pytest.mark.parametrize(("field", "value"), [
    ("temperature_2m", -100.1), ("temperature_2m", 100.1),
    ("precipitation", -0.1), ("precipitation", 1000.1),
    ("wind_speed_10m", -0.1), ("wind_speed_10m", 500.1),
    ("wind_gusts_10m", -0.1), ("wind_gusts_10m", 500.1),
    ("visibility", -0.1), ("visibility", 1_000_000.1),
    ("weather_code", 4), ("weather_code", 61.5),
])
def test_normalize_current_rejects_out_of_contract_values(field, value):
    models = _api("climate_logistics.models")
    payload = _payload()
    payload["current"][field] = value
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


@pytest.mark.parametrize(("field", "bounds"), [
    ("temperature_2m", (-100, 100)), ("precipitation", (0, 1000)),
    ("wind_speed_10m", (0, 500)), ("wind_gusts_10m", (0, 500)),
    ("visibility", (0, 1_000_000)),
])
def test_normalize_current_accepts_inclusive_bounds(field, bounds):
    normalized_name = {
        "temperature_2m": "temperature", "precipitation": "precipitation",
        "wind_speed_10m": "wind_speed", "wind_gusts_10m": "wind_gust",
        "visibility": "visibility",
    }[field]
    for value in bounds:
        payload = _payload()
        payload["current"][field] = value
        assert getattr(_normalize(payload), normalized_name) == value


@pytest.mark.parametrize("weather_code", [
    0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
    71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99,
])
def test_normalize_current_accepts_documented_wmo_codes(weather_code):
    payload = _payload()
    payload["current"]["weather_code"] = weather_code
    assert _normalize(payload).weather_code == weather_code


@pytest.mark.parametrize(("field", "unit"), [
    ("time", "unixtime"), ("interval", "seconds"), ("temperature_2m", "°C"),
    ("precipitation", "mm"), ("wind_speed_10m", "km/h"),
    ("wind_gusts_10m", "km/h"), ("visibility", "m"),
    ("weather_code", "wmo code"),
])
def test_normalize_current_rejects_incompatible_units(field, unit):
    models = _api("climate_logistics.models")
    payload = _payload()
    payload["current_units"][field] = "wrong unit"
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


@pytest.mark.parametrize("interval", [0, 3601, True, 900.5, None, "900"])
def test_normalize_current_rejects_invalid_interval(interval):
    models = _api("climate_logistics.models")
    payload = _payload()
    payload["current"]["interval"] = interval
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


@pytest.mark.parametrize(
    "timestamp",
    [True, None, float("nan"), float("inf"), "1791379200", 10**20],
)
def test_normalize_current_rejects_invalid_unix_timestamp(timestamp):
    models = _api("climate_logistics.models")
    payload = _payload()
    payload["current"]["time"] = timestamp
    with pytest.raises(models.WeatherValidationError):
        _normalize(payload)


def test_normalize_current_rejects_naive_ingestion_timestamp():
    normalizer = _api("climate_logistics.open_meteo")
    models = _api("climate_logistics.models")
    locations = _api("climate_logistics.locations")
    with pytest.raises(models.WeatherValidationError):
        normalizer.normalize_current(
            _payload(), locations.LOCATIONS[0],
            ingestion_timestamp=datetime(2026, 10, 7, 13, 20, 2),
        )
