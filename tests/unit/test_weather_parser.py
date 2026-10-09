from __future__ import annotations

import importlib
import json
from datetime import UTC

import pytest

pytestmark = pytest.mark.unit

VALID_EVENT = (
    '{"location_id":"RJ01","city":"Rio de Janeiro",'
    '"latitude":-22.9,"longitude":-43.17,"temperature":27.4,'
    '"precipitation":18.2,"wind_speed":42.1,"wind_gust":58.3,'
    '"visibility":7000,"weather_code":61,"measurement_interval_seconds":900,'
    '"event_timestamp":"2026-10-07T15:20:00+02:00",'
    '"ingestion_timestamp":"2026-10-07T08:20:02-05:00",'
    '"source":"open-meteo","schema_version":1}'
)

EXPECTED_FIELDS = (
    "location_id",
    "city",
    "latitude",
    "longitude",
    "temperature",
    "precipitation",
    "wind_speed",
    "wind_gust",
    "visibility",
    "weather_code",
    "measurement_interval_seconds",
    "event_timestamp",
    "ingestion_timestamp",
    "source",
    "schema_version",
)


def _parser():
    try:
        return importlib.import_module("climate_logistics.weather_parser")
    except ModuleNotFoundError:
        pytest.fail("climate_logistics.weather_parser ainda não foi implementado", pytrace=False)


def _wire_event(**changes: object) -> str:
    event = json.loads(VALID_EVENT)
    event.update(changes)
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False)


def test_parse_weather_event_normalizes_and_preserves_the_full_schema1_contract():
    parser = _parser()

    event = parser.parse_weather_event(VALID_EVENT)

    assert event.to_dict() == {
        "location_id": "RJ01",
        "city": "Rio de Janeiro",
        "latitude": -22.9,
        "longitude": -43.17,
        "temperature": 27.4,
        "precipitation": 18.2,
        "wind_speed": 42.1,
        "wind_gust": 58.3,
        "visibility": 7000.0,
        "weather_code": 61,
        "measurement_interval_seconds": 900,
        "event_timestamp": "2026-10-07T13:20:00Z",
        "ingestion_timestamp": "2026-10-07T13:20:02Z",
        "source": "open-meteo",
        "schema_version": 1,
    }
    assert event.event_timestamp.tzinfo is UTC
    assert event.ingestion_timestamp.tzinfo is UTC
    assert type(event.latitude) is float
    assert type(event.weather_code) is int
    assert type(event.measurement_interval_seconds) is int


def test_event_values_uses_the_explicit_flink_row_order_and_utc_strings():
    parser = _parser()
    event = parser.parse_weather_event(VALID_EVENT)

    assert parser.WEATHER_FIELDS == EXPECTED_FIELDS
    assert parser.event_values(event) == (
        "RJ01",
        "Rio de Janeiro",
        -22.9,
        -43.17,
        27.4,
        18.2,
        42.1,
        58.3,
        7000.0,
        61,
        900,
        "2026-10-07T13:20:00Z",
        "2026-10-07T13:20:02Z",
        "open-meteo",
        1,
    )
    assert parser.WEATHER_KINDS == (
        "STRING",
        "STRING",
        "DOUBLE",
        "DOUBLE",
        "DOUBLE",
        "DOUBLE",
        "DOUBLE",
        "DOUBLE",
        "DOUBLE",
        "INT",
        "INT",
        "STRING",
        "STRING",
        "STRING",
        "INT",
    )


def test_diagnostic_summary_contains_only_the_compact_parse_evidence():
    parser = _parser()
    values = parser.event_values(parser.parse_weather_event(VALID_EVENT))

    assert parser.diagnostic_summary(values) == (
        '{"component":"flink","operation":"parse","status":"success",'
        '"location_id":"RJ01","event_timestamp":"2026-10-07T13:20:00Z",'
        '"ingestion_timestamp":"2026-10-07T13:20:02Z","schema_version":1}'
    )


@pytest.mark.parametrize(
    "raw",
    [
        "{\"location_id\":",
        "[]",
        _wire_event(schema_version=2),
        _wire_event(source="other-source"),
        _wire_event(weather_code=999),
        _wire_event(event_timestamp="2026-10-07T13:20:00"),
        json.dumps(
            {key: value for key, value in json.loads(VALID_EVENT).items() if key != "source"},
            separators=(",", ":"),
        ),
    ],
    ids=[
        "invalid-json", "non-object", "schema-version", "source", "wmo-code",
        "naive-time", "missing-field",
    ],
)
def test_known_wire_contract_errors_are_controlled_and_do_not_leak_the_payload(raw: str):
    parser = _parser()

    with pytest.raises(parser.WeatherParseError) as exc_info:
        parser.parse_weather_event(raw)

    assert raw not in str(exc_info.value)


def test_parser_rejects_messages_over_16_kibibytes_before_model_construction():
    parser = _parser()
    raw = _wire_event(city="x" * 17_000)

    with pytest.raises(parser.WeatherParseError) as exc_info:
        parser.parse_weather_event(raw)

    assert "16384" in str(exc_info.value)
    assert "x" * 32 not in str(exc_info.value)


def test_parser_controls_json_integer_digit_limit_errors_without_echoing_payload():
    parser = _parser()
    raw = VALID_EVENT.replace('"weather_code":61', '"weather_code":' + "9" * 5_000)

    with pytest.raises(parser.WeatherParseError) as exc_info:
        parser.parse_weather_event(raw)

    assert raw not in str(exc_info.value)


def test_parser_controls_excessive_json_nesting_without_echoing_payload():
    parser = _parser()
    raw = '{"nested":' + "[" * 2_000 + "0" + "]" * 2_000 + "}"

    with pytest.raises(parser.WeatherParseError) as exc_info:
        parser.parse_weather_event(raw)

    assert "nested" not in str(exc_info.value) or "excessive" in str(exc_info.value)
    assert raw not in str(exc_info.value)


def test_parser_controls_strings_that_cannot_be_encoded_as_utf8():
    parser = _parser()
    raw = _wire_event(city="bad" + chr(0xD800))

    with pytest.raises(parser.WeatherParseError) as exc_info:
        parser.parse_weather_event(raw)

    assert chr(0xD800) not in str(exc_info.value)


def test_parser_does_not_convert_unexpected_programming_errors_to_bad_data(monkeypatch):
    parser = _parser()

    def broken_constructor(**kwargs: object) -> object:
        raise RuntimeError("programmer bug")

    monkeypatch.setattr(parser, "WeatherEvent", broken_constructor)

    with pytest.raises(RuntimeError, match="programmer bug"):
        parser.parse_weather_event(VALID_EVENT)
@pytest.mark.parametrize("field", ["city", "location_id"])
def test_parser_rejects_json_escaped_surrogates_before_worker_serialization(field: str):
    parser = _parser()
    payload = json.loads(VALID_EVENT)
    payload[field] = chr(0xD800)
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)

    assert r"\ud800" in raw
    with pytest.raises(parser.WeatherParseError):
        parser.parse_weather_event(raw)