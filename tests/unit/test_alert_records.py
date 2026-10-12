from __future__ import annotations

import copy
import hashlib
import importlib
import json

import pytest


def _records():
    try:
        return importlib.import_module("climate_logistics.business_records")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.business_records":
            pytest.fail("the business record contract has not been implemented yet", pytrace=False)
        raise


def _assessment(
    *,
    location_id: str = "RJ01",
    rate: float = 20.0,
    score: int = 50,
    level: str = "HIGH",
) -> dict[str, object]:
    return {
        "component": "flink",
        "operation": "risk_assessment",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": 1_791_633_600_000,
        "window_end_ms": 1_791_635_400_000,
        "event_count": 3,
        "temperature_avg": 22.5,
        "precipitation_max": 5.0,
        "precipitation_rate_max_mm_per_hour": rate,
        "wind_speed_max": 0.0,
        "wind_gust_max": 0.0,
        "visibility_min": 10_000.0,
        "measurement_interval_min_seconds": 300,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_620_000,
        "last_event_timestamp_ms": 1_791_634_900_000,
        "risk_score": score,
        "risk_level": level,
        "risk_components": {
            "precipitation_rate": score,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
        "risk_rules_version": "mvp-1",
    }


def _alert(*, location_id: str = "RJ01") -> dict[str, object]:
    records = _records()
    alert = records.make_alert(_assessment(location_id=location_id))
    assert alert is not None
    return alert


def _raw(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rate", "score", "level"),
    [(20.0, 50, "HIGH"), (30.0, 75, "CRITICAL")],
)
def test_parse_alert_accepts_only_valid_high_and_critical_contracts(
    rate: float, score: int, level: str
):
    records = _records()
    assessment = _assessment(rate=rate, score=score, level=level)
    expected = copy.deepcopy(assessment)
    expected["operation"] = "weather_alert"
    expected["alert_id"] = "98590ad7f4adc9416e3ea34896b77cf103fccea8a4c5069c153d52adec32bbb1"

    assert records.parse_alert(_raw(expected)) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(operation="risk_assessment"),
        lambda value: value.update(component="producer"),
        lambda value: value.update(schema_version=True),
        lambda value: value.update(schema_version=2),
        lambda value: value.update(risk_rules_version="mvp-2"),
        lambda value: value.update(alert_id="0" * 64),
        lambda value: value.update(risk_score=75),
        lambda value: value.pop("alert_id"),
        lambda value: value.update(unexpected="field"),
    ],
)
def test_parse_alert_rejects_invalid_or_inconsistent_contracts(mutate):
    records = _records()
    alert = _alert()
    mutate(alert)

    with pytest.raises(records.BusinessRecordError):
        records.parse_alert(_raw(alert))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rate", "score", "level"),
    [(0.0, 0, "LOW"), (2.0, 25, "MEDIUM")],
)
def test_parse_alert_rejects_alerts_below_high_even_with_a_forged_id(
    rate: float, score: int, level: str
):
    records = _records()
    assessment = _assessment(rate=rate, score=score, level=level)
    assessment["operation"] = "weather_alert"
    assessment["alert_id"] = "98590ad7f4adc9416e3ea34896b77cf103fccea8a4c5069c153d52adec32bbb1"

    with pytest.raises(records.BusinessRecordError):
        records.parse_alert(_raw(assessment))


@pytest.mark.unit
def test_parse_alert_rejects_duplicate_keys_bad_utf8_and_values_over_16_kib():
    records = _records()
    valid = _raw(_alert())
    duplicate_key = valid.replace(
        '"location_id":"RJ01"', '"location_id":"RJ01","location_id":"RJ01"'
    )
    bad_utf8 = valid.replace('"location_id":"RJ01"', '"location_id":"RJ\\ud800"')
    oversized = valid + (" " * (16_385 - len(valid)))
    too_deep = "[" * 2_000 + "]" * 2_000

    for raw in (duplicate_key, bad_utf8, oversized, too_deep, b"{}"):
        with pytest.raises(records.BusinessRecordError):
            records.parse_alert(raw)


@pytest.mark.unit
def test_alert_fingerprint_normalizes_measurement_numbers_and_json_key_order():
    records = _records()
    alert = _alert()
    alternate = dict(reversed(tuple(alert.items())))
    alternate["precipitation_rate_max_mm_per_hour"] = 20

    expected_normalized = dict(alternate)
    for field in (
        "temperature_avg",
        "precipitation_max",
        "precipitation_rate_max_mm_per_hour",
        "wind_speed_max",
        "wind_gust_max",
        "visibility_min",
    ):
        expected_normalized[field] = float(expected_normalized[field])
    expected = hashlib.sha256(
        json.dumps(
            expected_normalized,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    assert records.alert_fingerprint(alert) == expected
    assert records.alert_fingerprint(alternate) == expected
    assert type(expected_normalized["schema_version"]) is int
    assert all(type(value) is int for value in expected_normalized["risk_components"].values())


@pytest.mark.unit
def test_alert_fingerprint_changes_for_a_real_payload_difference():
    records = _records()
    first = _alert()
    changed = copy.deepcopy(first)
    changed["temperature_avg"] = 23.0

    assert records.alert_fingerprint(first) != records.alert_fingerprint(changed)


@pytest.mark.unit
def test_alert_id_cache_classifies_duplicates_and_divergence_without_overwriting():
    records = _records()
    cache = records.AlertIdCache()
    first = _alert()
    changed = copy.deepcopy(first)
    changed["temperature_avg"] = 23.0

    assert cache.classify(first) == "new"
    cache.remember(first)
    assert cache.classify(first) == "duplicate"
    assert cache.classify(changed) == "divergent"
    cache.remember(changed)
    assert cache.classify(first) == "duplicate"
    assert cache.classify(changed) == "divergent"


@pytest.mark.unit
def test_alert_id_cache_evicts_oldest_inserted_id_without_refreshing_duplicates():
    records = _records()
    cache = records.AlertIdCache(capacity=2)
    first, second, third = (_alert(location_id=value) for value in ("RJ01", "SP01", "CT01"))

    cache.remember(first)
    cache.remember(second)
    assert cache.classify(first) == "duplicate"
    cache.remember(third)

    assert cache.classify(first) == "new"
    assert cache.classify(second) == "duplicate"
    assert cache.classify(third) == "duplicate"


@pytest.mark.unit
@pytest.mark.parametrize("capacity", [0, -1, True, 1.5, None])
def test_alert_id_cache_requires_a_positive_non_boolean_integer(capacity: object):
    records = _records()

    with pytest.raises(ValueError):
        records.AlertIdCache(capacity=capacity)
