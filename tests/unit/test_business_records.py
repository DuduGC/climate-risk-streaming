from __future__ import annotations

import copy
import importlib
import json
import math
from collections.abc import Callable
from typing import Any

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
    components: dict[str, int] | None = None,
) -> dict[str, object]:
    if components is None:
        components = {
            "precipitation_rate": score,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        }
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
        "risk_components": components,
        "risk_rules_version": "mvp-1",
    }


def _raw(assessment: dict[str, object]) -> str:
    return json.dumps(assessment, ensure_ascii=False, separators=(",", ":"))


@pytest.mark.unit
def test_valid_assessment_contract_has_exactly_21_fields_and_four_risk_classes():
    records = _records()
    expected_fields = frozenset(
        {
            "component",
            "operation",
            "schema_version",
            "location_id",
            "window_start_ms",
            "window_end_ms",
            "event_count",
            "temperature_avg",
            "precipitation_max",
            "precipitation_rate_max_mm_per_hour",
            "wind_speed_max",
            "wind_gust_max",
            "visibility_min",
            "measurement_interval_min_seconds",
            "measurement_interval_max_seconds",
            "first_event_timestamp_ms",
            "last_event_timestamp_ms",
            "risk_score",
            "risk_level",
            "risk_components",
            "risk_rules_version",
        }
    )
    cases = (
        (0.0, 0, "LOW"),
        (2.0, 25, "MEDIUM"),
        (20.0, 50, "HIGH"),
        (30.0, 75, "CRITICAL"),
        (60.0, 100, "CRITICAL"),
    )

    assert records.ASSESSMENT_FIELDS == expected_fields
    assert records.ALERT_FIELDS == expected_fields | {"alert_id"}
    for rate, score, level in cases:
        assessment = _assessment(
            rate=rate,
            score=score,
            level=level,
            components={
                "precipitation_rate": score,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        )
        assert records.parse_assessment(_raw(assessment)) == assessment


@pytest.mark.unit
def test_assessment_identity_and_alert_id_use_the_compact_utf8_list_hash():
    records = _records()
    assessment = _assessment()
    identity = records.assessment_identity(assessment)

    assert identity == ("RJ01", 1_791_633_600_000, 1_791_635_400_000, "mvp-1")
    expected_ascii_hash = "98590ad7f4adc9416e3ea34896b77cf103fccea8a4c5069c153d52adec32bbb1"
    assert records.alert_id(identity) == expected_ascii_hash
    unicode_identity = ("RJ🌧", 1_791_633_600_000, 1_791_635_400_000, "mvp-1")
    expected_unicode_hash = "b09de62a56ee5833720a7147fc3591fbde6c5e596020811f3d8b76b377e6a463"
    assert records.alert_id(unicode_identity) == expected_unicode_hash

    for index, replacement in enumerate(("SP01", 1_791_633_600_001, 1_791_635_400_001, "mvp-2")):
        changed = list(identity)
        changed[index] = replacement
        assert records.alert_id(tuple(changed)) != records.alert_id(identity)
    assert records.alert_id(tuple(identity)) == records.alert_id(identity)


@pytest.mark.unit
def test_alert_and_jdbc_payload_preserve_assessment_and_are_replay_deterministic():
    records = _records()
    assessment = _assessment(location_id="RJ🌧")
    original = copy.deepcopy(assessment)

    alert = records.make_alert(assessment)
    first_values = records.jdbc_values(assessment)
    second_values = records.jdbc_values(copy.deepcopy(assessment))

    expected_alert = copy.deepcopy(original)
    expected_alert["operation"] = "weather_alert"
    expected_alert["alert_id"] = "b09de62a56ee5833720a7147fc3591fbde6c5e596020811f3d8b76b377e6a463"
    assert alert == expected_alert
    assert set(alert) == records.ALERT_FIELDS
    assert first_values == second_values
    assert first_values[1] == expected_alert["alert_id"]
    assert json.loads(first_values[0]) == original
    assert json.dumps(
        json.loads(first_values[0]),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ) == first_values[0]
    assert assessment == original


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rate", "score", "level", "expected_alert"),
    [
        (0.0, 0, "LOW", False),
        (2.0, 25, "MEDIUM", False),
        (20.0, 50, "HIGH", True),
        (30.0, 75, "CRITICAL", True),
    ],
)
def test_only_valid_high_or_critical_assessments_create_alerts(
    rate: float, score: int, level: str, expected_alert: bool
):
    records = _records()
    assessment = _assessment(
        rate=rate,
        score=score,
        level=level,
        components={
            "precipitation_rate": score,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
    )

    alert = records.make_alert(assessment)
    serialized, alert_id = records.jdbc_values(assessment)

    assert (alert is not None) is expected_alert
    assert (alert_id is not None) is expected_alert
    assert json.loads(serialized) == assessment


@pytest.mark.unit
def test_score_49_is_rejected_and_boundary_50_is_a_high_alert():
    records = _records()
    inconsistent_49 = _assessment(
        score=49,
        level="MEDIUM",
        components={
            "precipitation_rate": 25,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
    )
    valid_50 = _assessment()

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(inconsistent_49))
    assert records.make_alert(valid_50)["risk_score"] == 50
    assert records.make_alert(valid_50)["risk_level"] == "HIGH"


@pytest.mark.unit
@pytest.mark.parametrize(
    "entry_point",
    ["parse_assessment", "assessment_identity", "make_alert", "jdbc_values"],
)
def test_negative_precipitation_max_is_rejected_with_other_high_inputs_valid(
    entry_point: str,
):
    records = _records()
    assessment = _assessment()
    assessment["precipitation_max"] = -1.0
    value: object = _raw(assessment) if entry_point == "parse_assessment" else assessment

    with pytest.raises(records.BusinessRecordError):
        getattr(records, entry_point)(value)


@pytest.mark.unit
def test_parser_rejects_ascii_escaped_surrogate_after_json_decoding():
    records = _records()
    raw = _raw(_assessment()).replace('"RJ01"', '"RJ\\ud800"', 1)

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(raw)


@pytest.mark.unit
@pytest.mark.parametrize("entry_point", ["assessment_identity", "make_alert", "jdbc_values"])
def test_mapping_entry_points_reject_invalid_utf8_identity_text(entry_point: str):
    records = _records()
    assessment = _assessment(location_id="RJ\ud800")

    with pytest.raises(records.BusinessRecordError):
        getattr(records, entry_point)(assessment)


@pytest.mark.unit
def test_parser_rejects_event_count_outside_postgresql_bigint():
    records = _records()
    assessment = _assessment()
    assessment["event_count"] = 2**63

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(assessment))


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(component="other"),
        lambda value: value.update(operation="weather_alert"),
        lambda value: value.update(operation="window_aggregate"),
        lambda value: value.update(schema_version=True),
        lambda value: value.update(schema_version=2),
        lambda value: value.update(risk_rules_version="mvp-2"),
        lambda value: value.update(location_id=" "),
        lambda value: value.update(event_count=0),
        lambda value: value.update(event_count=True),
        lambda value: value.update(event_count=3.0),
        lambda value: value.update(window_start_ms=True),
        lambda value: value.update(window_end_ms=1_791_633_600_000),
        lambda value: value.update(first_event_timestamp_ms=1_791_633_599_999),
        lambda value: value.update(last_event_timestamp_ms=1_791_635_400_000),
        lambda value: value.update(first_event_timestamp_ms=True),
        lambda value: value.update(last_event_timestamp_ms=1_791_633_619_999),
        lambda value: value.update(
            first_event_timestamp_ms=1_791_633_700_000,
            last_event_timestamp_ms=1_791_633_620_000,
        ),
        lambda value: value.update(measurement_interval_min_seconds=True),
        lambda value: value.update(measurement_interval_min_seconds=0),
        lambda value: value.update(measurement_interval_max_seconds=3601),
        lambda value: value.update(
            measurement_interval_min_seconds=901,
            measurement_interval_max_seconds=900,
        ),
    ],
)
def test_parser_rejects_invalid_envelope_counts_timestamps_and_intervals(
    mutate: Callable[[dict[str, object]], Any],
):
    records = _records()
    assessment = _assessment()
    mutate(assessment)

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(assessment))


@pytest.mark.unit
@pytest.mark.parametrize(
    "field",
    [
        "temperature_avg",
        "precipitation_max",
        "precipitation_rate_max_mm_per_hour",
        "wind_speed_max",
        "wind_gust_max",
        "visibility_min",
    ],
)
@pytest.mark.parametrize("invalid", [None, True, "12.5", math.nan, math.inf, -math.inf])
def test_parser_rejects_null_boolean_wrong_type_and_non_finite_measurements(
    field: str, invalid: object
):
    records = _records()
    assessment = _assessment()
    assessment[field] = invalid

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(assessment))


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.pop("wind_gust_max"),
        lambda value: value.update(unexpected=1),
        lambda value: value.update(risk_score=49),
        lambda value: value.update(risk_level="MEDIUM"),
        lambda value: value.update(
            risk_components={
                "precipitation_rate": 25,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            }
        ),
        lambda value: value.update(risk_components={"precipitation_rate": 50}),
        lambda value: value.update(risk_components={"precipitation_rate": True}),
        lambda value: value.update(wind_speed_max=60.0),
        lambda value: value.update(temperature_avg=math.nan),
    ],
)
def test_parser_rejects_extra_missing_and_inconsistent_risk_fields(
    mutate: Callable[[dict[str, object]], Any],
):
    records = _records()
    assessment = _assessment()
    mutate(assessment)

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(assessment))


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw",
    ["[]", "null", "NaN"],
)
def test_parser_rejects_non_object_and_non_json_values(raw: str):
    records = _records()

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(raw)


@pytest.mark.unit
def test_parser_rejects_duplicate_keys_and_json_numbers_that_overflow_float():
    records = _records()
    valid_raw = _raw(_assessment())
    duplicate_key = valid_raw.replace(
        '"location_id":"RJ01"',
        '"location_id":"RJ01","location_id":"RJ01"',
        1,
    )
    infinite_float = valid_raw.replace('"temperature_avg":22.5', '"temperature_avg":1e309', 1)

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(duplicate_key)
    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(infinite_float)


@pytest.mark.unit
def test_parser_enforces_utf8_byte_limit_and_rejects_surrogates():
    records = _records()
    oversized = _raw(_assessment()).replace('"RJ01"', '"' + ("é" * 8_200) + '"', 1)

    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(oversized)
    with pytest.raises(records.BusinessRecordError):
        records.parse_assessment(_raw(_assessment(location_id="\ud800")))


@pytest.mark.unit
def test_public_mapping_functions_revalidate_and_reject_alert_reentry():
    records = _records()
    assessment = _assessment()
    alert = records.make_alert(assessment)

    with pytest.raises(records.BusinessRecordError):
        records.assessment_identity({"location_id": "RJ01"})
    with pytest.raises(records.BusinessRecordError):
        records.jdbc_values(alert)
    with pytest.raises(records.BusinessRecordError):
        records.make_alert(alert)
    with pytest.raises(records.BusinessRecordError):
        records.make_alert({"not": "an assessment"})
