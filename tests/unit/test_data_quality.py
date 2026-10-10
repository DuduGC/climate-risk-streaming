from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone, tzinfo
from importlib import import_module

import pytest

pytestmark = pytest.mark.unit

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _quality():
    try:
        return import_module("climate_logistics.data_quality")
    except ModuleNotFoundError:
        pytest.fail("climate_logistics.data_quality ainda não foi implementado", pytrace=False)


def _event(**changes: object):
    models = import_module("climate_logistics.models")
    fields = {
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
        "event_timestamp": datetime(2026, 10, 9, 12, tzinfo=UTC),
        "ingestion_timestamp": datetime(2026, 10, 9, 12, tzinfo=UTC),
    }
    fields.update(changes)
    return models.WeatherEvent(**fields)


def test_temporal_policy_accepts_all_inclusive_limits():
    quality = _quality()
    ingestion = EPOCH + timedelta(hours=24)
    event = _event(event_timestamp=EPOCH, ingestion_timestamp=ingestion)

    quality.validate_event_time(
        event,
        observed_at=ingestion - timedelta(minutes=5),
    )


def test_temporal_policy_accepts_event_five_minutes_after_ingestion():
    quality = _quality()
    ingestion = datetime(2026, 10, 9, 12, tzinfo=UTC)
    event = _event(
        event_timestamp=ingestion + timedelta(minutes=5),
        ingestion_timestamp=ingestion,
    )

    quality.validate_event_time(event, observed_at=ingestion + timedelta(minutes=5))


def test_temporal_policy_accepts_ingestion_five_minutes_after_observed_clock():
    quality = _quality()
    observed = datetime(2026, 10, 9, 12, tzinfo=UTC)
    ingestion = observed + timedelta(minutes=5)
    event = _event(event_timestamp=ingestion, ingestion_timestamp=ingestion)

    quality.validate_event_time(event, observed_at=observed)


@pytest.mark.parametrize(
    ("event_time", "ingestion_time", "observed_at", "reason"),
    [
        (
            EPOCH - timedelta(microseconds=1),
            EPOCH,
            EPOCH + timedelta(days=2),
            "timestamp_before_epoch",
        ),
        (
            datetime(2026, 10, 9, 12, tzinfo=UTC) + timedelta(minutes=5, microseconds=1),
            datetime(2026, 10, 9, 12, tzinfo=UTC),
            datetime(2026, 10, 9, 12, 5, tzinfo=UTC),
            "event_after_ingestion",
        ),
        (
            datetime(2026, 10, 9, 12, tzinfo=UTC),
            datetime(2026, 10, 9, 12, tzinfo=UTC),
            datetime(2026, 10, 9, 11, 54, 59, 999999, tzinfo=UTC),
            "ingestion_in_future",
        ),
        (
            EPOCH,
            EPOCH + timedelta(hours=24, microseconds=1),
            EPOCH + timedelta(days=2),
            "excessive_ingestion_delay",
        ),
    ],
    ids=["before-epoch", "event-too-far-ahead", "ingestion-too-far-ahead", "delay-too-long"],
)
def test_temporal_policy_reports_the_controlled_reason_at_one_microsecond_over_limit(
    event_time: datetime,
    ingestion_time: datetime,
    observed_at: datetime,
    reason: str,
):
    quality = _quality()
    event = _event(event_timestamp=event_time, ingestion_timestamp=ingestion_time)
    before = event.to_dict()

    with pytest.raises(quality.DataQualityError) as exc_info:
        quality.validate_event_time(event, observed_at=observed_at)

    assert exc_info.value.reason == reason
    assert event.to_dict() == before


def test_temporal_policy_uses_first_applicable_reason_in_documented_order():
    quality = _quality()
    event = _event(
        event_timestamp=EPOCH - timedelta(microseconds=1),
        ingestion_timestamp=EPOCH + timedelta(hours=25),
    )

    with pytest.raises(quality.DataQualityError) as exc_info:
        quality.validate_event_time(event, observed_at=EPOCH + timedelta(days=2))

    assert exc_info.value.reason == "timestamp_before_epoch"


def test_temporal_policy_controls_maximum_utc_timestamps_without_overflow():
    quality = _quality()
    latest = datetime.max.replace(tzinfo=UTC)
    event = _event(event_timestamp=latest, ingestion_timestamp=latest)

    with pytest.raises(quality.DataQualityError) as exc_info:
        quality.validate_event_time(event, observed_at=datetime(2026, 10, 9, tzinfo=UTC))

    assert exc_info.value.reason == "ingestion_in_future"


def test_temporal_policy_accepts_historical_replay_using_original_ingestion_time():
    quality = _quality()
    event_time = datetime(2020, 1, 1, 12, tzinfo=UTC)
    event = _event(
        event_timestamp=event_time,
        ingestion_timestamp=event_time + timedelta(hours=24),
    )

    quality.validate_event_time(event, observed_at=datetime(2026, 10, 9, tzinfo=UTC))


@pytest.mark.parametrize(
    "observed_at",
    [
        datetime(2026, 10, 9, 12),
        "2026-10-09T12:00:00Z",
        datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))),
    ],
    ids=["naive", "wrong-type", "cannot-convert-to-utc"],
)
def test_invalid_observed_clock_is_configuration_error_not_bad_event(observed_at: object):
    quality = _quality()

    with pytest.raises(ValueError) as exc_info:
        quality.validate_event_time(_event(), observed_at=observed_at)

    assert not isinstance(exc_info.value, quality.DataQualityError)


class _InvalidOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta:
        return timedelta(hours=25)

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


def test_unconvertible_observed_clock_timezone_is_configuration_error():
    quality = _quality()
    observed_at = datetime(2026, 10, 9, 12, tzinfo=_InvalidOffset())

    with pytest.raises(ValueError) as exc_info:
        quality.validate_event_time(_event(), observed_at=observed_at)

    assert not isinstance(exc_info.value, quality.DataQualityError)


def test_event_identity_normalizes_offsets_and_ignores_ingestion_and_payload_revision():
    quality = _quality()
    event_time = datetime(2026, 10, 9, 12, 30, tzinfo=UTC)
    original = _event(
        event_timestamp=event_time,
        ingestion_timestamp=event_time + timedelta(seconds=10),
    )
    revised = _event(
        city="Rio revisto",
        temperature=19.0,
        event_timestamp=event_time.astimezone(timezone(timedelta(hours=2))),
        ingestion_timestamp=event_time + timedelta(minutes=12),
    )

    identity = quality.event_identity(original)

    assert identity == ("RJ01", "2026-10-09T12:30:00Z")
    assert quality.event_identity(revised) == identity
    assert (
        quality.event_identity(_event(location_id="SP01", event_timestamp=event_time))
        != identity
    )
    assert (
        quality.event_identity(_event(event_timestamp=event_time + timedelta(seconds=1)))
        != identity
    )
