"""Per-message validation, logging, and offset confirmation for weather alerts."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, TextIO

from climate_logistics.business_records import AlertIdCache, BusinessRecordError, parse_alert
from climate_logistics.locations import LOCATIONS

ALERTS_TOPIC = "weather.alerts"
_KNOWN_LOCATION_IDS = frozenset(location.location_id for location in LOCATIONS)


class AlertMessageError(RuntimeError):
    """Raised when the Kafka message cannot be safely handled as an alert."""


class AlertCommitError(RuntimeError):
    """Raised when Kafka does not confirm the exact message offset commit."""


def emit_record(record: Mapping[str, object], stream: TextIO | None = None) -> None:
    """Write one compact JSON log line and flush the selected text stream."""
    target = sys.stdout if stream is None else stream
    document = dict(record)
    document.pop("timestamp", None)
    document = {"timestamp": _utc_timestamp(), **document}
    line = json.dumps(
        document,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    target.write(line + "\n")
    target.flush()


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _message_context(message: Any) -> dict[str, object]:
    try:
        if message.error() is not None:
            raise AlertMessageError("Kafka returned an errored alert message")
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
    except AlertMessageError:
        raise
    except Exception:
        raise AlertMessageError("Kafka alert message metadata is unavailable") from None

    if (
        topic != ALERTS_TOPIC
        or type(partition) is not int
        or partition < 0
        or type(offset) is not int
        or offset < 0
    ):
        raise AlertMessageError("Kafka alert message metadata is invalid")
    return {"topic": ALERTS_TOPIC, "partition": partition, "offset": offset}


def _commit_safely(commit: Callable[[Any], None], message: Any) -> None:
    try:
        commit(message)
    except Exception:
        raise AlertCommitError("alert commit failed") from None


def handle_message(
    message: Any,
    *,
    cache: AlertIdCache,
    emit: Callable[[Mapping[str, object]], None],
    commit: Callable[[Any], None],
) -> str:
    """Validate and emit one Kafka alert before confirming its offset."""
    context = _message_context(message)

    def invalid(reason: str, *, alert: Mapping[str, object] | None = None) -> str:
        record: dict[str, object] = {
            "timestamp": _utc_timestamp(),
            "component": "alert_consumer",
            "operation": "alert_invalid",
            "status": "error",
            **context,
            "reason": reason,
        }
        if alert is not None:
            record["alert"] = dict(alert)
        emit(record)
        _commit_safely(commit, message)
        return "invalid"

    try:
        raw_key = message.key()
    except Exception:
        return invalid("invalid_key")
    if not isinstance(raw_key, bytes):
        return invalid("invalid_key")
    try:
        key = raw_key.decode("utf-8", errors="strict")
    except Exception:
        return invalid("invalid_key")
    if not key:
        return invalid("invalid_key")

    try:
        raw_value = message.value()
    except Exception:
        return invalid("invalid_alert")
    if not isinstance(raw_value, bytes):
        return invalid("invalid_alert")
    try:
        body = raw_value.decode("utf-8", errors="strict")
    except Exception:
        return invalid("invalid_alert")
    try:
        alert = parse_alert(body)
    except BusinessRecordError:
        return invalid("invalid_alert")

    if key not in _KNOWN_LOCATION_IDS or key != alert["location_id"]:
        return invalid("alert_key_mismatch")

    classification = cache.classify(alert)
    if classification == "new":
        emit(
            {
                "timestamp": _utc_timestamp(),
                "component": "alert_consumer",
                "operation": "alert_received",
                "status": "success",
                **context,
                "alert": alert,
            }
        )
        _commit_safely(commit, message)
        cache.remember(alert)
        return "new"
    if classification == "duplicate":
        emit(
            {
                "timestamp": _utc_timestamp(),
                "component": "alert_consumer",
                "operation": "alert_duplicate",
                "status": "skipped",
                **context,
                "alert": alert,
            }
        )
        _commit_safely(commit, message)
        return "duplicate"
    return invalid("alert_id_conflict", alert=alert)


def commit_message(consumer: Any, message: Any) -> None:
    """Synchronously commit one message and verify its confirmed offset result."""
    try:
        topic = message.topic()
        partition = message.partition()
        expected_offset = message.offset() + 1
        result = consumer.commit(message=message, asynchronous=False)
        if result is None:
            raise ValueError
        confirmations = list(result)
        confirmed = False
        for item in confirmations:
            if getattr(item, "error", None) is not None:
                raise ValueError
            if (
                getattr(item, "topic", None) == topic
                and getattr(item, "partition", None) == partition
                and getattr(item, "offset", None) == expected_offset
            ):
                confirmed = True
        if not confirmed:
            raise ValueError
    except Exception:
        raise AlertCommitError("alert commit failed") from None
