"""Per-message validation, logging, and offset confirmation for weather alerts."""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from climate_logistics.business_records import AlertIdCache, BusinessRecordError, parse_alert
from climate_logistics.consumer_health import DEFAULT_HEALTH_PATH, write_health
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


class ConsumerConfigurationError(ValueError):
    """Raised when public alert-consumer bootstrap settings are unsafe or invalid."""


_GROUP_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_BROKER = re.compile(r"(?:[A-Za-z0-9_.-]+|\[[0-9A-Fa-f:.]+\]):([0-9]{1,5})\Z")
_INVALID_OFFSET = -1001
_METADATA_INTERVAL_SECONDS = 15.0
_METADATA_TIMEOUT_SECONDS = 5.0
_POLL_TIMEOUT_SECONDS = 1.0


@dataclass(frozen=True)
class ConsumerSettings:
    """Public Kafka settings for the alert consumer."""

    bootstrap_servers: str = "kafka:9092"
    group_id: str = "weather-alerts-consumer"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ConsumerSettings:
        bootstrap = env.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        group_id = env.get("ALERT_CONSUMER_GROUP_ID", "weather-alerts-consumer")
        if not isinstance(bootstrap, str) or not bootstrap or bootstrap != bootstrap.strip():
            raise ConsumerConfigurationError("KAFKA_BOOTSTRAP_SERVERS inválido")
        brokers = bootstrap.split(",")
        for broker in brokers:
            match = _BROKER.fullmatch(broker)
            if match is None or not 1 <= int(match.group(1)) <= 65_535:
                raise ConsumerConfigurationError("KAFKA_BOOTSTRAP_SERVERS inválido")
        if not isinstance(group_id, str) or _GROUP_ID.fullmatch(group_id) is None:
            raise ConsumerConfigurationError("ALERT_CONSUMER_GROUP_ID inválido")
        return cls(bootstrap_servers=bootstrap, group_id=group_id)


def _consumer_config(settings: ConsumerSettings) -> dict[str, object]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "latest",
        "socket.timeout.ms": 10_000,
        "session.timeout.ms": 10_000,
        "heartbeat.interval.ms": 3_000,
        "max.poll.interval.ms": 60_000,
    }


def _new_topic_partition(template: Any, offset: int) -> Any:
    try:
        from confluent_kafka import TopicPartition
    except ModuleNotFoundError:
        import copy

        partition = copy.copy(template)
        partition.offset = offset
        return partition
    return TopicPartition(template.topic, template.partition, offset)


def _partition_key(partition: Any) -> tuple[str, int]:
    try:
        topic = partition.topic
        number = partition.partition
        error = getattr(partition, "error", None)
    except Exception:
        raise RuntimeError("alert assignment failed") from None
    if (
        topic != ALERTS_TOPIC
        or type(number) is not int
        or number < 0
        or error is not None
    ):
        raise RuntimeError("alert assignment failed")
    return topic, number


def _confirmed_offsets(result: Any, expected: list[Any]) -> None:
    if result is None:
        raise RuntimeError("alert assignment failed")
    try:
        actual = list(result)
        expected_by_key = {_partition_key(item): item.offset for item in expected}
        actual_by_key: dict[tuple[str, int], int] = {}
        for item in actual:
            key = _partition_key(item)
            if key in actual_by_key or type(item.offset) is not int or item.offset < 0:
                raise RuntimeError("alert assignment failed")
            actual_by_key[key] = item.offset
    except Exception:
        raise RuntimeError("alert assignment failed") from None
    if actual_by_key != expected_by_key:
        raise RuntimeError("alert assignment failed")


def initialize_assignment(consumer: Any, partitions: Sequence[Any]) -> None:
    """Assign committed positions and initialize new partitions at captured high watermarks."""
    requested = list(partitions)
    if not requested:
        consumer.assign([])
        return

    requested_keys = [_partition_key(item) for item in requested]
    if len(set(requested_keys)) != len(requested_keys):
        raise RuntimeError("alert assignment failed")
    try:
        committed = consumer.committed(requested, timeout=_METADATA_TIMEOUT_SECONDS)
        committed_items = list(committed)
    except Exception:
        raise RuntimeError("alert assignment failed") from None
    committed_by_key: dict[tuple[str, int], Any] = {}
    try:
        for item in committed_items:
            key = _partition_key(item)
            if key in committed_by_key:
                raise RuntimeError("alert assignment failed")
            committed_by_key[key] = item
    except Exception:
        raise RuntimeError("alert assignment failed") from None
    if set(committed_by_key) != set(requested_keys):
        raise RuntimeError("alert assignment failed")

    assigned: list[Any] = []
    baselines: list[Any] = []
    for partition, key in zip(requested, requested_keys, strict=True):
        previous = committed_by_key[key]
        try:
            offset = previous.offset
        except Exception:
            raise RuntimeError("alert assignment failed") from None
        if type(offset) is not int:
            raise RuntimeError("alert assignment failed")
        if offset >= 0:
            assigned.append(_new_topic_partition(partition, offset))
            continue
        if offset != _INVALID_OFFSET:
            raise RuntimeError("alert assignment failed")
        try:
            low, high = consumer.get_watermark_offsets(
                partition,
                timeout=_METADATA_TIMEOUT_SECONDS,
                cached=False,
            )
        except Exception:
            raise RuntimeError("alert assignment failed") from None
        if type(low) is not int or type(high) is not int or low < 0 or high < low:
            raise RuntimeError("alert assignment failed")
        baseline = _new_topic_partition(partition, high)
        assigned.append(baseline)
        baselines.append(baseline)

    try:
        consumer.assign(assigned)
        if baselines:
            result = consumer.commit(offsets=baselines, asynchronous=False)
            _confirmed_offsets(result, baselines)
    except Exception:
        raise RuntimeError("alert assignment failed") from None


def _refresh_metadata(consumer: Any) -> float:
    try:
        metadata = consumer.list_topics(ALERTS_TOPIC, timeout=_METADATA_TIMEOUT_SECONDS)
        topics = metadata.topics
        topic = topics.get(ALERTS_TOPIC)
        partitions = topic.partitions
        if (
            topic is None
            or getattr(topic, "error", None) is not None
            or not isinstance(partitions, Mapping)
            or not partitions
        ):
            raise RuntimeError("alert metadata unavailable")
        for number, partition in partitions.items():
            if (
                type(number) is not int
                or number < 0
                or getattr(partition, "error", None) is not None
            ):
                raise RuntimeError("alert metadata unavailable")
    except Exception:
        raise RuntimeError("alert metadata unavailable") from None
    return time.monotonic()


def _has_assignment(consumer: Any) -> bool:
    try:
        assignment = list(consumer.assignment())
        return bool(assignment) and all(
            _partition_key(item)[0] == ALERTS_TOPIC for item in assignment
        )
    except Exception:
        return False


def _is_partition_eof(error: Any) -> bool:
    try:
        from confluent_kafka import KafkaError

        return error.code() == KafkaError._PARTITION_EOF
    except Exception:
        return False


def _default_consumer_factory(config: Mapping[str, object]) -> Any:
    from confluent_kafka import Consumer

    return Consumer(dict(config))


def _report_runtime_failure() -> None:
    try:
        sys.stderr.write("alert consumer failed\n")
        sys.stderr.flush()
    except Exception:
        pass


def _emit_lifecycle(
    emit: Callable[[Mapping[str, object]], None], operation: str, status: str
) -> None:
    emit(
        {
            "timestamp": _utc_timestamp(),
            "component": "alert_consumer",
            "operation": operation,
            "status": status,
        }
    )


def run_consumer(
    settings: ConsumerSettings,
    *,
    stop: threading.Event,
    health_path: Path,
    consumer_factory: Callable[..., Any] | None = None,
    emit: Callable[[Mapping[str, object]], None] = emit_record,
) -> int:
    """Run one sequential Kafka consumer and keep its local health heartbeat current."""
    consumer: Any | None = None
    exit_code = 0
    ready = False
    metadata_at = 0.0
    error_logged = False
    now = time.monotonic()

    def log_error() -> None:
        nonlocal error_logged, exit_code
        exit_code = 1
        if error_logged:
            return
        error_logged = True
        try:
            _emit_lifecycle(emit, "consumer_error", "error")
        except Exception:
            _report_runtime_failure()

    try:
        write_health(
            health_path,
            pid=os.getpid(),
            ready=False,
            updated_at=now,
            metadata_at=metadata_at,
        )
        _emit_lifecycle(emit, "consumer_started", "success")
        factory = _default_consumer_factory if consumer_factory is None else consumer_factory
        consumer = factory(_consumer_config(settings))
        assignment_failed = False

        def on_assign(client: Any, assigned: list[Any]) -> None:
            nonlocal assignment_failed
            try:
                initialize_assignment(client, assigned)
                _emit_lifecycle(emit, "consumer_assigned", "success")
            except Exception:
                assignment_failed = True

        consumer.subscribe([ALERTS_TOPIC], on_assign=on_assign)
        metadata_at = _refresh_metadata(consumer)
        next_metadata_at = metadata_at + _METADATA_INTERVAL_SECONDS
        cache = AlertIdCache()

        while not stop.is_set():
            if time.monotonic() >= next_metadata_at:
                metadata_at = _refresh_metadata(consumer)
                next_metadata_at = metadata_at + _METADATA_INTERVAL_SECONDS
            message = consumer.poll(_POLL_TIMEOUT_SECONDS)
            if assignment_failed:
                raise RuntimeError("alert assignment failed")
            assigned = _has_assignment(consumer)
            if message is not None and not stop.is_set():
                try:
                    error = message.error()
                except Exception:
                    raise RuntimeError("alert message error unavailable") from None
                if error is not None:
                    if not _is_partition_eof(error):
                        raise RuntimeError("alert message error")
                elif assigned:
                    handle_message(
                        message,
                        cache=cache,
                        emit=emit,
                        commit=lambda received: commit_message(consumer, received),
                    )
            ready = assigned and not stop.is_set()
            heartbeat_at = time.monotonic()
            write_health(
                health_path,
                pid=os.getpid(),
                ready=ready,
                updated_at=heartbeat_at,
                metadata_at=metadata_at,
            )
    except Exception:
        log_error()
    finally:
        try:
            write_health(
                health_path,
                pid=os.getpid(),
                ready=False,
                updated_at=time.monotonic(),
                metadata_at=metadata_at,
            )
        except Exception:
            log_error()
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                log_error()
        try:
            _emit_lifecycle(
                emit,
                "consumer_stopped",
                "success" if exit_code == 0 else "error",
            )
        except Exception:
            _report_runtime_failure()
            exit_code = 1
    return exit_code


def _install_signal_handlers(stop: threading.Event) -> dict[int, Any]:
    if threading.current_thread() is not threading.main_thread():
        return {}

    previous: dict[int, Any] = {}

    def request_stop(signum: int, frame: object) -> None:
        stop.set()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
    except Exception:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        raise
    return previous


def _restore_signal_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def main() -> int:
    """Install shutdown handlers and run from the supported environment settings."""
    stop = threading.Event()
    try:
        settings = ConsumerSettings.from_env(os.environ)
    except ConsumerConfigurationError:
        _report_runtime_failure()
        try:
            write_health(
                DEFAULT_HEALTH_PATH,
                pid=os.getpid(),
                ready=False,
                updated_at=time.monotonic(),
                metadata_at=0.0,
            )
        except Exception:
            pass
        return 1

    try:
        previous = _install_signal_handlers(stop)
    except Exception:
        _report_runtime_failure()
        return 1
    try:
        return run_consumer(settings, stop=stop, health_path=DEFAULT_HEALTH_PATH)
    finally:
        try:
            _restore_signal_handlers(previous)
        except Exception:
            _report_runtime_failure()


if __name__ == "__main__":
    raise SystemExit(main())
