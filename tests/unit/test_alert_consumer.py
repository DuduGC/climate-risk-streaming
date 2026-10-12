from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any

import pytest
from test_alert_records import _alert, _raw


def _consumer_module():
    try:
        return importlib.import_module("climate_logistics.alert_consumer")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.alert_consumer":
            pytest.fail("the alert consumer has not been implemented yet", pytrace=False)
        raise


def _records():
    return importlib.import_module("climate_logistics.business_records")


class FakeMessage:
    def __init__(
        self,
        *,
        key: bytes | None = b"RJ01",
        value: bytes | None = None,
        topic: str = "weather.alerts",
        partition: int = 1,
        offset: int = 7,
        error: object | None = None,
    ) -> None:
        self._key = key
        self._value = _raw(_alert()).encode("utf-8") if value is None else value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> object | None:
        return self._error


@pytest.mark.unit
def test_emit_record_writes_one_compact_json_line_and_flushes():
    module = _consumer_module()

    class FlushTrackingStream(StringIO):
        flushed = False

        def flush(self) -> None:
            self.flushed = True
            super().flush()

    stream = FlushTrackingStream()
    module.emit_record({"operation": "alert_received", "city": "São Paulo"}, stream)

    rendered = json.loads(stream.getvalue())
    timestamp = datetime.fromisoformat(rendered["timestamp"].replace("Z", "+00:00"))
    assert rendered["operation"] == "alert_received"
    assert rendered["city"] == "São Paulo"
    assert timestamp.tzinfo == UTC
    assert stream.getvalue().count("\n") == 1
    assert stream.flushed is True


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["write", "flush"])
def test_emit_record_propagates_stdout_write_and_flush_failures(failure: str):
    module = _consumer_module()

    class FailingStream:
        def write(self, value: str) -> int:
            if failure == "write":
                raise OSError("write failed")
            return len(value)

        def flush(self) -> None:
            if failure == "flush":
                raise OSError("flush failed")

    with pytest.raises(OSError, match=f"{failure} failed"):
        module.emit_record({"operation": "alert_received"}, FailingStream())


@pytest.mark.unit
def test_valid_alert_is_emitted_before_synchronous_commit_then_remembered():
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    message = FakeMessage()
    sequence: list[str] = []
    emitted: list[dict[str, object]] = []

    def emit(record: dict[str, object]) -> None:
        sequence.append("emit")
        emitted.append(dict(record))

    def commit(received: Any) -> None:
        assert received is message
        sequence.append("commit")

    assert module.handle_message(message, cache=cache, emit=emit, commit=commit) == "new"
    assert sequence == ["emit", "commit"]
    assert emitted[0]["component"] == "alert_consumer"
    assert emitted[0]["operation"] == "alert_received"
    assert emitted[0]["status"] == "success"
    assert emitted[0]["timestamp"].endswith("Z")
    assert {key: emitted[0][key] for key in ("topic", "partition", "offset")} == {
        "topic": "weather.alerts",
        "partition": 1,
        "offset": 7,
    }
    assert emitted[0]["alert"] == _alert()
    assert cache.classify(_alert()) == "duplicate"


@pytest.mark.unit
def test_duplicate_and_divergent_alerts_are_emitted_then_committed_without_overwrite():
    module = _consumer_module()
    records = _records()
    cached = _alert()
    cache = records.AlertIdCache()
    cache.remember(cached)
    emitted: list[dict[str, object]] = []
    committed: list[FakeMessage] = []

    def emit(record: dict[str, object]) -> None:
        emitted.append(dict(record))

    def commit(message: FakeMessage) -> None:
        committed.append(message)

    duplicate = FakeMessage()
    assert module.handle_message(duplicate, cache=cache, emit=emit, commit=commit) == "duplicate"
    changed = dict(cached)
    changed["temperature_avg"] = 23.0
    divergent = FakeMessage(value=_raw(changed).encode("utf-8"))
    assert module.handle_message(divergent, cache=cache, emit=emit, commit=commit) == "invalid"

    assert [item["operation"] for item in emitted] == ["alert_duplicate", "alert_invalid"]
    assert [item["status"] for item in emitted] == ["skipped", "error"]
    assert emitted[0]["alert"] == cached
    assert emitted[1]["alert"] == changed
    assert emitted[1]["reason"] == "alert_id_conflict"
    assert committed == [duplicate, divergent]
    assert cache.classify(cached) == "duplicate"
    assert cache.classify(changed) == "divergent"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("message", "reason"),
    [
        (FakeMessage(value=b'{"raw":"secret-payload"}'), "invalid_alert"),
        (FakeMessage(key=b"CT01"), "alert_key_mismatch"),
        (FakeMessage(key=b"not-in-catalog"), "alert_key_mismatch"),
        (FakeMessage(key=b"\xff"), "invalid_key"),
        (FakeMessage(value=b"\xff"), "invalid_alert"),
    ],
)
def test_invalid_input_emits_constant_reason_without_raw_data_then_commits(
    message: FakeMessage, reason: str
):
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    emitted: list[dict[str, object]] = []
    committed: list[FakeMessage] = []

    result = module.handle_message(
        message,
        cache=cache,
        emit=lambda record: emitted.append(dict(record)),
        commit=committed.append,
    )

    assert result == "invalid"
    assert emitted[0]["component"] == "alert_consumer"
    assert emitted[0]["operation"] == "alert_invalid"
    assert emitted[0]["status"] == "error"
    assert emitted[0]["reason"] == reason
    assert {key: emitted[0][key] for key in ("topic", "partition", "offset")} == {
        "topic": "weather.alerts",
        "partition": 1,
        "offset": 7,
    }
    assert "raw" not in emitted[0]
    assert "secret-payload" not in json.dumps(emitted[0])
    assert committed == [message]


@pytest.mark.unit
def test_emit_failure_prevents_commit_and_cache_remember():
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    message = FakeMessage()
    committed: list[FakeMessage] = []

    with pytest.raises(OSError, match="stdout is full"):
        module.handle_message(
            message,
            cache=cache,
            emit=lambda record: (_ for _ in ()).throw(OSError("stdout is full")),
            commit=committed.append,
        )

    assert committed == []
    assert cache.classify(_alert()) == "new"


@pytest.mark.unit
def test_emit_failure_on_invalid_key_is_not_retried_or_committed():
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    message = FakeMessage(key=None)
    emitted = 0
    committed: list[FakeMessage] = []

    def fail_emit(record: dict[str, object]) -> None:
        nonlocal emitted
        emitted += 1
        raise OSError("stdout is full")

    with pytest.raises(OSError, match="stdout is full"):
        module.handle_message(message, cache=cache, emit=fail_emit, commit=committed.append)

    assert emitted == 1
    assert committed == []
    assert cache.classify(_alert()) == "new"


@pytest.mark.unit
def test_commit_failure_is_generic_and_does_not_remember_alert():
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    message = FakeMessage()

    with pytest.raises(module.AlertCommitError, match="alert commit failed") as raised:
        module.handle_message(
            message,
            cache=cache,
            emit=lambda record: None,
            commit=lambda _: (_ for _ in ()).throw(RuntimeError("secret payload and key")),
        )

    assert "secret payload" not in str(raised.value)
    assert cache.classify(_alert()) == "new"


@pytest.mark.unit
def test_returned_partition_error_prevents_handler_cache_remember():
    module = _consumer_module()
    records = _records()
    cache = records.AlertIdCache()
    message = FakeMessage()

    @dataclass
    class ErrorPartition:
        topic: str = "weather.alerts"
        partition: int = 1
        offset: int = 8
        error: object | None = RuntimeError("broker secret")

    class Consumer:
        def commit(self, *, message: object, asynchronous: bool) -> list[ErrorPartition]:
            return [ErrorPartition()]

    with pytest.raises(module.AlertCommitError, match="alert commit failed"):
        module.handle_message(
            message,
            cache=cache,
            emit=lambda record: None,
            commit=lambda received: module.commit_message(Consumer(), received),
        )

    assert cache.classify(_alert()) == "new"


@pytest.mark.unit
def test_topic_mismatch_and_kafka_message_error_fail_before_emit_or_commit():
    module = _consumer_module()
    records = _records()

    for message in (
        FakeMessage(topic="weather.raw"),
        FakeMessage(error=RuntimeError("broker secret")),
    ):
        events: list[dict[str, object]] = []
        commits: list[FakeMessage] = []
        with pytest.raises(module.AlertMessageError):
            module.handle_message(
                message,
                cache=records.AlertIdCache(),
                emit=lambda record: events.append(dict(record)),
                commit=commits.append,
            )
        assert events == []
        assert commits == []


@pytest.mark.unit
def test_commit_message_requires_matching_partition_offset_and_no_error():
    module = _consumer_module()
    message = FakeMessage()

    class FakeConsumer:
        result: object
        call: tuple[object, object] | None = None

        def commit(self, *, message: object, asynchronous: bool) -> object:
            self.call = (message, asynchronous)
            return self.result

    @dataclass
    class TopicPartition:
        topic: str
        partition: int
        offset: int
        error: object | None = None

    consumer = FakeConsumer()
    consumer.result = [TopicPartition("weather.alerts", 1, 8)]
    module.commit_message(consumer, message)
    assert consumer.call == (message, False)

    for result in (
        [TopicPartition("weather.alerts", 1, 8, RuntimeError("broker secret"))],
        [TopicPartition("weather.alerts", 1, 7)],
        [TopicPartition("weather.alerts", 2, 8)],
        [],
        None,
    ):
        consumer.result = result
        with pytest.raises(module.AlertCommitError, match="alert commit failed"):
            module.commit_message(consumer, message)
