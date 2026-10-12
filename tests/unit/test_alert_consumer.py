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


@pytest.mark.unit
def test_consumer_settings_accept_only_plain_bootstrap_and_safe_group_id():
    module = _consumer_module()

    defaults = module.ConsumerSettings.from_env({})
    configured = module.ConsumerSettings.from_env(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092,broker-2:9093",
            "ALERT_CONSUMER_GROUP_ID": "weather-alerts-qa-a1b2",
            "APP_DB_PASSWORD": "ignored-by-consumer",
        }
    )

    assert defaults.bootstrap_servers == "kafka:9092"
    assert defaults.group_id == "weather-alerts-consumer"
    assert configured.bootstrap_servers == "kafka:9092,broker-2:9093"
    assert configured.group_id == "weather-alerts-qa-a1b2"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bootstrap",
    [
        "",
        " ",
        "kafka:9092 ",
        "http://kafka:9092",
        "user:secret@kafka:9092",
        "kafka:9092?password=secret",
        "kafka:9092\nattacker:9093",
        "kafka:9092,",
        "kafka:70000",
    ],
)
def test_consumer_settings_reject_invalid_bootstrap_without_echoing_values(bootstrap):
    module = _consumer_module()

    with pytest.raises(module.ConsumerConfigurationError) as raised:
        module.ConsumerSettings.from_env({"KAFKA_BOOTSTRAP_SERVERS": bootstrap})

    assert "secret" not in str(raised.value)
    if bootstrap.strip():
        assert bootstrap not in str(raised.value)


@pytest.mark.unit
@pytest.mark.parametrize("group_id", ["", " ", "_bad", "bad/group", "bad group", "x" * 129])
def test_consumer_settings_reject_invalid_group_ids_without_echoing_values(group_id):
    module = _consumer_module()

    with pytest.raises(module.ConsumerConfigurationError) as raised:
        module.ConsumerSettings.from_env({"ALERT_CONSUMER_GROUP_ID": group_id})

    if group_id.strip():
        assert group_id not in str(raised.value)


@pytest.mark.unit
def test_consumer_loop_uses_manual_latest_config_and_keeps_idle_assignment_healthy(
    tmp_path, monkeypatch
):
    module = _consumer_module()
    stop = __import__("threading").Event()
    health_writes: list[dict[str, object]] = []
    original_write_health = module.write_health
    consumer = _RuntimeConsumer(stop=stop)
    received_config: dict[str, object] = {}
    emitted: list[dict[str, object]] = []

    def factory(config):
        received_config.update(config)
        return consumer

    def capture_health(path, **fields):
        health_writes.append(dict(fields))
        original_write_health(path, **fields)
        if fields["ready"]:
            stop.set()

    monkeypatch.setattr(module, "write_health", capture_health)
    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=stop,
        health_path=tmp_path / "health.json",
        consumer_factory=factory,
        emit=lambda record: emitted.append(dict(record)),
    )

    assert result == 0
    assert received_config == {
        "bootstrap.servers": "kafka:9092",
        "group.id": "weather-alerts-consumer",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "latest",
        "socket.timeout.ms": 10_000,
        "session.timeout.ms": 10_000,
        "heartbeat.interval.ms": 3_000,
        "max.poll.interval.ms": 60_000,
    }
    assert consumer.subscribed_topics == ["weather.alerts"]
    assert consumer.poll_timeouts == [1.0]
    assert any(item["ready"] is True for item in health_writes)
    assert health_writes[-1]["ready"] is False
    assert consumer.closed is True
    assert consumer.global_commit_calls == 0
    assert [record["operation"] for record in emitted] == [
        "consumer_started",
        "consumer_assigned",
        "consumer_stopped",
    ]
    assert all(
        record["component"] == "alert_consumer"
        and record["status"] == "success"
        and record["timestamp"].endswith("Z")
        for record in emitted
    )


@pytest.mark.unit
def test_assignment_preserves_committed_offsets_and_commits_only_new_latest_baselines():
    module = _consumer_module()
    consumer = _RuntimeConsumer(
        initial_commits={("weather.alerts", 0): 13},
        assigned_partitions=(0, 1),
        watermarks={1: (4, 30)},
    )

    module.initialize_assignment(
        consumer,
        [_RuntimeTopicPartition("weather.alerts", 0), _RuntimeTopicPartition("weather.alerts", 1)],
    )

    assert [(item.partition, item.offset) for item in consumer.assignments[-1]] == [
        (0, 13),
        (1, 30),
    ]
    assert len(consumer.offset_commit_calls) == 1
    assert [(item.partition, item.offset) for item in consumer.offset_commit_calls[0]] == [(1, 30)]


@pytest.mark.unit
def test_empty_assignment_and_invalid_metadata_never_report_ready(tmp_path, monkeypatch):
    module = _consumer_module()

    empty_stop = __import__("threading").Event()
    empty_consumer = _RuntimeConsumer(stop=empty_stop, assigned_partitions=())
    empty_writes: list[dict[str, object]] = []
    original_write_health = module.write_health

    def stop_after_two_polls(consumer, timeout):
        consumer.poll_timeouts.append(timeout)
        if len(consumer.poll_timeouts) == 2:
            empty_stop.set()
        return None

    empty_consumer.poll = lambda timeout: stop_after_two_polls(empty_consumer, timeout)

    def capture_empty_health(path, **fields):
        empty_writes.append(dict(fields))
        original_write_health(path, **fields)

    monkeypatch.setattr(module, "write_health", capture_empty_health)
    assert (
        module.run_consumer(
            module.ConsumerSettings.from_env({}),
            stop=empty_stop,
            health_path=tmp_path / "empty.json",
            consumer_factory=lambda config: empty_consumer,
            emit=lambda record: None,
        )
        == 0
    )
    assert not any(item["ready"] for item in empty_writes)

    bad_metadata = _RuntimeConsumer(metadata_error=RuntimeError("broker secret"))
    bad_writes: list[dict[str, object]] = []

    def capture_bad_health(path, **fields):
        bad_writes.append(dict(fields))
        original_write_health(path, **fields)

    monkeypatch.setattr(module, "write_health", capture_bad_health)
    assert (
        module.run_consumer(
            module.ConsumerSettings.from_env({}),
            stop=__import__("threading").Event(),
            health_path=tmp_path / "bad.json",
            consumer_factory=lambda config: bad_metadata,
            emit=lambda record: None,
        )
        == 1
    )
    assert not any(item["ready"] for item in bad_writes)
    assert bad_metadata.closed is True


@pytest.mark.unit
def test_operational_message_error_is_not_sent_to_handler_or_committed(tmp_path, capsys):
    module = _consumer_module()
    consumer = _RuntimeConsumer(messages=[FakeMessage(error=RuntimeError("broker secret"))])
    emitted: list[dict[str, object]] = []
    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=__import__("threading").Event(),
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=lambda record: emitted.append(dict(record)),
    )

    assert result == 1
    assert [record["operation"] for record in emitted] == [
        "consumer_started",
        "consumer_assigned",
        "consumer_error",
        "consumer_stopped",
    ]
    assert emitted[2]["status"] == "error"
    assert all("broker secret" not in json.dumps(record) for record in emitted)
    assert consumer.message_commit_calls == 0
    assert consumer.global_commit_calls == 0
    assert consumer.closed is True
    assert "broker secret" not in capsys.readouterr().err


@pytest.mark.unit
def test_loop_reuses_one_bounded_cache_across_messages(tmp_path):
    module = _consumer_module()
    stop = __import__("threading").Event()
    alert = _alert()
    reordered = dict(reversed(tuple(alert.items())))
    reordered["precipitation_rate_max_mm_per_hour"] = 20
    messages = [
        FakeMessage(),
        FakeMessage(value=json.dumps(reordered, separators=(",", ":")).encode("utf-8")),
    ]
    consumer = _RuntimeConsumer(messages=messages)
    emitted: list[dict[str, object]] = []

    def emit(record):
        emitted.append(dict(record))
        if record.get("operation") == "alert_duplicate":
            stop.set()

    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=stop,
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=emit,
    )

    assert result == 0
    assert [
        record["operation"]
        for record in emitted
        if record["operation"] in {"alert_received", "alert_duplicate"}
    ] == [
        "alert_received",
        "alert_duplicate",
    ]
    assert consumer.message_commit_calls == 2
    assert consumer.global_commit_calls == 0
    assert consumer.closed is True


@pytest.mark.unit
def test_consumer_exception_closes_without_global_commit_or_secret_error_output(
    tmp_path, capsys
):
    module = _consumer_module()
    consumer = _RuntimeConsumer(initial_commits={("weather.alerts", 0): 7})

    def fail_poll(timeout):
        raise RuntimeError("broker secret")

    consumer.poll = fail_poll
    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=__import__("threading").Event(),
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=lambda record: None,
    )

    assert result == 1
    assert consumer.closed is True
    assert consumer.global_commit_calls == 0
    assert "broker secret" not in capsys.readouterr().err


@pytest.mark.unit
def test_signal_handler_requests_shutdown_without_exposing_signal_details(monkeypatch):
    module = _consumer_module()
    stop = __import__("threading").Event()
    registered = {}
    prior = {module.signal.SIGINT: object(), module.signal.SIGTERM: object()}

    def fake_signal(signum, handler):
        registered[signum] = handler
        return prior[signum]

    monkeypatch.setattr(module.signal, "signal", fake_signal)
    previous = module._install_signal_handlers(stop)

    assert previous == prior
    registered[module.signal.SIGTERM](module.signal.SIGTERM, None)
    assert stop.is_set()


@pytest.mark.unit
def test_assignment_callback_failure_logs_error_and_closes_without_processing(
    tmp_path, monkeypatch
):
    module = _consumer_module()
    consumer = _RuntimeConsumer(messages=[FakeMessage()])
    emitted: list[dict[str, object]] = []

    def fail_assignment(*args, **kwargs):
        raise RuntimeError("sensitive assignment detail")

    monkeypatch.setattr(module, "initialize_assignment", fail_assignment)
    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=__import__("threading").Event(),
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=lambda record: emitted.append(dict(record)),
    )

    assert result == 1
    assert [record["operation"] for record in emitted] == [
        "consumer_started",
        "consumer_error",
        "consumer_stopped",
    ]
    assert all("sensitive assignment detail" not in json.dumps(record) for record in emitted)
    assert consumer.message_commit_calls == 0
    assert consumer.global_commit_calls == 0
    assert consumer.closed is True


@pytest.mark.unit
def test_alert_log_failure_emits_generic_error_and_prevents_message_commit(tmp_path):
    module = _consumer_module()
    consumer = _RuntimeConsumer(messages=[FakeMessage()])
    emitted: list[dict[str, object]] = []

    def fail_alert_output(record):
        if record.get("operation") == "alert_received":
            raise OSError("sensitive stdout detail")
        emitted.append(dict(record))

    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=__import__("threading").Event(),
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=fail_alert_output,
    )

    assert result == 1
    assert [record["operation"] for record in emitted] == [
        "consumer_started",
        "consumer_assigned",
        "consumer_error",
        "consumer_stopped",
    ]
    assert all("sensitive stdout detail" not in json.dumps(record) for record in emitted)
    assert consumer.message_commit_calls == 0
    assert consumer.global_commit_calls == 0
    assert consumer.closed is True


@pytest.mark.unit
def test_stopped_log_failure_still_invalidates_health_and_closes_consumer(
    tmp_path, monkeypatch
):
    module = _consumer_module()
    stop = __import__("threading").Event()
    consumer = _RuntimeConsumer(stop=stop)
    original_write_health = module.write_health

    def stop_after_ready(path, **fields):
        original_write_health(path, **fields)
        if fields["ready"]:
            stop.set()

    def fail_stopped_log(record):
        if record.get("operation") == "consumer_stopped":
            raise OSError("shutdown log failure")

    monkeypatch.setattr(module, "write_health", stop_after_ready)
    result = module.run_consumer(
        module.ConsumerSettings.from_env({}),
        stop=stop,
        health_path=tmp_path / "health.json",
        consumer_factory=lambda config: consumer,
        emit=fail_stopped_log,
    )

    health = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert result == 1
    assert health["ready"] is False
    assert consumer.closed is True
    assert consumer.global_commit_calls == 0


@dataclass
class _RuntimeTopicPartition:
    topic: str
    partition: int
    offset: int = -1001
    error: object | None = None


class _RuntimeConsumer:
    def __init__(
        self,
        *,
        stop=None,
        initial_commits=None,
        assigned_partitions=(0,),
        watermarks=None,
        messages=(),
        metadata_error=None,
    ):
        self.stop = stop
        self.initial_commits = dict(initial_commits or {})
        self.assigned_partitions = assigned_partitions
        self.watermarks = dict(watermarks or {})
        self.messages = list(messages)
        self.metadata_error = metadata_error
        self.subscribed_topic = None
        self.on_assign = None
        self.assignments = []
        self.offset_commit_calls = []
        self.message_commit_calls = 0
        self.global_commit_calls = 0
        self.poll_timeouts = []
        self.closed = False
        self._assignment = []
        self._callback_called = False
        self._empty_stop_after_first_poll = False

    def subscribe(self, topics, *, on_assign):
        self.subscribed_topics = list(topics)
        self.on_assign = on_assign

    def list_topics(self, topic, *, timeout):
        assert timeout == 5
        error = self.metadata_error
        topic_metadata = type(
            "TopicMetadata",
            (),
            {"error": error, "partitions": {} if error else {0: object()}},
        )()
        return type("Metadata", (), {"topics": {topic: topic_metadata}})()

    def committed(self, partitions, *, timeout):
        assert timeout == 5
        return [
            _RuntimeTopicPartition(
                item.topic,
                item.partition,
                self.initial_commits.get((item.topic, item.partition), -1001),
            )
            for item in partitions
        ]

    def get_watermark_offsets(self, partition, *, timeout, cached):
        assert timeout == 5
        assert cached is False
        return self.watermarks.get(partition.partition, (0, 10))

    def assign(self, partitions):
        self._assignment = list(partitions)
        self.assignments.append(list(partitions))

    def commit(self, *, message=None, offsets=None, asynchronous=False):
        assert asynchronous is False
        if message is not None:
            self.message_commit_calls += 1
            return [
                _RuntimeTopicPartition(message.topic(), message.partition(), message.offset() + 1)
            ]
        if offsets is None:
            self.global_commit_calls += 1
            return None
        self.offset_commit_calls.append(list(offsets))
        for item in offsets:
            self.initial_commits[(item.topic, item.partition)] = item.offset
        return [
            _RuntimeTopicPartition(item.topic, item.partition, item.offset) for item in offsets
        ]

    def poll(self, timeout):
        self.poll_timeouts.append(timeout)
        if not self._callback_called:
            self._callback_called = True
            self.on_assign(
                self,
                [
                    _RuntimeTopicPartition("weather.alerts", partition)
                    for partition in self.assigned_partitions
                ],
            )
        if self.messages:
            return self.messages.pop(0)
        return None

    def assignment(self):
        return list(self._assignment)

    def close(self):
        self.closed = True
