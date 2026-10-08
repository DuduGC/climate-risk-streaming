from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from climate_logistics import kafka_publisher
from climate_logistics.models import WeatherEvent

pytestmark = pytest.mark.unit

INGESTION = datetime(2026, 10, 8, 12, 34, 56, 789000, tzinfo=UTC)


def event(*, location_id: str = "RJ01", city: str = "Rio de Janeiro") -> WeatherEvent:
    return WeatherEvent(
        location_id=location_id,
        city=city,
        latitude=-22.9,
        longitude=-43.17,
        temperature=27.4,
        precipitation=18.2,
        wind_speed=42.1,
        wind_gust=58.3,
        visibility=7000,
        weather_code=61,
        measurement_interval_seconds=900,
        event_timestamp=datetime(2026, 10, 8, 12, 30, tzinfo=UTC),
        ingestion_timestamp=INGESTION,
    )


class FakeProducer:
    def __init__(self) -> None:
        self.accepted: list[tuple[dict[str, object], object]] = []
        self.produce_attempts = 0
        self.callbacks: list[object] = []
        self.poll_timeouts: list[float] = []
        self.flush_timeouts: list[float] = []
        self.buffer_errors = 0
        self.callback_error: object | None = None
        self.callback_errors: list[object | None] = []
        self.deliver_on_poll = False
        self.flush_without_callbacks = False
        self.flush_pending_count: int | None = None

    def produce(self, topic: str, **kwargs: object) -> None:
        self.produce_attempts += 1
        kwargs["topic"] = topic
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.accepted.append((kwargs, kwargs["on_delivery"]))
        self.callbacks.append(kwargs["on_delivery"])

    def _next_error(self) -> object | None:
        return self.callback_errors.pop(0) if self.callback_errors else self.callback_error

    def poll(self, timeout: float = 0) -> int:
        self.poll_timeouts.append(timeout)
        if self.deliver_on_poll and self.callbacks:
            callback = self.callbacks.pop(0)
            callback(self._next_error(), None)
            return 1
        return 0

    def flush(self, timeout: float = 0) -> int:
        self.flush_timeouts.append(timeout)
        if self.flush_without_callbacks:
            if self.flush_pending_count is None:
                return len(self.callbacks)
            return self.flush_pending_count
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(self._next_error(), None)
        return 0

def test_serialization_preserves_schema_and_uses_ingestion_as_create_time():
    key, value, timestamp = kafka_publisher.serialize_event(event())

    assert key == b"RJ01"
    assert json.loads(value) == event().to_dict()
    assert value.decode("utf-8") == json.dumps(
        event().to_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    assert timestamp == int(INGESTION.timestamp() * 1000)


def test_serialization_accepts_exactly_16_kib_and_rejects_the_next_byte():
    base = event()
    base_bytes = json.dumps(base.to_dict(), ensure_ascii=False, separators=(",", ":")).encode()
    exact_city = "x" * (len(base.city) + 16_384 - len(base_bytes))
    exact = event(city=exact_city)

    assert len(kafka_publisher.serialize_event(exact)[1]) == 16_384
    with pytest.raises(kafka_publisher.KafkaPublishError, match="16384"):
        kafka_publisher.serialize_event(event(city=exact_city + "x"))


def test_serialization_rejects_non_finite_json_number():
    invalid = SimpleNamespace(to_dict=lambda: {"temperature": float("nan")})

    with pytest.raises(kafka_publisher.KafkaPublishError):
        kafka_publisher.serialize_event(invalid)


def test_existing_compatible_topics_are_left_untouched(monkeypatch: pytest.MonkeyPatch):
    admin = FakeAdmin()
    monkeypatch.setattr(kafka_publisher, "AdminClient", lambda config: pytest.fail("new admin"))

    kafka_publisher.ensure_topics("kafka:9092", admin=admin)

    assert admin.created == []


def test_missing_topics_are_created_with_three_partitions_and_replication_one(
    monkeypatch: pytest.MonkeyPatch,
):
    admin = FakeAdmin(existing=set())
    created_specs: list[FakeNewTopic] = []

    def new_topic(name: str, *, num_partitions: int, replication_factor: int):
        spec = FakeNewTopic(name, num_partitions, replication_factor)
        created_specs.append(spec)
        return spec

    monkeypatch.setattr(kafka_publisher, "NewTopic", new_topic)

    kafka_publisher.ensure_topics("kafka:9092", admin=admin)

    actual = [
        (topic.topic, topic.num_partitions, topic.replication_factor)
        for topic in created_specs
    ]
    assert actual == [
        ("weather.raw", 3, 1),
        ("weather.alerts", 3, 1),
    ]
    assert admin.created == ["weather.raw", "weather.alerts"]


def test_incompatible_topic_topology_fails_without_mutating_topics():
    admin = FakeAdmin(partitions=2)

    with pytest.raises(kafka_publisher.KafkaSetupError, match="weather.raw"):
        kafka_publisher.ensure_topics("kafka:9092", admin=admin)

    assert admin.created == []


def test_topic_already_exists_race_is_rechecked_and_accepted(
    monkeypatch: pytest.MonkeyPatch,
):
    admin = FakeAdmin(existing=set(), race_on_create=True)
    monkeypatch.setattr(kafka_publisher, "NewTopic", FakeNewTopic)
    monkeypatch.setattr(kafka_publisher, "KafkaException", FakeKafkaException)

    kafka_publisher.ensure_topics("kafka:9092", admin=admin)

    assert admin.created == ["weather.raw", "weather.alerts"]
    assert admin.metadata_reads >= 2


def test_new_producer_receives_the_required_delivery_configuration(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}
    fake = FakeProducer()

    def build(config: dict[str, object]):
        captured.update(config)
        return fake

    monkeypatch.setattr(kafka_publisher, "Producer", build)
    kafka_publisher.KafkaPublisher("kafka:9092")

    assert captured == {
        "bootstrap.servers": "kafka:9092",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "linger.ms": 20,
        "batch.size": 32_768,
        "delivery.timeout.ms": 10_000,
    }

def test_finish_batch_counts_each_duplicate_identity_delivery():
    producer = FakeProducer()
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)

    publisher.enqueue(event())
    publisher.enqueue(event())

    assert publisher.finish_batch() == 2
    assert len(producer.accepted) == 2
    assert [item[0]["key"] for item in producer.accepted] == [b"RJ01", b"RJ01"]


def test_delivery_nack_is_raised_after_callbacks_are_served():
    producer = FakeProducer()
    producer.callback_error = RuntimeError("broker rejected record")
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)
    publisher.enqueue(event())

    with pytest.raises(kafka_publisher.KafkaPublishError, match="delivery"):
        publisher.finish_batch()


def test_delivery_nack_served_by_poll_remains_fatal_until_batch_finish():
    producer = FakeProducer()
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)
    publisher.enqueue(event())
    producer.buffer_errors = 1
    producer.deliver_on_poll = True
    producer.callback_error = RuntimeError("broker rejected first record")
    original_poll = producer.poll

    def poll_first_nack(timeout: float = 0) -> int:
        delivered = original_poll(timeout)
        producer.callback_error = None
        producer.deliver_on_poll = False
        return delivered

    producer.poll = poll_first_nack  # type: ignore[method-assign]
    publisher.enqueue(event(location_id="SP01"))

    with pytest.raises(kafka_publisher.KafkaPublishError, match="delivery"):
        publisher.finish_batch()

def test_delivery_error_retains_the_successful_ack_count():
    producer = FakeProducer()
    producer.callback_errors = [None, None, RuntimeError("broker rejected third record")]
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)
    for location_id in ("RJ01", "SP01", "CT01"):
        publisher.enqueue(event(location_id=location_id))

    with pytest.raises(kafka_publisher.KafkaPublishError, match="delivery") as error:
        publisher.finish_batch()

    assert error.value.delivered_count == 2

def test_flush_zero_without_delivery_callback_is_not_success():
    producer = FakeProducer()
    producer.flush_without_callbacks = True
    producer.flush_pending_count = 0
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)
    publisher.enqueue(event())

    with pytest.raises(kafka_publisher.KafkaPublishError, match="callback"):
        publisher.finish_batch()

    assert producer.flush_timeouts and producer.flush_timeouts[0] <= 12


def test_pending_records_after_flush_fail_the_batch():
    producer = FakeProducer()
    producer.flush_without_callbacks = True
    producer.flush_pending_count = 1
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer)
    publisher.enqueue(event())

    with pytest.raises(kafka_publisher.KafkaPublishError, match="pendente"):
        publisher.finish_batch()


def test_queue_full_retries_until_global_five_second_deadline():
    producer = FakeProducer()
    producer.buffer_errors = 100
    now = [0.0]

    def clock() -> float:
        return now[0]

    original_poll = producer.poll

    def advancing_poll(timeout: float = 0) -> int:
        result = original_poll(timeout)
        now[0] += max(timeout, 0.1)
        return result

    producer.poll = advancing_poll  # type: ignore[method-assign]
    publisher = kafka_publisher.KafkaPublisher("kafka:9092", producer=producer, clock=clock)

    with pytest.raises(kafka_publisher.KafkaPublishError, match="fila"):
        publisher.enqueue(event())

    assert now[0] <= 5.5
    assert producer.poll_timeouts
    assert all(0 < timeout <= 5 for timeout in producer.poll_timeouts)


def test_queue_full_does_not_retry_after_poll_passes_deadline():
    producer = FakeProducer()
    producer.buffer_errors = 1
    now = [0.0]
    publisher = kafka_publisher.KafkaPublisher(
        "kafka:9092", producer=producer, clock=lambda: now[0]
    )
    original_poll = producer.poll

    def poll_advances_past_deadline(timeout: float = 0) -> int:
        result = original_poll(timeout)
        now[0] += 6
        return result

    producer.poll = poll_advances_past_deadline  # type: ignore[method-assign]

    with pytest.raises(kafka_publisher.KafkaPublishError, match="fila"):
        publisher.enqueue(event())

    assert producer.produce_attempts == 1
    assert producer.accepted == []

class FakeNewTopic:
    def __init__(self, topic: str, num_partitions: int, replication_factor: int) -> None:
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor


class FakeFuture:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error

    def result(self, timeout: float | None = None) -> None:
        if self.error:
            raise self.error


class FakeKafkaException(Exception):
    pass


class FakeAdmin:
    def __init__(
        self,
        *,
        existing: set[str] | None = None,
        partitions: int = 3,
        race_on_create: bool = False,
    ) -> None:
        self.existing = {"weather.raw", "weather.alerts"} if existing is None else set(existing)
        self.partitions = partitions
        self.race_on_create = race_on_create
        self.created: list[str] = []
        self.metadata_reads = 0

    def list_topics(self, *, timeout: float):
        self.metadata_reads += 1
        topics = {name: self._metadata(self.partitions) for name in self.existing}
        return SimpleNamespace(topics=topics)

    def create_topics(self, topics: list[FakeNewTopic], *, request_timeout: float):
        futures: dict[str, FakeFuture] = {}
        for topic in topics:
            self.created.append(topic.topic)
            self.existing.add(topic.topic)
            error = _already_exists_error() if self.race_on_create else None
            futures[topic.topic] = FakeFuture(error)
        return futures

    @staticmethod
    def _metadata(partition_count: int):
        partitions = {
            index: SimpleNamespace(replicas=[1]) for index in range(partition_count)
        }
        return SimpleNamespace(error=None, partitions=partitions)


def _already_exists_error() -> BaseException:
    error = SimpleNamespace(code=lambda: kafka_publisher.TOPIC_ALREADY_EXISTS)
    return FakeKafkaException(error)
