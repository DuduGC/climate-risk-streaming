from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from climate_logistics.kafka_publisher import KafkaPublisher, KafkaPublishError, ensure_topics
from climate_logistics.locations import LOCATIONS
from climate_logistics.models import WeatherEvent

pytestmark = [pytest.mark.integration]


def _require_kafka() -> str:
    if os.environ.get("KAFKA_INTEGRATION") != "1":
        pytest.skip("defina KAFKA_INTEGRATION=1 para usar o broker Kafka real")
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _sample_event() -> WeatherEvent:
    return WeatherEvent(
        location_id=f"IT-{uuid4().hex}",
        city="Fixture de integração",
        latitude=-22.9,
        longitude=-43.17,
        temperature=27.4,
        precipitation=0,
        wind_speed=10,
        wind_gust=12,
        visibility=9000,
        weather_code=1,
        measurement_interval_seconds=900,
        event_timestamp=datetime.now(UTC),
        ingestion_timestamp=datetime.now(UTC),
    )


def _consumer(bootstrap: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"producer-integration-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )


def _high_watermarks(consumer: Consumer, topic: str) -> list[TopicPartition]:
    deadline = time.monotonic() + 15
    transient_codes = {3, 6}
    for name in (
        "_NOT_LEADER_FOR_PARTITION",
        "NOT_LEADER_FOR_PARTITION",
        "_UNKNOWN_TOPIC",
        "UNKNOWN_TOPIC",
        "_UNKNOWN_TOPIC_OR_PART",
        "UNKNOWN_TOPIC_OR_PART",
    ):
        code = getattr(KafkaError, name, None)
        if isinstance(code, int):
            transient_codes.add(code)

    result: list[TopicPartition] = []
    for partition in range(3):
        topic_partition = TopicPartition(topic, partition)
        last_transient: KafkaException | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_transient is not None:
                    raise last_transient
                raise TimeoutError("prazo para capturar high watermarks Kafka excedido")
            try:
                consumer.list_topics(topic=topic, timeout=min(2, remaining))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("prazo para capturar high watermarks Kafka excedido")
                _, high = consumer.get_watermark_offsets(
                    topic_partition,
                    timeout=min(5, remaining),
                )
                result.append(TopicPartition(topic, partition, high))
                break
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                error_code = getattr(error, "code", None)
                code = error_code() if callable(error_code) else None
                if code not in transient_codes:
                    raise
                last_transient = exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(0.1, remaining))
    return result

def test_real_kafka_fixture_is_delivered_with_key_value_timestamp_and_no_commits():
    bootstrap = _require_kafka()
    ensure_topics(bootstrap)
    consumer = _consumer(bootstrap)
    event = _sample_event()
    try:
        start_offsets = _high_watermarks(consumer, "weather.raw")
        consumer.assign(start_offsets)
        publisher = KafkaPublisher(bootstrap)
        try:
            publisher.enqueue(event)
            assert publisher.finish_batch() == 1
        finally:
            publisher.close()

        deadline = time.monotonic() + 20
        received = None
        while time.monotonic() < deadline and received is None:
            message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            if message.key() == event.location_id.encode("utf-8"):
                received = message
        assert received is not None, "o registro fixture não apareceu em weather.raw"
        assert received.value() == json.dumps(
            event.to_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        timestamp_type, timestamp_ms = received.timestamp()
        assert timestamp_type == 1
        assert timestamp_ms == int(event.ingestion_timestamp.timestamp() * 1000)
    finally:
        consumer.close()


def test_real_sdk_reports_delivery_failure_for_closed_tcp_port_within_deadline():
    _require_kafka()
    started = time.monotonic()
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        closed_port = server.getsockname()[1]
    publisher = KafkaPublisher(f"127.0.0.1:{closed_port}")
    try:
        publisher.enqueue(_sample_event())
        with pytest.raises(KafkaPublishError, match="delivery|Kafka|broker|timeout|pending"):
            publisher.finish_batch()
    finally:
        try:
            publisher.close()
        except KafkaPublishError:
            pass
    assert time.monotonic() - started <= 30


@pytest.mark.live
def test_once_cli_publishes_real_open_meteo_events_to_kafka():
    bootstrap = _require_kafka()
    if os.environ.get("OPEN_METEO_LIVE") != "1":
        pytest.skip("defina OPEN_METEO_LIVE=1 para consultar a API pública")
    ensure_topics(bootstrap)
    consumer = _consumer(bootstrap)
    try:
        offsets = _high_watermarks(consumer, "weather.raw")
        consumer.assign(offsets)
        environment = os.environ.copy()
        environment["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        environment["OPEN_METEO_LIVE"] = "1"
        source_path = os.path.abspath("src")
        environment["PYTHONPATH"] = os.pathsep.join(
            part for part in (source_path, environment.get("PYTHONPATH", "")) if part
        )
        started = datetime.now(UTC)
        result = subprocess.run(
            [sys.executable, "-m", "climate_logistics.producer", "--once"],
            capture_output=True,
            text=True,
            env=environment,
            timeout=180,
            check=False,
        )
        finished = datetime.now(UTC)
        assert result.returncode == 0, f"CLI --once falhou: {result.stdout[-2000:]}"
        expected_ids = {location.location_id for location in LOCATIONS}
        received: dict[str, tuple[dict[str, object], int]] = {}
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and received.keys() < expected_ids:
            message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            key = message.key().decode("utf-8") if message.key() else ""
            if key in expected_ids:
                received[key] = (json.loads(message.value()), message.timestamp()[1])
        assert received.keys() >= expected_ids
        for location_id in expected_ids:
            payload, timestamp_ms = received[location_id]
            ingestion_value = str(payload["ingestion_timestamp"])
            ingestion = datetime.fromisoformat(ingestion_value.replace("Z", "+00:00"))
            assert started <= ingestion <= finished
            assert timestamp_ms == int(ingestion.timestamp() * 1000)
    finally:
        consumer.close()
