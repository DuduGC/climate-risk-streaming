from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime

import pytest

from climate_logistics import producer
from climate_logistics.models import WeatherEvent

pytestmark = pytest.mark.unit

INGESTION = datetime(2026, 10, 8, 12, 34, 56, tzinfo=UTC)


def event(location_id: str) -> WeatherEvent:
    return WeatherEvent(
        location_id=location_id,
        city="Cidade",
        latitude=-22.9,
        longitude=-43.17,
        temperature=27.4,
        precipitation=0,
        wind_speed=10,
        wind_gust=12,
        visibility=9000,
        weather_code=1,
        measurement_interval_seconds=900,
        event_timestamp=datetime(2026, 10, 8, 12, 30, tzinfo=UTC),
        ingestion_timestamp=INGESTION,
    )


class FakeClient:
    def __init__(self, events: dict[str, WeatherEvent], errors: set[str] | None = None) -> None:
        self.events = events
        self.errors = set() if errors is None else errors
        self.calls: list[str] = []
        self.closed = False
        self.on_fetch = None

    def fetch(self, location):
        self.calls.append(location.location_id)
        if self.on_fetch:
            self.on_fetch(location)
        if location.location_id in self.errors:
            raise RuntimeError("HTTP unavailable")
        return self.events[location.location_id]

    def close(self) -> None:
        self.closed = True


class FakePublisher:
    def __init__(
        self,
        *,
        delivered: int | None = None,
        enqueue_error: Exception | None = None,
        finish_error: Exception | None = None,
    ) -> None:
        self.events: list[WeatherEvent] = []
        self.delivered = delivered
        self.enqueue_error = enqueue_error
        self.finish_error = finish_error
        self.finished = 0
        self.closed = False

    def enqueue(self, item: WeatherEvent) -> None:
        if self.enqueue_error:
            raise self.enqueue_error
        self.events.append(item)

    def finish_batch(self) -> int:
        self.finished += 1
        if self.finish_error:
            raise self.finish_error
        return len(self.events) if self.delivered is None else self.delivered

    def close(self) -> None:
        self.closed = True

def test_settings_use_public_defaults_and_accept_interval_boundaries():
    assert producer.ProducerSettings.from_env({}) == producer.ProducerSettings(
        "kafka:9092", 300
    )
    assert producer.ProducerSettings.from_env(
        {"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", "WEATHER_POLL_INTERVAL_SECONDS": "1"}
    ).poll_interval_seconds == 1
    assert producer.ProducerSettings.from_env(
        {"WEATHER_POLL_INTERVAL_SECONDS": "86400"}
    ).poll_interval_seconds == 86400


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "https://kafka:9092",
        "kafka:9092/path",
        "user:secret@kafka:9092",
        "kafka:9092, kafka2:9092",
    ],
)
def test_settings_reject_invalid_bootstrap_without_echoing_value(value: str):
    with pytest.raises(producer.ProducerConfigurationError) as error:
        producer.ProducerSettings.from_env({"KAFKA_BOOTSTRAP_SERVERS": value})

    assert "secret" not in str(error.value)


@pytest.mark.parametrize("value", ["", "0", "-1", "1.5", "one", "86401"])
def test_settings_reject_invalid_poll_interval(value: str):
    with pytest.raises(producer.ProducerConfigurationError):
        producer.ProducerSettings.from_env({"WEATHER_POLL_INTERVAL_SECONDS": value})


def test_run_cycle_publishes_all_locations_and_returns_true_when_all_are_confirmed(caplog):
    events = {name: event(name) for name in ("RJ01", "SP01", "CT01")}
    client = FakeClient(events)
    publisher = FakePublisher()
    logger = logging.getLogger("producer-unit-success")

    with caplog.at_level(logging.INFO, logger=logger.name):
        complete = producer.run_cycle(
            client, publisher, stop=threading.Event(), logger=logger
        )

    assert complete is True
    assert client.calls == ["RJ01", "SP01", "CT01"]
    assert publisher.events == list(events.values())
    assert publisher.finished == 1
    records = [json.loads(record.message) for record in caplog.records]
    assert all(record["component"] == "producer" for record in records)
    assert any(
        record["operation"] == "cycle" and record["status"] == "success"
        for record in records
    )


def test_run_cycle_continues_after_one_location_failure_and_reports_partial():
    events = {name: event(name) for name in ("RJ01", "CT01")}
    client = FakeClient(events, errors={"SP01"})
    publisher = FakePublisher()

    complete = producer.run_cycle(
        client, publisher, stop=threading.Event(), logger=logging.getLogger("producer-unit-partial")
    )

    assert complete is False
    assert client.calls == ["RJ01", "SP01", "CT01"]
    assert [item.location_id for item in publisher.events] == ["RJ01", "CT01"]
    assert publisher.finished == 1


def test_run_cycle_returns_false_if_a_valid_event_was_not_confirmed():
    client = FakeClient({name: event(name) for name in ("RJ01", "SP01", "CT01")})
    publisher = FakePublisher(delivered=2)

    assert producer.run_cycle(
        client, publisher, stop=threading.Event(),
        logger=logging.getLogger("producer-unit-unconfirmed"),
    ) is False


def test_run_cycle_stops_starting_new_fetches_after_stop_is_requested():
    stop = threading.Event()
    client = FakeClient({"RJ01": event("RJ01")})
    client.on_fetch = lambda location: stop.set()
    publisher = FakePublisher()

    complete = producer.run_cycle(
        client, publisher, stop=stop, logger=logging.getLogger("producer-unit-stop")
    )

    assert complete is False
    assert client.calls == ["RJ01"]
    assert [item.location_id for item in publisher.events] == ["RJ01"]
    assert publisher.finished == 1


def test_run_cycle_closes_batch_before_propagating_kafka_failure():
    client = FakeClient({name: event(name) for name in ("RJ01", "SP01", "CT01")})
    publisher = FakePublisher(enqueue_error=producer.KafkaPublishError("broker unavailable"))

    with pytest.raises(producer.KafkaPublishError, match="broker unavailable"):
        producer.run_cycle(
            client,
            publisher,
            stop=threading.Event(),
            logger=logging.getLogger("producer-unit-fatal"),
        )

    assert publisher.finished == 1


def test_main_once_returns_zero_only_for_a_complete_confirmed_cycle(monkeypatch):
    client = FakeClient({name: event(name) for name in ("RJ01", "SP01", "CT01")})
    publisher = FakePublisher()
    monkeypatch.setattr(producer, "ensure_topics", lambda bootstrap: None)
    monkeypatch.setattr(producer, "KafkaPublisher", lambda bootstrap, **kwargs: publisher)
    monkeypatch.setattr(producer, "OpenMeteoClient", lambda: client)

    assert producer.main(["--once"]) == 0
    assert publisher.closed is True
    assert client.closed is True


def test_main_emits_parseable_json_logs_to_stdout(capsys, monkeypatch):
    client = FakeClient({name: event(name) for name in ("RJ01", "SP01", "CT01")})
    publisher = FakePublisher()
    monkeypatch.setattr(producer, "ensure_topics", lambda bootstrap: None)
    monkeypatch.setattr(producer, "KafkaPublisher", lambda bootstrap, **kwargs: publisher)
    monkeypatch.setattr(producer, "OpenMeteoClient", lambda: client)

    assert producer.main(["--once"]) == 0

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert any(
        record["component"] == "producer"
        and record["operation"] == "cycle"
        and record["status"] == "success"
        for record in records
    )

def test_main_once_logs_two_confirmed_deliveries_when_third_nacks(capsys, monkeypatch):
    client = FakeClient({name: event(name) for name in ("RJ01", "SP01", "CT01")})
    error = producer.KafkaPublishError("delivery callback returned an error")
    error.delivered_count = 2
    publisher = FakePublisher(finish_error=error)
    monkeypatch.setattr(producer, "ensure_topics", lambda bootstrap: None)
    monkeypatch.setattr(producer, "KafkaPublisher", lambda bootstrap, **kwargs: publisher)
    monkeypatch.setattr(producer, "OpenMeteoClient", lambda: client)

    assert producer.main(["--once"]) == 1

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    failed_cycles = [
        record for record in records
        if record["operation"] == "cycle" and record["status"] == "failed"
    ]
    assert failed_cycles[-1]["delivered"] == 2

def test_main_once_returns_nonzero_for_partial_http_cycle(monkeypatch):
    client = FakeClient(
        {"RJ01": event("RJ01"), "CT01": event("CT01")}, errors={"SP01"}
    )
    publisher = FakePublisher()
    monkeypatch.setattr(producer, "ensure_topics", lambda bootstrap: None)
    monkeypatch.setattr(producer, "KafkaPublisher", lambda bootstrap, **kwargs: publisher)
    monkeypatch.setattr(producer, "OpenMeteoClient", lambda: client)

    assert producer.main(["--once"]) == 1
    assert publisher.closed is True
    assert client.closed is True
