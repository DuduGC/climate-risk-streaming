from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid

import pytest

from climate_logistics.business_records import make_alert

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("ALERT_CONSUMER_INTEGRATION") != "1",
        reason="set ALERT_CONSUMER_INTEGRATION=1 to use the local Kafka broker",
    ),
]

_TOPIC = "weather.alerts"
_DEADLINE_SECONDS = 90.0


def _alert(window_start_ms: int) -> dict[str, object]:
    assessment: dict[str, object] = {
        "component": "flink",
        "operation": "risk_assessment",
        "schema_version": 1,
        "location_id": "RJ01",
        "window_start_ms": window_start_ms,
        "window_end_ms": window_start_ms + 1_800_000,
        "event_count": 3,
        "temperature_avg": 22.5,
        "precipitation_max": 5.0,
        "precipitation_rate_max_mm_per_hour": 20.0,
        "wind_speed_max": 0.0,
        "wind_gust_max": 0.0,
        "visibility_min": 10_000.0,
        "measurement_interval_min_seconds": 300,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": window_start_ms + 20_000,
        "last_event_timestamp_ms": window_start_ms + 1_300_000,
        "risk_score": 50,
        "risk_level": "HIGH",
        "risk_components": {
            "precipitation_rate": 50,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
        "risk_rules_version": "mvp-1",
    }
    alert = make_alert(assessment)
    assert alert is not None
    identity = [
        alert["location_id"],
        alert["window_start_ms"],
        alert["window_end_ms"],
        alert["risk_rules_version"],
    ]
    expected_id = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert alert["alert_id"] == expected_id
    return alert


def _docker(arguments: list[str], *, timeout: float = 30.0) -> str:
    result = subprocess.run(
        ["docker", *arguments],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("alert-consumer Docker QA command failed")
    return result.stdout.strip()


def _start_consumer(container_name: str, group_id: str) -> None:
    _docker(
        [
            "compose",
            "run",
            "-T",
            "-d",
            "--no-deps",
            "--name",
            container_name,
            "-e",
            f"ALERT_CONSUMER_GROUP_ID={group_id}",
            "alert-consumer",
        ]
    )


def _wait_healthy(container_name: str, timeout: float = _DEADLINE_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _docker(
            [
                "inspect",
                "--format",
                (
                    "{{.State.Status}}|{{if .State.Health}}"
                    "{{.State.Health.Status}}{{else}}missing{{end}}"
                ),
                container_name,
            ]
        )
        state, health = status.split("|", maxsplit=1)
        if state != "running":
            raise RuntimeError("alert-consumer QA container exited")
        if health == "healthy":
            return
        time.sleep(1)
    raise TimeoutError("alert-consumer QA health deadline exceeded")


def _logs(container_name: str) -> tuple[str, list[dict[str, object]]]:
    raw = _docker(["logs", container_name])
    records: list[dict[str, object]] = []
    for line in raw.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            records.append(item)
    return raw, records


def _run_client_script(code: str, environment: dict[str, str], *, timeout: float = 60) -> str:
    command = [
        "compose",
        "run",
        "-T",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
    ]
    for name, value in environment.items():
        command.extend(["-e", f"{name}={value}"])
    command.extend(["alert-consumer", "-c", code])
    return _docker(command, timeout=timeout)


def _publish(fixtures: list[dict[str, str]]) -> list[dict[str, int | str]]:
    code = """
import json, os, sys
from confluent_kafka import Producer
items = json.loads(os.environ['ALERT_CONSUMER_FIXTURES'])
acks, failures = [], []
producer = Producer({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')})
for index, item in enumerate(items):
    producer.produce(
        'weather.alerts',
        key=item['key'].encode('utf-8'),
        value=item['value'].encode('utf-8'),
        on_delivery=lambda error, message, index=index: (
            failures.append(index) if error else acks.append({
                'index': index,
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
            })
        ),
    )
pending = producer.flush(30)
if pending or failures or len(acks) != len(items):
    sys.exit(1)
print(json.dumps(sorted(acks, key=lambda item: item['index']), separators=(',', ':')))
"""
    result = _run_client_script(
        code,
        {"ALERT_CONSUMER_FIXTURES": json.dumps(fixtures, separators=(",", ":"))},
    )
    return json.loads(result.splitlines()[-1])


def _committed_offsets(group_id: str, partitions: list[int]) -> dict[int, int]:
    code = """
import json, os
from confluent_kafka import Consumer, TopicPartition
consumer = Consumer({
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': os.environ['ALERT_CONSUMER_GROUP_ID'],
    'enable.auto.commit': False,
})
try:
    items = consumer.committed(
        [
            TopicPartition('weather.alerts', partition)
            for partition in json.loads(os.environ['ALERT_CONSUMER_PARTITIONS'])
        ],
        timeout=5,
    )
    print(json.dumps({str(item.partition): item.offset for item in items}, separators=(',', ':')))
finally:
    consumer.close()
"""
    result = _run_client_script(
        code,
        {
            "ALERT_CONSUMER_GROUP_ID": group_id,
            "ALERT_CONSUMER_PARTITIONS": json.dumps(partitions, separators=(",", ":")),
        },
    )
    offsets = json.loads(result.splitlines()[-1])
    return {int(partition): offset for partition, offset in offsets.items()}


def _wait_for_offsets(
    container_name: str,
    acknowledgements: list[dict[str, int | str]],
    expected_operations: list[str],
    expected_alert_ids: list[str | None],
    raw_marker: str | None = None,
) -> list[dict[str, object]]:
    expected_keys = {
        (str(ack["topic"]), int(ack["partition"]), int(ack["offset"]))
        for ack in acknowledgements
    }
    deadline = time.monotonic() + _DEADLINE_SECONDS
    while time.monotonic() < deadline:
        raw, records = _logs(container_name)
        matching: dict[tuple[object, object, object], list[dict[str, object]]] = {}
        for record in records:
            key = (record.get("topic"), record.get("partition"), record.get("offset"))
            if key in expected_keys:
                matching.setdefault(key, []).append(record)
        if any(len(items) > 1 for items in matching.values()):
            raise AssertionError("alert-consumer QA logged a fixture offset more than once")
        if len(matching) == len(expected_keys):
            by_key = {
                (str(ack["topic"]), int(ack["partition"]), int(ack["offset"])): index
                for index, ack in enumerate(acknowledgements)
            }
            for key, items in matching.items():
                record = items[0]
                index = by_key[key]
                if record.get("operation") != expected_operations[index]:
                    raise AssertionError("alert-consumer QA operation mismatch")
                alert_id = expected_alert_ids[index]
                if alert_id is not None:
                    alert = record.get("alert")
                    if not isinstance(alert, dict) or alert.get("alert_id") != alert_id:
                        raise AssertionError("alert-consumer QA alert identity mismatch")
                elif "raw" in record or "alert" in record:
                    raise AssertionError("alert-consumer QA invalid record exposed its payload")
            if raw_marker is not None and raw_marker in raw:
                raise AssertionError("alert-consumer QA log exposed raw invalid data")
            return records
        time.sleep(0.5)
    raise TimeoutError("alert-consumer QA log deadline exceeded")


def _assert_commits(acks: list[dict[str, int | str]], group_id: str) -> None:
    expected: dict[int, int] = {}
    for ack in acks:
        partition = int(ack["partition"])
        expected[partition] = max(expected.get(partition, 0), int(ack["offset"]) + 1)
    committed = _committed_offsets(group_id, list(expected))
    if any(committed.get(partition, -1) < offset for partition, offset in expected.items()):
        raise AssertionError("alert-consumer QA group did not commit fixture offsets")


def _remove_consumer(container_name: str) -> None:
    result = subprocess.run(
        ["docker", "rm", "--force", container_name],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0 and "No such container" not in result.stderr:
        raise RuntimeError("alert-consumer QA cleanup failed")


def test_alert_consumer_delivery_dedup_invalid_idle_health_and_group_resume():
    unique = uuid.uuid4().hex[:12]
    group_id = f"weather-alerts-qa-{unique}"
    first_container = f"alert-consumer-qa-{unique}-first"
    restarted_container = f"alert-consumer-qa-{unique}-restart"
    started_containers: list[str] = []
    now_ms = int(time.time() * 1000)
    window_start_ms = now_ms // 1_800_000 * 1_800_000 + int(unique[:4], 16)
    first_alert = _alert(window_start_ms)
    second_alert = _alert(window_start_ms + 1_800_000)
    equivalent = dict(reversed(tuple(first_alert.items())))
    equivalent["precipitation_rate_max_mm_per_hour"] = 20
    raw_marker = f"qa-invalid-{unique}"
    fixtures = [
        {"key": "RJ01", "value": json.dumps(first_alert, separators=(",", ":"))},
        {"key": "RJ01", "value": json.dumps(equivalent, separators=(",", ":"))},
        {"key": "RJ01", "value": json.dumps({"raw": raw_marker}, separators=(",", ":"))},
    ]

    try:
        started_containers.append(first_container)
        _start_consumer(first_container, group_id)
        _wait_healthy(first_container)

        first_acks = _publish(fixtures)
        _wait_for_offsets(
            first_container,
            first_acks,
            ["alert_received", "alert_duplicate", "alert_invalid"],
            [str(first_alert["alert_id"]), str(first_alert["alert_id"]), None],
            raw_marker=raw_marker,
        )
        time.sleep(2)
        _wait_healthy(first_container)
        _assert_commits(first_acks, group_id)

        _remove_consumer(first_container)
        started_containers.remove(first_container)
        started_containers.append(restarted_container)
        _start_consumer(restarted_container, group_id)
        _wait_healthy(restarted_container)
        time.sleep(2)
        old_offsets = {
            (str(ack["topic"]), int(ack["partition"]), int(ack["offset"]))
            for ack in first_acks
        }
        _, restarted_records = _logs(restarted_container)
        if any(
            (record.get("topic"), record.get("partition"), record.get("offset")) in old_offsets
            for record in restarted_records
        ):
            raise AssertionError("alert-consumer QA replayed a committed fixture after restart")

        second_fixtures = [
            {"key": "RJ01", "value": json.dumps(second_alert, separators=(",", ":"))}
        ]
        second_acks = _publish(second_fixtures)
        _wait_for_offsets(
            restarted_container,
            second_acks,
            ["alert_received"],
            [str(second_alert["alert_id"])],
        )
        time.sleep(2)
        _wait_healthy(restarted_container)
        _assert_commits(second_acks, group_id)
    finally:
        for container_name in started_containers:
            _remove_consumer(container_name)
