"""Native PyFlink adapters for PostgreSQL business records and Kafka alerts."""

from __future__ import annotations

import json
import logging

from pyflink.common import Row, Types
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaSink,
)
from pyflink.datastream.functions import FlatMapFunction, MapFunction
from pyflink.java_gateway import get_gateway

from climate_logistics.business_config import BusinessSinkSettings
from climate_logistics.business_records import jdbc_values, make_alert, parse_assessment
from climate_logistics.business_sql import BUSINESS_UPSERT_SQL
from climate_logistics.locations import LOCATIONS

LOGGER = logging.getLogger(__name__)
BUSINESS_ALERT_TOPIC = "weather.alerts"
_LOCATION_IDS = frozenset(location.location_id for location in LOCATIONS)


class _FilterBusinessAssessments(FlatMapFunction):
    """Validate risk records and keep configured locations in the business branch."""

    def __init__(self) -> None:
        self._unknown_locations = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("business_sinks")
        self._unknown_locations = metrics.counter("unknown_locations")

    def flat_map(self, raw: str):
        assessment = parse_assessment(raw)
        location_id = assessment["location_id"]
        if location_id not in _LOCATION_IDS:
            self._unknown_locations.inc()
            LOGGER.warning(
                json.dumps(
                    {
                        "component": "flink",
                        "operation": "business_sinks",
                        "status": "rejected",
                        "reason": "unknown_location",
                        "location_id": location_id,
                        "metric": "unknown_locations",
                    },
                    separators=(",", ":"),
                )
            )
            return
        yield raw


class _PrepareJdbcRecord(MapFunction):
    """Prepare the two JDBC parameters without claiming sink delivery."""

    def __init__(self) -> None:
        self._prepared_records = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("business_sinks")
        self._prepared_records = metrics.counter("jdbc_records_prepared")

    def map(self, raw: str) -> Row:
        payload, eligible_alert_id = jdbc_values(parse_assessment(raw))
        row = Row(payload, eligible_alert_id)
        self._prepared_records.inc()
        return row


class _PrepareAlertRecord(FlatMapFunction):
    """Emit only eligible alert rows with deterministic UTF-8 JSON payloads."""

    def __init__(self) -> None:
        self._prepared_alerts = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("business_sinks")
        self._prepared_alerts = metrics.counter("alert_records_prepared")

    def flat_map(self, raw: str):
        assessment = parse_assessment(raw)
        alert = make_alert(assessment)
        if alert is None:
            return
        payload = json.dumps(
            alert,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        self._prepared_alerts.inc()
        yield Row(assessment["location_id"], payload)


def _jdbc_row_type():
    return Types.ROW([Types.STRING(), Types.STRING()])


def _alert_row_type():
    return Types.ROW([Types.STRING(), Types.STRING()])


def _jdbc_connection_options(settings: BusinessSinkSettings) -> JdbcConnectionOptions:
    gateway = get_gateway()
    connector_jdbc = gateway.jvm.org.apache.flink.connector.jdbc
    java_options = (
        connector_jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(settings.jdbc_url())
        .withDriverName("org.postgresql.Driver")
        .withUsername(settings.user)
        .withPassword(settings.password)
        .withConnectionCheckTimeoutSeconds(5)
        .build()
    )
    return JdbcConnectionOptions(java_options)


def _jdbc_execution_options(settings: BusinessSinkSettings) -> JdbcExecutionOptions:
    gateway = get_gateway()
    java_options = (
        gateway.jvm.org.apache.flink.connector.jdbc.JdbcExecutionOptions.builder()
        .withBatchSize(settings.batch_size)
        .withBatchIntervalMs(settings.batch_interval_ms)
        .withMaxRetries(settings.max_retries)
        .build()
    )
    return JdbcExecutionOptions(java_options)


def _kafka_producer_properties(settings: BusinessSinkSettings) -> dict[str, str]:
    """Return bounded producer retries while retaining at-least-once recovery."""
    return {
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "3",
        "request.timeout.ms": "10000",
        "delivery.timeout.ms": "30000",
        "max.block.ms": "10000",
        "max.in.flight.requests.per.connection": "5",
    }


def _kafka_delivery_guarantee() -> DeliveryGuarantee:
    return DeliveryGuarantee.AT_LEAST_ONCE


def filter_business_assessments(stream: DataStream) -> DataStream:
    """Validate the 21-field contract and reject unknown locations for business sinks."""
    return stream.flat_map(
        _FilterBusinessAssessments(),
        output_type=Types.STRING(),
    ).name("weather-business-records").uid("weather-business-records")


def attach_business_sinks(
    risk_stream: DataStream,
    settings: BusinessSinkSettings,
) -> None:
    """Attach native JDBC and Kafka sinks after validated risk assessments."""
    business_stream = filter_business_assessments(risk_stream)
    jdbc_records = business_stream.map(
        _PrepareJdbcRecord(),
        output_type=_jdbc_row_type(),
    ).name("weather-jdbc-records").uid("weather-jdbc-records")

    gateway = get_gateway()
    jdbc_statement_builder = gateway.jvm.org.climatelogistics.BusinessJdbcStatementBuilder()
    java_jdbc_sink = gateway.jvm.org.apache.flink.connector.jdbc.JdbcSink.sink(
        BUSINESS_UPSERT_SQL,
        jdbc_statement_builder,
        _jdbc_execution_options(settings)._j_jdbc_execution_options,
        _jdbc_connection_options(settings)._j_jdbc_connection_options,
    )
    jdbc_sink = JdbcSink(java_jdbc_sink)
    jdbc_records.add_sink(jdbc_sink).name("weather-postgres").uid("weather-postgres")

    alert_records = business_stream.flat_map(
        _PrepareAlertRecord(),
        output_type=_alert_row_type(),
    ).name("weather-alert-records").uid("weather-alert-records")
    java_serializer = gateway.jvm.org.climatelogistics.AlertRecordSerializationSchema(
        BUSINESS_ALERT_TOPIC
    )
    java_kafka_builder = (
        gateway.jvm.org.apache.flink.connector.kafka.sink.KafkaSink.builder()
        .setBootstrapServers(settings.kafka_bootstrap_servers)
        .setRecordSerializer(java_serializer)
        .setDeliveryGuarantee(
            gateway.jvm.org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE
        )
    )
    for key, value in _kafka_producer_properties(settings).items():
        java_kafka_builder.setProperty(key, value)
    kafka_sink = KafkaSink(java_kafka_builder.build())
    alert_records.sink_to(kafka_sink).name("weather-alerts").uid("weather-alerts")
