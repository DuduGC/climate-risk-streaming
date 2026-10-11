from __future__ import annotations

import importlib
import json

import pytest

pytest.importorskip("pyflink", reason="PyFlink SDK tests run in the Flink runtime image")

from pyflink.common import Types  # noqa: E402
from pyflink.datastream import StreamExecutionEnvironment  # noqa: E402
from pyflink.java_gateway import get_gateway  # noqa: E402


def _flink_business():
    try:
        return importlib.import_module("climate_logistics.flink_business")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.flink_business":
            pytest.fail("the PyFlink business sink adapter is not implemented yet", pytrace=False)
        raise


def _assessment(location_id: str = "RJ01", *, high_risk: bool = True) -> dict[str, object]:
    from climate_logistics.risk_engine import enrich_metric

    metric: dict[str, object] = {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": 1_791_633_600_000,
        "window_end_ms": 1_791_635_400_000,
        "event_count": 1,
        "temperature_avg": 20.0,
        "precipitation_max": 0.0,
        "precipitation_rate_max_mm_per_hour": 2.0 if high_risk else 0.0,
        "wind_speed_max": 40.0 if high_risk else 0.0,
        "wind_gust_max": 50.0 if high_risk else 0.0,
        "visibility_min": 6_000.0,
        "measurement_interval_min_seconds": 900,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_700_000,
        "last_event_timestamp_ms": 1_791_633_700_000,
    }
    return enrich_metric(metric)


def _settings():
    from climate_logistics.business_config import BusinessSinkSettings

    return BusinessSinkSettings(
        database="climate",
        user="climate_app",
        password="private-test-password",
        kafka_bootstrap_servers="kafka:9092",
    )


def _java_row(*fields: object):
    gateway = get_gateway()
    row = gateway.jvm.org.apache.flink.types.Row(len(fields))
    for index, value in enumerate(fields):
        row.setField(index, value)
    return row


class _Counter:
    def __init__(self) -> None:
        self.value = 0

    def inc(self) -> None:
        self.value += 1


class _Group:
    def __init__(self, groups: dict[str, _Group] | None = None) -> None:
        self.groups = {} if groups is None else groups
        self.counters: dict[str, _Counter] = {}

    def add_group(self, name: str) -> _Group:
        return self.groups.setdefault(name, _Group(self.groups))

    def counter(self, name: str) -> _Counter:
        return self.counters.setdefault(name, _Counter())


class _RuntimeContext:
    def __init__(self) -> None:
        self.metrics = _Group()

    def get_metrics_group(self) -> _Group:
        return self.metrics


@pytest.mark.unit
def test_business_filter_counts_unknown_location_and_propagates_contract_errors(caplog):
    business = _flink_business()
    function = business._FilterBusinessAssessments()
    context = _RuntimeContext()
    function.open(context)
    known = json.dumps(_assessment(), ensure_ascii=False, separators=(",", ":"))
    unknown = json.dumps(
        _assessment("QA-UNKNOWN"), ensure_ascii=False, separators=(",", ":")
    )

    assert list(function.flat_map(known)) == [known]
    assert list(function.flat_map(unknown)) == []
    counter = context.metrics.groups["business_sinks"].counters["unknown_locations"]
    assert counter.value == 1
    diagnostic = json.loads(caplog.records[-1].message)
    assert diagnostic["operation"] == "business_sinks"
    assert diagnostic["metric"] == "unknown_locations"
    assert diagnostic["location_id"] == "QA-UNKNOWN"
    with pytest.raises(ValueError):
        list(function.flat_map("{invalid-json"))


@pytest.mark.unit
def test_business_filter_is_a_real_string_datastream_transform():
    business = _flink_business()
    env = StreamExecutionEnvironment.get_execution_environment()
    source = env.from_collection(
        [json.dumps(_assessment(), separators=(",", ":"))],
        type_info=Types.STRING(),
    )

    filtered = business.filter_business_assessments(source)

    assert str(filtered.get_type()) == str(Types.STRING())
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )
    assert "weather-business-records" in descriptions


@pytest.mark.unit
def test_jdbc_preparation_uses_two_string_columns_and_the_task1_parameterized_sql():
    business = _flink_business()
    preparer = business._PrepareJdbcRecord()
    preparer.open(_RuntimeContext())
    from climate_logistics.business_sql import BUSINESS_UPSERT_SQL

    jdbc_type = business._jdbc_row_type()
    prepared = preparer.map(json.dumps(_assessment(), separators=(",", ":")))

    assert str(jdbc_type) == str(Types.ROW([Types.STRING(), Types.STRING()]))
    assert len(prepared) == 2
    assert json.loads(prepared[0]).keys() == _assessment().keys()
    assert prepared[1] is not None
    assert BUSINESS_UPSERT_SQL.count("?") == 2


@pytest.mark.unit
def test_kafka_preparation_uses_the_same_row_type_and_emits_only_the_22_field_alert():
    business = _flink_business()
    preparer = business._PrepareAlertRecord()
    preparer.open(_RuntimeContext())

    alert_type = business._alert_row_type()
    prepared = list(preparer.flat_map(json.dumps(_assessment(), separators=(",", ":"))))

    assert str(alert_type) == str(Types.ROW([Types.STRING(), Types.STRING()]))
    assert len(prepared) == 1
    assert prepared[0][0] == "RJ01"
    payload = json.loads(prepared[0][1])
    assert len(payload) == 22
    assert payload["operation"] == "weather_alert"
    assert payload["alert_id"]


@pytest.mark.unit
def test_jdbc_sink_sdk_options_keep_batching_retries_and_bounded_connection_validation():
    business = _flink_business()
    settings = _settings()

    connection = business._jdbc_connection_options(settings)
    execution = business._jdbc_execution_options(settings)
    j_connection = connection._j_jdbc_connection_options
    j_execution = execution._j_jdbc_execution_options

    assert j_connection.getConnectionCheckTimeoutSeconds() == 5
    assert j_connection.getDbURL() == settings.jdbc_url()
    assert j_execution.getBatchSize() == 100
    assert j_execution.getBatchIntervalMs() == 2000
    assert j_execution.getMaxRetries() == 3


@pytest.mark.unit
def test_jdbc_binder_uses_public_connector_contract_and_preserves_null_alert_id():
    gateway = get_gateway()
    builder_type = gateway.jvm.org.climatelogistics.BusinessJdbcStatementBuilder
    builder = builder_type()
    jdbc_statement_builder = gateway.jvm.java.lang.Class.forName(
        "org.apache.flink.connector.jdbc.JdbcStatementBuilder"
    )
    payload = '{"risk_score":50}'
    jvm_row = _java_row(payload, None)

    parameters = builder.getParameters(jvm_row)

    assert jdbc_statement_builder.isAssignableFrom(builder.getClass())
    parameter_types = [
        tuple(parameter.getName() for parameter in method.getParameterTypes())
        for method in builder.getClass().getMethods()
        if method.getName() == "getParameters"
    ]
    assert parameter_types == [("org.apache.flink.types.Row",)]
    assert parameters[0] == payload
    assert parameters[1] is None


@pytest.mark.unit
@pytest.mark.parametrize("row_values", [("RJ01",), ("RJ01", 1), (None, None)])
def test_jdbc_binder_rejects_rows_outside_two_string_columns_with_nullable_alert_id(
    row_values,
):
    gateway = get_gateway()
    builder = gateway.jvm.org.climatelogistics.BusinessJdbcStatementBuilder()
    jvm_row = _java_row(*row_values)

    with pytest.raises(Exception):
        builder.getParameters(jvm_row)


@pytest.mark.unit
def test_kafka_sink_uses_at_least_once_and_bounded_idempotent_producer_properties():
    business = _flink_business()

    properties = business._kafka_producer_properties(_settings())

    assert properties == {
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "3",
        "request.timeout.ms": "10000",
        "delivery.timeout.ms": "30000",
        "max.block.ms": "10000",
        "max.in.flight.requests.per.connection": "5",
    }
    assert (
        business._kafka_delivery_guarantee()
        is business.DeliveryGuarantee.AT_LEAST_ONCE
    )
    assert all("password" not in key.lower() for key in properties)
    assert "private-test-password" not in repr(properties)


@pytest.mark.unit
def test_alert_serializer_emits_location_key_and_exact_json_value_as_utf8():
    gateway = get_gateway()
    serializer_type = gateway.jvm.org.climatelogistics.AlertRecordSerializationSchema
    schema = serializer_type("weather.alerts")
    payload = json.dumps(
        {"operation": "weather_alert", "alert_id": "abc", "city": "São Paulo"},
        ensure_ascii=False,
        separators=(",", ":"),
    )

    jvm_row = _java_row("SP01", payload)
    record = schema.serialize(jvm_row, None, 17)
    key = gateway.jvm.java.lang.String(record.key(), "UTF-8")
    value = gateway.jvm.java.lang.String(record.value(), "UTF-8")

    assert record.topic() == "weather.alerts"
    assert key == "SP01"
    assert value == payload


@pytest.mark.unit
@pytest.mark.parametrize(
    "row_values",
    [("RJ01",), ("RJ01", None), (1, "{}")],
)
def test_alert_serializer_rejects_rows_outside_the_two_non_null_string_contract(row_values):
    gateway = get_gateway()
    schema = gateway.jvm.org.climatelogistics.AlertRecordSerializationSchema("weather.alerts")

    row = _java_row(*row_values)
    with pytest.raises(Exception):
        schema.serialize(row, None, 17)


@pytest.mark.unit
def test_operational_graph_keeps_legacy_state_uids_and_adds_native_business_sinks():
    from climate_logistics.flink_job import JobSettings, build_pipeline

    env = build_pipeline(JobSettings(), business_sinks=_settings())
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )

    for uid in (
        "weather-raw-source",
        "weather-json-parser",
        "weather-event-deduplication",
        "weather-event-time",
        "weather-temporal-windows",
        "weather-late-counter",
        "weather-late-discard",
        "weather-risk-assessment",
        "weather-business-records",
        "weather-jdbc-records",
        "weather-postgres",
        "weather-alert-records",
        "weather-alerts",
    ):
        assert uid in descriptions


@pytest.mark.unit
def test_legacy_sdk_build_without_business_settings_does_not_add_business_sinks():
    from climate_logistics.flink_job import JobSettings, build_pipeline

    env = build_pipeline(JobSettings(), business_sinks=None)
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )

    for uid in (
        "weather-raw-source",
        "weather-event-deduplication",
        "weather-risk-assessment",
        "weather-ingestion",
        "weather-metrics",
        "weather-risk",
    ):
        assert uid in descriptions
    assert "weather-postgres" not in descriptions
    assert "weather-alerts" not in descriptions


@pytest.mark.unit
def test_qa_business_sink_opt_in_rejects_missing_audit_or_exclusive_identity_before_credentials(
    monkeypatch: pytest.MonkeyPatch,
):
    from climate_logistics import flink_job

    for variable in ("APP_DB_USER", "APP_DB_PASSWORD", "POSTGRES_DB"):
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(ValueError, match="--qa-audit"):
        flink_job.main(
            [
                "--qa-business-sinks",
                "--job-name",
                "weather-flink-qa-sinkgate1",
                "--group-id",
                "weather-flink-qa-sinkgate1-group",
            ]
        )

    with pytest.raises(ValueError, match="weather-flink-qa"):
        flink_job.main(
            [
                "--qa-business-sinks",
                "--qa-audit",
                "--job-name",
                "weather-kafka-ingestion",
                "--group-id",
                "weather-flink-ingestion",
            ]
        )


@pytest.mark.unit
def test_qa_prefixed_execute_without_audit_or_sink_opt_in_keeps_business_sinks_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    from climate_logistics import flink_job

    monkeypatch.setattr(
        flink_job.BusinessSinkSettings,
        "from_environment",
        classmethod(lambda cls, *, bootstrap_servers: pytest.fail("credentials were loaded")),
    )
    captured: dict[str, object] = {}

    class _Result:
        def get_job_id(self) -> str:
            return "00112233445566778899aabbccddeeff"

    class _Environment:
        def execute(self, job_name: str) -> _Result:
            return _Result()

    def capture_build(settings, *, business_sinks):
        captured["settings"] = settings
        captured["business_sinks"] = business_sinks
        return _Environment()

    monkeypatch.setattr(flink_job, "build_pipeline", capture_build)
    job_name = "weather-flink-qa-noaudit"
    settings = flink_job.JobSettings(group_id=f"{job_name}-group", bounded=True)

    flink_job.execute(job_name, settings)

    assert captured["business_sinks"] is None


@pytest.mark.unit
def test_legacy_qa_execute_keeps_business_sinks_disabled_without_loading_credentials(
    monkeypatch: pytest.MonkeyPatch,
):
    from climate_logistics import flink_job

    for variable in ("APP_DB_USER", "APP_DB_PASSWORD", "POSTGRES_DB"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(
        flink_job.BusinessSinkSettings,
        "from_environment",
        classmethod(lambda cls, *, bootstrap_servers: pytest.fail("credentials were loaded")),
    )
    captured: dict[str, object] = {}

    class _Result:
        def get_job_id(self) -> str:
            return "0123456789abcdef0123456789abcdef"

    class _Environment:
        def execute(self, job_name: str) -> _Result:
            captured["job_name"] = job_name
            return _Result()

    def capture_build(settings, *, business_sinks):
        captured["settings"] = settings
        captured["business_sinks"] = business_sinks
        return _Environment()

    monkeypatch.setattr(flink_job, "build_pipeline", capture_build)
    job_name = "weather-flink-qa-legacybusiness"
    settings = flink_job.JobSettings(
        qa_audit=True,
        group_id=f"{job_name}-group",
    )

    flink_job.execute(job_name, settings)

    assert captured["job_name"] == job_name
    assert captured["business_sinks"] is None


@pytest.mark.unit
def test_operational_execute_loads_application_settings_and_passes_them_to_the_graph(
    monkeypatch: pytest.MonkeyPatch,
):
    from climate_logistics import flink_job

    expected = _settings()
    loaded: list[str] = []
    monkeypatch.setattr(
        flink_job.BusinessSinkSettings,
        "from_environment",
        classmethod(
            lambda cls, *, bootstrap_servers: loaded.append(bootstrap_servers) or expected
        ),
    )
    captured: dict[str, object] = {}

    class _Result:
        def get_job_id(self) -> str:
            return "fedcba9876543210fedcba9876543210"

    class _Environment:
        def execute(self, job_name: str) -> _Result:
            return _Result()

    def capture_build(settings, *, business_sinks):
        captured["settings"] = settings
        captured["business_sinks"] = business_sinks
        return _Environment()

    monkeypatch.setattr(flink_job, "build_pipeline", capture_build)

    flink_job.execute("weather-kafka-ingestion")

    assert loaded == [flink_job.DEFAULT_BOOTSTRAP_SERVERS]
    assert isinstance(captured["business_sinks"], flink_job.BusinessSinkSettings)


@pytest.mark.unit
def test_qa_business_sink_opt_in_loads_credentials_only_after_identity_validation(
    monkeypatch: pytest.MonkeyPatch,
):
    from climate_logistics import flink_job

    expected = _settings()
    loads: list[str] = []

    def load_settings(cls, *, bootstrap_servers):
        loads.append(bootstrap_servers)
        return expected

    monkeypatch.setattr(
        flink_job.BusinessSinkSettings,
        "from_environment",
        classmethod(load_settings),
    )
    calls: list[tuple[str, object, object]] = []
    monkeypatch.setattr(
        flink_job,
        "execute",
        lambda job_name, settings, *, business_sinks=None: calls.append(
            (job_name, settings, business_sinks)
        ),
    )
    job_name = "weather-flink-qa-businessoptin"

    result = flink_job.main(
        [
            "--qa-business-sinks",
            "--qa-audit",
            "--job-name",
            job_name,
            "--group-id",
            f"{job_name}-group",
        ]
    )

    assert result == 0
    assert loads == [flink_job.DEFAULT_BOOTSTRAP_SERVERS]
    assert calls[0][0] == job_name
    assert calls[0][2] is expected
