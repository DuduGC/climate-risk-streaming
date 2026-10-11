from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest


def _business_config():
    try:
        from climate_logistics.business_config import BusinessSinkSettings
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.business_config":
            pytest.fail("business sink configuration has not been implemented yet", pytrace=False)
        raise
    return BusinessSinkSettings


def _set_valid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_DB_USER", "climate_app")
    monkeypatch.setenv("APP_DB_PASSWORD", "private-test-password")
    monkeypatch.setenv("POSTGRES_DB", "climate")
    monkeypatch.setenv("POSTGRES_USER", "climate_admin")


@pytest.mark.unit
def test_environment_configuration_uses_fixed_safe_defaults_and_public_jdbc_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_environment(monkeypatch)
    settings_type = _business_config()

    settings = settings_type.from_environment(bootstrap_servers="kafka:9092")

    assert settings.database == "climate"
    assert settings.user == "climate_app"
    assert settings.host == "postgres"
    assert settings.port == 5432
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.jdbc_connect_timeout_seconds == 10
    assert settings.jdbc_socket_timeout_seconds == 10
    assert settings.batch_size == 100
    assert settings.batch_interval_ms == 2000
    assert settings.max_retries == 3
    assert settings.jdbc_url() == (
        "jdbc:postgresql://postgres:5432/climate"
        "?connectTimeout=10&socketTimeout=10&reWriteBatchedInserts=false"
    )
    assert "climate_app" not in settings.jdbc_url()
    assert "private-test-password" not in settings.jdbc_url()
    assert "private-test-password" not in repr(settings)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("APP_DB_USER", ""),
        ("APP_DB_USER", "   "),
        ("APP_DB_PASSWORD", ""),
        ("APP_DB_PASSWORD", "  "),
        ("POSTGRES_DB", ""),
        ("POSTGRES_DB", "\t"),
    ],
)
def test_environment_configuration_rejects_missing_or_blank_required_values_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    _set_valid_environment(monkeypatch)
    monkeypatch.setenv(variable, value)
    settings_type = _business_config()

    with pytest.raises(ValueError) as raised:
        settings_type.from_environment(bootstrap_servers="kafka:9092")

    assert "private-test-password" not in str(raised.value)


@pytest.mark.unit
@pytest.mark.parametrize("variable", ["APP_DB_USER", "APP_DB_PASSWORD", "POSTGRES_DB"])
def test_environment_configuration_requires_all_database_variables(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
) -> None:
    _set_valid_environment(monkeypatch)
    monkeypatch.delenv(variable)
    settings_type = _business_config()

    with pytest.raises(ValueError) as raised:
        settings_type.from_environment(bootstrap_servers="kafka:9092")

    assert "private-test-password" not in str(raised.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("database", "user"),
    [
        ("climate;DROP TABLE climate.locations", "climate_app"),
        ("climate", "app user"),
        ("climate", "climate_admin"),
        ("climate", "postgres"),
    ],
)
def test_environment_configuration_rejects_unsafe_identifiers_and_admin_identity(
    monkeypatch: pytest.MonkeyPatch,
    database: str,
    user: str,
) -> None:
    _set_valid_environment(monkeypatch)
    monkeypatch.setenv("POSTGRES_DB", database)
    monkeypatch.setenv("APP_DB_USER", user)
    settings_type = _business_config()

    with pytest.raises(ValueError):
        settings_type.from_environment(bootstrap_servers="kafka:9092")


@pytest.mark.unit
@pytest.mark.parametrize("port", [0, 65536, True, 5432.0, "5432"])
def test_connection_port_must_be_a_typed_tcp_port(port: object) -> None:
    settings_type = _business_config()

    with pytest.raises((TypeError, ValueError)):
        settings_type(
            database="climate",
            user="climate_app",
            password="private-test-password",
            kafka_bootstrap_servers="kafka:9092",
            port=port,
        )


@pytest.mark.unit
@pytest.mark.parametrize("port", [1, 65535])
def test_connection_port_accepts_only_documented_range_endpoints(port: int) -> None:
    settings_type = _business_config()
    settings = settings_type(
        database="climate",
        user="climate_app",
        password="private-test-password",
        kafka_bootstrap_servers="kafka:9092",
        port=port,
    )

    assert settings.port == port


@pytest.mark.unit
def test_business_sink_settings_are_immutable() -> None:
    settings_type = _business_config()
    settings = settings_type(
        database="climate",
        user="climate_app",
        password="private-test-password",
        kafka_bootstrap_servers="kafka:9092",
    )

    with pytest.raises(FrozenInstanceError):
        settings.port = 5433
