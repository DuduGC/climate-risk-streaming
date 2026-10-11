"""Runtime settings for PostgreSQL and Kafka business sinks."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

_SQL_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_SAFE_HOST = re.compile(r"[A-Za-z0-9.-]+\Z")


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} deve ser texto não vazio")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} contém caracteres de controle")
    return value


def _sql_identifier(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SQL_IDENTIFIER.fullmatch(text) is None:
        raise ValueError(f"{name} deve ser um identificador PostgreSQL simples")
    return text


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} deve ser um inteiro positivo")
    return value


@dataclass(frozen=True)
class BusinessSinkSettings:
    """Credentials and bounded connection/batching settings for native sinks."""

    database: str
    user: str
    password: str = field(repr=False)
    kafka_bootstrap_servers: str = "kafka:9092"
    host: str = "postgres"
    port: int = 5432
    jdbc_connect_timeout_seconds: int = 10
    jdbc_socket_timeout_seconds: int = 10
    batch_size: int = 100
    batch_interval_ms: int = 2000
    max_retries: int = 3

    def __post_init__(self) -> None:
        _sql_identifier(self.database, "POSTGRES_DB")
        _sql_identifier(self.user, "APP_DB_USER")
        _required_text(self.password, "APP_DB_PASSWORD")
        host = _required_text(self.host, "host")
        if _SAFE_HOST.fullmatch(host) is None:
            raise ValueError("host deve conter somente letras, números, ponto ou hífen")
        _required_text(self.kafka_bootstrap_servers, "kafka_bootstrap_servers")

        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port deve ser um inteiro entre 1 e 65535")
        _positive_integer(self.jdbc_connect_timeout_seconds, "jdbc_connect_timeout_seconds")
        _positive_integer(self.jdbc_socket_timeout_seconds, "jdbc_socket_timeout_seconds")
        _positive_integer(self.batch_size, "batch_size")
        _positive_integer(self.batch_interval_ms, "batch_interval_ms")
        if type(self.max_retries) is not int or self.max_retries < 0:
            raise ValueError("max_retries deve ser um inteiro não negativo")

    @classmethod
    def from_environment(cls, *, bootstrap_servers: str) -> BusinessSinkSettings:
        """Load only the application role and database name from the environment."""
        app_user = _sql_identifier(os.environ.get("APP_DB_USER"), "APP_DB_USER")
        password = _required_text(os.environ.get("APP_DB_PASSWORD"), "APP_DB_PASSWORD")
        database = _sql_identifier(os.environ.get("POSTGRES_DB"), "POSTGRES_DB")

        admin_user = os.environ.get("POSTGRES_USER")
        if admin_user is not None:
            admin_user = _sql_identifier(admin_user, "POSTGRES_USER")
            if app_user.casefold() == admin_user.casefold():
                raise ValueError("APP_DB_USER deve ser diferente de POSTGRES_USER")
        if app_user.lower() == "postgres":
            raise ValueError("APP_DB_USER não pode ser a role administrativa postgres")

        return cls(
            database=database,
            user=app_user,
            password=password,
            kafka_bootstrap_servers=bootstrap_servers,
        )

    def jdbc_url(self) -> str:
        """Return a public JDBC URL; credentials are supplied separately."""
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?connectTimeout={self.jdbc_connect_timeout_seconds}"
            f"&socketTimeout={self.jdbc_socket_timeout_seconds}"
            "&reWriteBatchedInserts=false"
        )
