from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from climate_logistics import open_meteo
from climate_logistics.locations import LOCATIONS
from climate_logistics.models import WeatherValidationError

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parents[1] / "fixtures" / "open_meteo_current.json"
INGESTION = datetime(2026, 10, 7, 13, 20, 2, tzinfo=UTC)
MAX_RESPONSE_BYTES = 131_072


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _payload() -> dict[str, object]:
    return json.loads(_body())


def _client_type():
    client_type = getattr(open_meteo, "OpenMeteoClient", None)
    if client_type is None:
        pytest.fail("OpenMeteoClient ainda não foi implementado", pytrace=False)
    return client_type


def _open_meteo_error():
    error_type = getattr(open_meteo, "OpenMeteoError", None)
    if error_type is None:
        pytest.fail("OpenMeteoError ainda não foi implementado", pytrace=False)
    return error_type


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        body: bytes | None = None,
        *,
        headers: dict[str, str] | None = None,
        read_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.body = _body() if body is None else body
        self.headers = {} if headers is None else headers
        self.read_error = read_error
        self.order = order
        self.closed = False
        self.chunk_sizes: list[int] = []

    def iter_content(self, chunk_size: int):
        self.chunk_sizes.append(chunk_size)
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset : offset + chunk_size]
        if self.order is not None:
            self.order.append("body")
        if self.read_error is not None:
            raise self.read_error

    def close(self) -> None:
        self.closed = True
        if self.order is not None:
            self.order.append("response closed")


class FakeSession:
    def __init__(self, responses: list[object]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.close_count = 0

    def get(self, url: str, **kwargs: object):
        self.calls.append((url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def close(self) -> None:
        self.closed = True
        self.close_count += 1


def _make_client(session: FakeSession, *, sleeper=None, clock=None):
    options: dict[str, object] = {"session": session}
    if sleeper is not None:
        options["sleeper"] = sleeper
    if clock is not None:
        options["clock"] = clock
    return _client_type()(**options)


def test_fetch_sends_exact_open_meteo_query_and_captures_clock_after_body():
    order: list[str] = []
    response = FakeResponse(order=order)
    session = FakeSession([response])
    client = _make_client(session, clock=lambda: (order.append("clock"), INGESTION)[1])

    event = client.fetch(LOCATIONS[0])

    url, kwargs = session.calls[0]
    assert url == open_meteo.OPEN_METEO_URL
    assert kwargs == {
        "params": {
            "latitude": -22.9,
            "longitude": -43.17,
            "current": (
                "temperature_2m,precipitation,wind_speed_10m,"
                "wind_gusts_10m,visibility,weather_code"
            ),
            "temperature_unit": "celsius",
            "precipitation_unit": "mm",
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
            "timeformat": "unixtime",
        },
        "timeout": (3, 10),
        "stream": True,
        "allow_redirects": False,
        "verify": True,
    }
    assert response.chunk_sizes == [8192]
    assert order.index("body") < order.index("clock")
    assert event.to_dict()["ingestion_timestamp"] == "2026-10-07T13:20:02Z"
    assert (event.location_id, event.latitude, event.longitude) == ("RJ01", -22.9, -43.17)
    assert response.closed


def test_injected_session_remains_caller_owned_and_response_closes_on_success():
    response = FakeResponse()
    session = FakeSession([response])
    client = _make_client(session, clock=lambda: INGESTION)

    client.fetch(LOCATIONS[0])
    client.close()

    assert response.closed
    assert not session.closed


def test_injected_session_remains_caller_owned_after_context_manager():
    response = FakeResponse()
    session = FakeSession([response])

    with _make_client(session, clock=lambda: INGESTION) as client:
        client.fetch(LOCATIONS[0])

    assert response.closed
    assert not session.closed


def test_client_disables_environment_auth_and_closes_its_own_session(monkeypatch):
    response = FakeResponse()

    class OwnedSession(FakeSession):
        trust_env = True

    created: list[OwnedSession] = []

    def create_session():
        session = OwnedSession([response])
        created.append(session)
        return session

    monkeypatch.setattr(requests, "Session", create_session)
    client = _client_type()(clock=lambda: INGESTION)

    assert created[0].trust_env is False
    client.fetch(LOCATIONS[0])
    client.close()
    client.close()

    assert response.closed
    assert created[0].closed
    assert created[0].close_count == 1


def test_three_transient_statuses_use_bounded_backoff_and_close_before_sleep():
    responses = [FakeResponse(503), FakeResponse(503), FakeResponse()]
    waits: list[float] = []

    def sleeper(delay: float) -> None:
        assert responses[len(waits)].closed
        waits.append(delay)

    session = FakeSession(responses)
    event = _make_client(session, sleeper=sleeper, clock=lambda: INGESTION).fetch(LOCATIONS[0])

    assert event.temperature == 27.4
    assert len(session.calls) == 3
    assert waits == [0.5, 1.0]
    assert all(response.closed for response in responses)


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_each_transient_server_status_is_retried(status):
    responses = [FakeResponse(status), FakeResponse()]
    waits: list[float] = []
    session = FakeSession(responses)

    event = _make_client(
        session, sleeper=waits.append, clock=lambda: INGESTION
    ).fetch(LOCATIONS[0])

    assert event.weather_code == 61
    assert len(session.calls) == 2
    assert waits == [0.5]
    assert all(response.closed for response in responses)


def test_repeated_transient_status_stops_after_three_requests_without_final_sleep():
    responses = [FakeResponse(503), FakeResponse(503), FakeResponse(503)]
    waits: list[float] = []
    session = FakeSession(responses)

    with pytest.raises(_open_meteo_error()) as error:
        _make_client(session, sleeper=waits.append).fetch(LOCATIONS[0])

    assert "RJ01" in str(error.value)
    assert len(session.calls) == 3
    assert waits == [0.5, 1.0]
    assert all(response.closed for response in responses)


@pytest.mark.parametrize(
    "exception",
    [
        requests.Timeout("slow response"),
        requests.ConnectionError("connection reset"),
    ],
)
def test_transient_request_exceptions_retry_twice(exception):
    session = FakeSession([exception, exception, FakeResponse()])
    waits: list[float] = []

    event = _make_client(
        session, sleeper=waits.append, clock=lambda: INGESTION
    ).fetch(LOCATIONS[0])

    assert event.weather_code == 61
    assert len(session.calls) == 3
    assert waits == [0.5, 1.0]


@pytest.mark.parametrize(
    "read_error",
    [
        requests.Timeout("read stalled"),
        requests.ConnectionError("connection reset during read"),
    ],
)
def test_transient_read_errors_close_response_before_retry(read_error):
    responses = [
        FakeResponse(read_error=read_error),
        FakeResponse(read_error=read_error),
        FakeResponse(),
    ]
    waits: list[float] = []

    def sleeper(delay: float) -> None:
        assert responses[len(waits)].closed
        waits.append(delay)

    session = FakeSession(responses)
    event = _make_client(
        session, sleeper=sleeper, clock=lambda: INGESTION
    ).fetch(LOCATIONS[0])

    assert event.weather_code == 61
    assert len(session.calls) == 3
    assert waits == [0.5, 1.0]
    assert all(response.closed for response in responses)


@pytest.mark.parametrize(
    ("retry_after", "expected_delay"),
    [
        ("2", 2.0),
        ("100", 5.0),
        (None, 0.5),
        ("later", 0.5),
        ("-1", 0.5),
        ("NaN", 0.5),
        ("inf", 0.5),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0.5),
    ],
)
def test_429_uses_bounded_numeric_retry_after_or_backoff(retry_after, expected_delay):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    responses = [FakeResponse(429, headers=headers), FakeResponse()]
    waits: list[float] = []
    session = FakeSession(responses)

    event = _make_client(
        session, sleeper=waits.append, clock=lambda: INGESTION
    ).fetch(LOCATIONS[0])

    assert event.weather_code == 61
    assert len(session.calls) == 2
    assert waits == [expected_delay]
    assert all(response.closed for response in responses)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 301, 302])
def test_permanent_http_errors_and_redirects_are_not_retried(status):
    response = FakeResponse(status, b"DO_NOT_INCLUDE_THIS_RESPONSE_BODY")
    session = FakeSession([response, FakeResponse()])
    waits: list[float] = []
    client = _make_client(session, sleeper=waits.append)

    with pytest.raises(_open_meteo_error()) as error:
        client.fetch(LOCATIONS[0])

    assert "RJ01" in str(error.value)
    assert "DO_NOT_INCLUDE_THIS_RESPONSE_BODY" not in str(error.value)
    assert len(session.calls) == 1
    assert waits == []
    assert response.closed


def test_tls_error_is_permanent_even_though_requests_classifies_it_as_connection_error():
    session = FakeSession([requests.exceptions.SSLError("certificate rejected")])
    waits: list[float] = []

    with pytest.raises(_open_meteo_error()) as error:
        _make_client(session, sleeper=waits.append).fetch(LOCATIONS[0])

    assert "RJ01" in str(error.value)
    assert len(session.calls) == 1
    assert waits == []


@pytest.mark.parametrize(
    ("body", "error_type"),
    [
        (b"", None),
        (b"\xff", None),
        (b'{"current":', None),
        (b"[]", WeatherValidationError),
    ],
)
def test_invalid_body_or_non_object_json_fails_without_retry(body, error_type):
    response = FakeResponse(body=body)
    session = FakeSession([response, FakeResponse()])

    expected = _open_meteo_error() if error_type is None else error_type
    with pytest.raises(expected):
        _make_client(session, clock=lambda: INGESTION).fetch(LOCATIONS[0])

    assert len(session.calls) == 1
    assert response.closed


def test_weather_contract_error_is_not_retried_or_rewritten():
    payload = _payload()
    payload["current"]["temperature_2m"] = 999
    response = FakeResponse(body=json.dumps(payload).encode())
    session = FakeSession([response, FakeResponse()])

    with pytest.raises(WeatherValidationError, match="temperature"):
        _make_client(session, clock=lambda: INGESTION).fetch(LOCATIONS[0])

    assert len(session.calls) == 1
    assert response.closed


def test_exact_decompressed_body_limit_is_accepted_without_content_length():
    body = _body()
    exact = body + b" " * (MAX_RESPONSE_BYTES - len(body))
    response = FakeResponse(body=exact)
    session = FakeSession([response])

    event = _make_client(session, clock=lambda: INGESTION).fetch(LOCATIONS[0])

    assert event.weather_code == 61
    assert len(response.body) == MAX_RESPONSE_BYTES
    assert response.closed


def test_first_byte_over_decompressed_body_limit_fails_and_closes_response():
    body = _body()
    too_large = body + b" " * (MAX_RESPONSE_BYTES + 1 - len(body))
    response = FakeResponse(body=too_large)
    session = FakeSession([response, FakeResponse()])
    waits: list[float] = []

    with pytest.raises(_open_meteo_error()) as error:
        _make_client(session, sleeper=waits.append).fetch(LOCATIONS[0])

    assert "RJ01" in str(error.value)
    assert len(session.calls) == 1
    assert waits == []
    assert response.closed

@pytest.mark.parametrize(
    "body",
    [
        b'{"x":' + b"9" * 5000 + b"}",
        b"[" * 2000 + b"0" + b"]" * 2000,
    ],
    ids=["integer-digit-limit", "json-nesting-limit"],
)
def test_pathological_json_is_wrapped_without_retry_or_body_content(body):
    assert len(body) < MAX_RESPONSE_BYTES
    response = FakeResponse(body=body)
    session = FakeSession([response, FakeResponse()])
    waits: list[float] = []

    with pytest.raises(_open_meteo_error()) as error:
        _make_client(session, sleeper=waits.append).fetch(LOCATIONS[0])

    assert "RJ01" in str(error.value)
    assert body.decode("ascii") not in str(error.value)
    assert len(session.calls) == 1
    assert waits == []
    assert response.closed
