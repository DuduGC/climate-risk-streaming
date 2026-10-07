from __future__ import annotations

import gzip
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlsplit

import pytest
import requests

from climate_logistics import open_meteo
from climate_logistics.locations import LOCATIONS

pytestmark = pytest.mark.integration

INGESTION = datetime(2026, 10, 7, 13, 20, 2, tzinfo=UTC)
MAX_RESPONSE_BYTES = 131_072


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

def _valid_body() -> bytes:
    return (Path(__file__).parents[1] / "fixtures" / "open_meteo_current.json").read_bytes()


@pytest.fixture
def local_server(monkeypatch):
    state: dict[str, object] = {"requests": 0, "paths": [], "script": []}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            state["requests"] = int(state["requests"]) + 1
            paths = state["paths"]
            assert isinstance(paths, list)
            paths.append(self.path)
            script = state["script"]
            assert isinstance(script, list)
            item = script.pop(0)
            status = int(item.get("status", 200))
            headers = item.get("headers", {})
            body = item.get("body", b"")
            delay = float(item.get("delay", 0))
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            if item.get("content_length", True):
                self.send_header("Content-Length", str(len(body)))
            else:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            if delay:
                time.sleep(delay)
            if body:
                try:
                    self.wfile.write(body)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

        def log_message(self, format: str, *args: object) -> None:
            return

    class QuietThreadingHTTPServer(ThreadingHTTPServer):
        def handle_error(self, request: object, client_address: object) -> None:
            return


    server = QuietThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/v1/forecast"
    monkeypatch.setattr(open_meteo, "OPEN_METEO_URL", endpoint, raising=False)
    try:
        yield endpoint, state
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)


def _fetch(state, *, sleeper=None, session=None):
    client_type = _client_type()
    options: dict[str, object] = {"clock": lambda: INGESTION}
    if sleeper is not None:
        options["sleeper"] = sleeper
    if session is not None:
        options["session"] = session
    with client_type(**options) as client:
        return client.fetch(LOCATIONS[0])


def test_loopback_server_valid_json_produces_event_and_requests_current_fields(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.append({"status": 200, "body": _valid_body()})

    event = _fetch(state)

    paths = state["paths"]
    assert state["requests"] == 1
    assert isinstance(paths, list)
    query = urlsplit(paths[0]).query
    assert "latitude=-22.9" in query
    assert "longitude=-43.17" in query
    assert "timezone=UTC" in query
    assert "timeformat=unixtime" in query
    assert event.to_dict()["ingestion_timestamp"] == "2026-10-07T13:20:02Z"
    assert event.weather_code == 61


def test_loopback_server_503_then_200_retries_and_returns_event(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.extend(
        [
            {"status": 503, "body": b"temporary"},
            {"status": 200, "body": _valid_body()},
        ]
    )
    waits: list[float] = []

    event = _fetch(state, sleeper=waits.append)

    assert state["requests"] == 2
    assert waits == [0.5]
    assert event.location_id == "RJ01"


def test_loopback_server_429_then_200_respects_retry_after(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.extend(
        [
            {"status": 429, "headers": {"Retry-After": "0"}, "body": b"busy"},
            {"status": 200, "body": _valid_body()},
        ]
    )
    waits: list[float] = []

    event = _fetch(state, sleeper=waits.append)

    assert state["requests"] == 2
    assert waits == [0.0]
    assert event.weather_code == 61


def test_loopback_server_400_is_not_retried(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.extend(
        [
            {"status": 400, "body": b"private provider response"},
            {"status": 200, "body": _valid_body()},
        ]
    )

    with pytest.raises(_open_meteo_error(), match="RJ01"):
        _fetch(state)

    assert state["requests"] == 1


def test_loopback_redirect_is_not_followed(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.extend(
        [
            {
                "status": 302,
                "headers": {"Location": "/redirect-target"},
                "body": b"",
            },
            {"status": 200, "body": _valid_body()},
            {"status": 200, "body": _valid_body()},
        ]
    )

    with pytest.raises(_open_meteo_error()):
        _fetch(state)

    assert state["requests"] == 1


@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
def test_loopback_server_rejects_oversized_decompressed_body_without_length(
    local_server, compressed
):
    _endpoint, state = local_server
    body = _valid_body()
    oversized = body + b" " * (MAX_RESPONSE_BYTES + 1 - len(body))
    headers: dict[str, str] = {}
    if compressed:
        oversized = gzip.compress(oversized)
        headers["Content-Encoding"] = "gzip"
    script = state["script"]
    assert isinstance(script, list)
    script.append(
        {
            "status": 200,
            "headers": headers,
            "body": oversized,
            "content_length": False,
        }
    )

    with pytest.raises(_open_meteo_error(), match="RJ01"):
        _fetch(state)

    assert state["requests"] == 1


def test_slow_loopback_response_uses_short_test_transport_timeout(local_server):
    _endpoint, state = local_server
    script = state["script"]
    assert isinstance(script, list)
    script.extend(
        [
            {"status": 200, "body": _valid_body(), "delay": 0.25},
            {"status": 200, "body": _valid_body(), "delay": 0.25},
            {"status": 200, "body": _valid_body(), "delay": 0.25},
        ]
    )
    underlying_session = requests.Session()

    class ShortTimeoutSession:
        def get(self, url: str, **kwargs: object):
            kwargs["timeout"] = (0.05, 0.05)
            return underlying_session.get(url, **kwargs)

        def close(self) -> None:
            underlying_session.close()

    started = time.monotonic()
    try:
        with pytest.raises(_open_meteo_error(), match="RJ01"):
            _fetch(state, sleeper=lambda _delay: None, session=ShortTimeoutSession())
    finally:
        underlying_session.close()

    assert time.monotonic() - started < 3
    assert state["requests"] == 3