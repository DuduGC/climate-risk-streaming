"""Atomic health heartbeat and Docker healthcheck for the alert consumer."""

from __future__ import annotations

import json
import math
import os
import tempfile
import time
from pathlib import Path
from typing import Any

DEFAULT_HEALTH_PATH = Path("/tmp/alert-consumer-health.json")
_HEALTH_FIELDS = frozenset({"pid", "ready", "updated_at", "metadata_at"})
_HEALTH_MAX_AGE_SECONDS = 45.0


def _finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(value)
    )


def write_health(
    path: Path,
    *,
    pid: int,
    ready: bool,
    updated_at: float,
    metadata_at: float,
) -> None:
    """Atomically replace a small health file containing monotonic timestamps."""
    if type(pid) is not int or pid <= 0 or type(ready) is not bool:
        raise ValueError("invalid health metadata")
    if not _finite_number(updated_at) or not _finite_number(metadata_at):
        raise ValueError("invalid health timestamps")

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "pid": pid,
        "ready": ready,
        "updated_at": updated_at,
        "metadata_at": metadata_at,
    }
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(document, allow_nan=False, separators=(",", ":")))
            stream.flush()
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except (OSError, OverflowError, ValueError):
        return False
    return True


def healthcheck(path: Path, *, now: float | None = None) -> bool:
    """Return whether a ready process has fresh metadata and heartbeat timestamps."""
    current = time.monotonic() if now is None else now
    if not _finite_number(current):
        return False
    try:
        document: Any = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(document, dict) or set(document) != _HEALTH_FIELDS:
        return False

    pid = document["pid"]
    updated_at = document["updated_at"]
    metadata_at = document["metadata_at"]
    if (
        type(pid) is not int
        or pid <= 0
        or document["ready"] is not True
        or not _finite_number(updated_at)
        or not _finite_number(metadata_at)
        or metadata_at < 0
        or updated_at < metadata_at
        or updated_at > current
        or metadata_at > current
        or current - updated_at > _HEALTH_MAX_AGE_SECONDS
        or current - metadata_at > _HEALTH_MAX_AGE_SECONDS
    ):
        return False
    return _process_is_alive(pid)


def main() -> int:
    """Docker healthcheck entry point; deliberately does not connect to Kafka."""
    return 0 if healthcheck(DEFAULT_HEALTH_PATH) else 1


if __name__ == "__main__":
    raise SystemExit(main())
