#!/usr/bin/env python3
"""Render the PostgreSQL location seed from the application's location catalog."""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Sequence
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from climate_logistics.locations import LOCATIONS  # noqa: E402

SEED_FIELDS = ("location_id", "city", "latitude", "longitude")
DEFAULT_OUTPUT = REPOSITORY_ROOT / "docker" / "postgres" / "locations.csv"


def render_location_seed(output: Path) -> None:
    """Write the location catalog as a stable UTF-8 CSV with LF line endings."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=SEED_FIELDS, lineterminator="\n")
        writer.writeheader()
        for location in LOCATIONS:
            writer.writerow(
                {
                    "location_id": location.location_id,
                    "city": location.city,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                }
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="CSV path to write (default: docker/postgres/locations.csv)",
    )
    args = parser.parse_args(argv)
    render_location_seed(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
