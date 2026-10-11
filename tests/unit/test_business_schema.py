from __future__ import annotations

import csv
import importlib
import re
import subprocess
import sys
from pathlib import Path

import pytest

from climate_logistics.locations import LOCATIONS

ROOT = Path(__file__).resolve().parents[2]
SEED_RENDERER = ROOT / "scripts" / "render_location_seed.py"
VERSIONED_SEED = ROOT / "docker" / "postgres" / "locations.csv"
SEED_FIELDS = ("location_id", "city", "latitude", "longitude")


def _business_sql():
    try:
        return importlib.import_module("climate_logistics.business_sql")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.business_sql":
            pytest.fail(
                "the PostgreSQL business statement has not been implemented yet",
                pytrace=False,
            )
        raise


@pytest.mark.unit
def test_business_upsert_statement_has_exactly_two_typed_jdbc_parameters():
    sql = _business_sql().BUSINESS_UPSERT_SQL
    normalized = re.sub(r"\s+", " ", sql.strip())

    assert sql.count("?") == 2
    assert re.match(
        r"(?i)^WITH incoming AS \(\s*SELECT \?::jsonb AS assessment, \?::text AS alert_id\s*\)",
        normalized,
    )
    assert re.search(r"(?i)RETURNING\b", sql)
    assert re.search(r"(?i)INSERT INTO climate\.weather_alerts\b", sql)


@pytest.mark.unit
def test_location_seed_renderer_matches_catalog_and_versioned_utf8_artifact(tmp_path: Path):
    assert SEED_RENDERER.is_file(), "location seed renderer has not been implemented yet"
    generated_seed = tmp_path / "locations.csv"
    result = subprocess.run(
        [sys.executable, str(SEED_RENDERER), "--output", str(generated_seed)],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    assert generated_seed.is_file()
    generated_bytes = generated_seed.read_bytes()
    assert b"\r" not in generated_bytes
    assert generated_bytes.decode("utf-8").startswith(
        "location_id,city,latitude,longitude\n"
    )
    with generated_seed.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert tuple(reader.fieldnames or ()) == SEED_FIELDS
        actual_rows = list(reader)
    expected_rows = [
        {
            "location_id": location.location_id,
            "city": location.city,
            "latitude": str(location.latitude),
            "longitude": str(location.longitude),
        }
        for location in LOCATIONS
    ]
    assert actual_rows == expected_rows
    assert VERSIONED_SEED.read_bytes() == generated_bytes
