from __future__ import annotations

import json
import os

import pytest

from climate_logistics import open_meteo
from climate_logistics.locations import LOCATIONS

pytestmark = [pytest.mark.integration, pytest.mark.live]


def test_live_rj01_response_matches_weather_contract_and_serializes():
    if os.environ.get("OPEN_METEO_LIVE") != "1":
        pytest.skip("defina OPEN_METEO_LIVE=1 para consultar a Open-Meteo real")

    with open_meteo.OpenMeteoClient() as client:
        event = client.fetch(LOCATIONS[0])

    serialized = event.to_dict()
    assert serialized["location_id"] == "RJ01"
    assert serialized["city"] == "Rio de Janeiro"
    assert serialized["latitude"] == -22.9
    assert serialized["longitude"] == -43.17
    assert 1 <= serialized["measurement_interval_seconds"] <= 3600
    assert serialized["source"] == "open-meteo"
    assert serialized["schema_version"] == 1
    json.dumps(serialized, allow_nan=False)