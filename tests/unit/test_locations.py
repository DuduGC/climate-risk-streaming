from __future__ import annotations

import importlib
from dataclasses import FrozenInstanceError

import pytest

pytestmark = pytest.mark.unit


def _api(name):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"{name} ainda não foi implementado", pytrace=False)


@pytest.mark.parametrize(("location_id", "city", "latitude", "longitude"), [
    ("", "Rio", -22.9, -43.17), (" ", "Rio", -22.9, -43.17),
    (1, "Rio", -22.9, -43.17), ("RJ01", "", -22.9, -43.17),
    ("RJ01", " ", -22.9, -43.17), ("RJ01", None, -22.9, -43.17),
    ("RJ01", 1, -22.9, -43.17), ("RJ01", "Rio", True, -43.17),
    ("RJ01", "Rio", "-22.9", -43.17), ("RJ01", "Rio", float("nan"), -43.17),
    ("RJ01", "Rio", float("inf"), -43.17), ("RJ01", "Rio", -90.1, -43.17),
    ("RJ01", "Rio", 90.1, -43.17), ("RJ01", "Rio", -22.9, False),
    ("RJ01", "Rio", -22.9, "-43.17"), ("RJ01", "Rio", -22.9, float("nan")),
    ("RJ01", "Rio", -22.9, float("-inf")), ("RJ01", "Rio", -22.9, -180.1),
    ("RJ01", "Rio", -22.9, 180.1),
])
def test_location_rejects_invalid_identity_or_coordinates(location_id, city, latitude, longitude):
    models = _api("climate_logistics.models")
    with pytest.raises(models.WeatherValidationError):
        models.Location(location_id, city, latitude, longitude)


def test_location_accepts_coordinate_bounds_and_is_frozen():
    models = _api("climate_logistics.models")
    southern = models.Location("SOUTH", "South", -90, -180)
    northern = models.Location("NORTH", "North", 90, 180)
    assert (southern.latitude, southern.longitude) == (-90, -180)
    assert (northern.latitude, northern.longitude) == (90, 180)
    with pytest.raises(FrozenInstanceError):
        southern.city = "Other"


def test_initial_locations_match_the_approved_configuration():
    module = _api("climate_logistics.locations")
    models = _api("climate_logistics.models")
    assert module.LOCATIONS == (
        models.Location("RJ01", "Rio de Janeiro", -22.90, -43.17),
        models.Location("SP01", "São Paulo", -23.55, -46.63),
        models.Location("CT01", "Curitiba", -25.43, -49.27),
    )
    assert isinstance(module.LOCATIONS, tuple)


def test_validate_locations_rejects_empty_or_duplicate_ids():
    module = _api("climate_logistics.locations")
    models = _api("climate_logistics.models")
    first = models.Location("RJ01", "Rio", -22.9, -43.17)
    duplicate = models.Location("RJ01", "Other", -23.0, -44.0)
    with pytest.raises(models.WeatherValidationError):
        module.validate_locations(())
    with pytest.raises(models.WeatherValidationError):
        module.validate_locations((first, duplicate))
    assert module.validate_locations(module.LOCATIONS) is None
