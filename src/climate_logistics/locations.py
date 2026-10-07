"""Configuração inicial das localidades de coleta."""

from __future__ import annotations

from collections.abc import Sequence

from climate_logistics.models import Location, WeatherValidationError

LOCATIONS: tuple[Location, ...] = (
    Location("RJ01", "Rio de Janeiro", -22.90, -43.17),
    Location("SP01", "São Paulo", -23.55, -46.63),
    Location("CT01", "Curitiba", -25.43, -49.27),
)


def validate_locations(locations: Sequence[Location]) -> None:
    """Rejeita configuração vazia, IDs repetidos ou entradas que não são Location."""
    if not locations:
        raise WeatherValidationError("locations não pode ser vazio")
    seen_ids: set[str] = set()
    for location in locations:
        if not isinstance(location, Location):
            raise WeatherValidationError("cada entrada deve ser Location")
        if location.location_id in seen_ids:
            raise WeatherValidationError(f"location_id duplicado: {location.location_id}")
        seen_ids.add(location.location_id)


validate_locations(LOCATIONS)
