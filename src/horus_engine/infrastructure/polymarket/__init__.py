"""Read-only adapters for Polymarket's public Gamma API."""

from .catalog import PolymarketMarketCatalogGateway
from .errors import (
    PolymarketHttpError,
    PolymarketInfrastructureError,
    PolymarketPayloadError,
)

__all__ = [
    "PolymarketHttpError",
    "PolymarketInfrastructureError",
    "PolymarketMarketCatalogGateway",
    "PolymarketPayloadError",
]
