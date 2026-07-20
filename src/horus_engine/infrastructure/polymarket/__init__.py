"""Read-only adapters for Polymarket's public Gamma and CLOB APIs."""

from .catalog import PolymarketMarketCatalogGateway
from .errors import (
    PolymarketHttpError,
    PolymarketInfrastructureError,
    PolymarketPayloadError,
)
from .order_book import PolymarketOrderBookSnapshotGateway

__all__ = [
    "PolymarketHttpError",
    "PolymarketInfrastructureError",
    "PolymarketMarketCatalogGateway",
    "PolymarketOrderBookSnapshotGateway",
    "PolymarketPayloadError",
]
