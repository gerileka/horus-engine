"""Read-only Gamma implementation of the exchange-neutral market catalog."""

from collections.abc import Mapping

import httpx

from horus_engine.application import Market, MarketId

from .errors import PolymarketHttpError, PolymarketPayloadError
from .parsing import GammaRecord, market_from_gamma, parse_market_page

DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
_MARKETS_PATH = "/markets"


class PolymarketMarketCatalogGateway:
    """Retrieve public Polymarket market metadata without authentication.

    The caller owns the supplied HTTP client and remains responsible for closing
    it. This adapter intentionally exposes no order, account, or order-book API.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str = DEFAULT_GAMMA_BASE_URL,
        page_size: int = 100,
    ) -> None:
        """Create a catalog gateway using a caller-managed HTTP client."""
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("base_url must be a non-blank string")
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or page_size <= 0
        ):
            raise ValueError("page_size must be a positive integer")
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._page_size = page_size

    async def list_markets(self) -> tuple[Market, ...]:
        """Return all available CLOB markets in stable Gamma page order."""
        markets: list[Market] = []
        offset = 0
        while True:
            records = await self._market_page(
                {
                    "active": "true",
                    "closed": "false",
                    "limit": self._page_size,
                    "offset": offset,
                }
            )
            markets.extend(
                market
                for index, record in enumerate(records, start=offset)
                if (market := market_from_gamma(record, index)) is not None
            )
            if len(records) < self._page_size:
                return tuple(markets)
            offset += self._page_size

    async def get_market(self, market_id: MarketId) -> Market | None:
        """Return one market by Polymarket condition ID, including closed markets."""
        matches: list[Market] = []
        offset = 0
        while True:
            records = await self._market_page(
                {
                    "condition_ids": market_id.value,
                    "limit": self._page_size,
                    "offset": offset,
                }
            )
            matches.extend(
                market
                for index, record in enumerate(records, start=offset)
                if (market := market_from_gamma(record, index)) is not None
                and market.market_id == market_id
            )
            if len(records) < self._page_size:
                break
            offset += self._page_size
        if not matches:
            return None
        if len(set(matches)) != 1:
            raise PolymarketPayloadError(
                "Gamma returned incompatible records for one condition ID"
            )
        return matches[0]

    async def _market_page(
        self, params: Mapping[str, str | int]
    ) -> tuple[GammaRecord, ...]:
        """Fetch and decode a single Gamma market page with boundary-specific errors."""
        try:
            response = await self._client.get(
                f"{self._base_url}{_MARKETS_PATH}", params=params
            )
        except httpx.RequestError as error:
            raise PolymarketHttpError("Gamma market request failed") from error
        if not response.is_success:
            raise PolymarketHttpError(
                f"Gamma market request returned HTTP {response.status_code}"
            )
        return parse_market_page(response.text)
