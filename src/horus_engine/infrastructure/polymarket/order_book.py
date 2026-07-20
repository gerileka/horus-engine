"""Read-only CLOB implementation of the order-book snapshot gateway."""

import httpx

from horus_engine.application import MarketId, TokenId
from horus_engine.domain import OrderBook

from .clob_parsing import parse_order_book
from .errors import PolymarketHttpError

DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"
_BOOK_PATH = "/book"


class PolymarketOrderBookSnapshotGateway:
    """Retrieve public CLOB order-book snapshots without authentication.

    The caller owns the supplied HTTP client and remains responsible for closing
    it. This adapter deliberately provides snapshot retrieval only; streaming,
    trading, and account operations are separate capabilities.
    """

    def __init__(
        self, client: httpx.AsyncClient, base_url: str = DEFAULT_CLOB_BASE_URL
    ) -> None:
        """Create a snapshot gateway using a caller-managed HTTP client."""
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("base_url must be a non-blank string")
        self._client = client
        self._base_url = base_url.rstrip("/")

    async def get_order_book(self, market_id: MarketId, token_id: TokenId) -> OrderBook:
        """Return one validated, immutable CLOB order-book snapshot."""
        try:
            response = await self._client.get(
                f"{self._base_url}{_BOOK_PATH}", params={"token_id": token_id.value}
            )
        except httpx.RequestError as error:
            raise PolymarketHttpError("CLOB book request failed") from error
        if not response.is_success:
            raise PolymarketHttpError(
                f"CLOB book request returned HTTP {response.status_code}"
            )
        return parse_order_book(response.text, market_id, token_id)
