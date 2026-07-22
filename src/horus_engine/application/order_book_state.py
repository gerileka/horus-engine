"""Snapshot-first reconstruction of one normalized local order book.

Timestamp ordering is a safety check only: it rejects observations that arrive
before a successfully applied event, but cannot prove that no venue message was
missed. A stale or invalid state therefore always requires a new snapshot.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto

from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
    is_tick_aligned,
)

from .errors import (
    InvalidLocalOrderBook,
    MarketDataIdentityMismatch,
    OutOfOrderMarketDataEvent,
    SnapshotRequired,
    TickSizeStateMismatch,
)
from .events import (
    BookSnapshotReceived,
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataReconnected,
    PriceLevelChanged,
    TickSizeChanged,
    TradeObserved,
)
from .models import MarketId, TokenId


class LocalBookStatus(Enum):
    """Synchronization status of one reconstructed local order book."""

    AWAITING_SNAPSHOT = auto()
    SYNCHRONIZED = auto()
    STALE = auto()
    INVALID = auto()


@dataclass(frozen=True)
class LocalOrderBookView:
    """Immutable, exchange-neutral view of one local order-book state machine."""

    market_id: MarketId
    token_id: TokenId
    tick_size: TickSize
    status: LocalBookStatus
    book: OrderBook | None
    last_observed_at: datetime | None
    status_reason: str | None


class LocalOrderBookState:
    """Atomically reconstruct one token's book from normalized observations.

    Failed validation rejects an event without state changes. A valid level
    update that creates a crossed book is deliberately different: it records
    that observation, marks the machine invalid, and then requires a snapshot.
    """

    def __init__(
        self, market_id: MarketId, token_id: TokenId, tick_size: TickSize
    ) -> None:
        """Create a snapshot-first state machine for one fixed market token."""
        self._market_id = market_id
        self._token_id = token_id
        self._tick_size = tick_size
        self._status = LocalBookStatus.AWAITING_SNAPSHOT
        self._book: OrderBook | None = None
        self._last_observed_at: datetime | None = None
        self._status_reason: str | None = None
        self._bids: dict[Price, Quantity] = {}
        self._asks: dict[Price, Quantity] = {}

    @property
    def view(self) -> LocalOrderBookView:
        """Return a fresh immutable view without exposing internal mappings."""
        return LocalOrderBookView(
            self._market_id,
            self._token_id,
            self._tick_size,
            self._status,
            self._book,
            self._last_observed_at,
            self._status_reason,
        )

    def apply(self, event: MarketDataEvent) -> LocalOrderBookView:
        """Apply one normalized event or reject it without partial mutation."""
        if isinstance(event, BookSnapshotReceived):
            self._apply_snapshot(event)
        elif isinstance(event, PriceLevelChanged):
            self._apply_price_level_change(event)
        elif isinstance(event, TickSizeChanged):
            self._apply_tick_size_change(event)
        elif isinstance(event, MarketDataDisconnected):
            self._apply_disconnection(event)
        elif isinstance(event, MarketDataReconnected):
            self._apply_reconnection(event)
        elif isinstance(event, TradeObserved):
            self._validate_identity(event.market_id, event.token_id)
        else:
            raise TypeError("unsupported market-data event")
        return self.view

    def require_snapshot(self, reason: str) -> LocalOrderBookView:
        """Mark the retained observation stale until a new snapshot is applied."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("snapshot requirement reason must be a non-blank string")
        if self._status is not LocalBookStatus.INVALID:
            self._status = LocalBookStatus.STALE
        self._status_reason = reason.strip()
        return self.view

    def _apply_snapshot(self, event: BookSnapshotReceived) -> None:
        """Atomically replace local levels from one authoritative snapshot."""
        self._validate_identity(event.market_id, event.token_id)
        self._validate_timestamp(event.observed_at)
        bids = self._levels_to_mapping(event.book.bids)
        asks = self._levels_to_mapping(event.book.asks)
        candidate_book = self._build_book(bids, asks)
        if candidate_book.is_crossed:
            raise InvalidLocalOrderBook("snapshot book is crossed")
        self._commit(
            bids=bids,
            asks=asks,
            book=candidate_book,
            status=LocalBookStatus.SYNCHRONIZED,
            observed_at=event.observed_at,
            reason=None,
        )

    def _apply_price_level_change(self, event: PriceLevelChanged) -> None:
        """Apply one aggregate replacement or deletion to synchronized state."""
        self._validate_identity(event.market_id, event.token_id)
        self._validate_timestamp(event.observed_at)
        if self._status is not LocalBookStatus.SYNCHRONIZED:
            raise SnapshotRequired("authoritative snapshot required")
        self._validate_price(event.price)
        bids = self._bids.copy()
        asks = self._asks.copy()
        levels = bids if event.side is Side.BUY else asks
        if event.quantity.value == 0:
            levels.pop(event.price, None)
        else:
            levels[event.price] = Quantity(event.quantity.value)
        candidate_book = self._build_book(bids, asks)
        status = (
            LocalBookStatus.INVALID
            if candidate_book.is_crossed
            else LocalBookStatus.SYNCHRONIZED
        )
        reason = (
            "crossed local book requires snapshot"
            if candidate_book.is_crossed
            else None
        )
        self._commit(
            bids=bids,
            asks=asks,
            book=candidate_book,
            status=status,
            observed_at=event.observed_at,
            reason=reason,
        )

    def _apply_tick_size_change(self, event: TickSizeChanged) -> None:
        """Track a compatible new tick and force authoritative resynchronization."""
        self._validate_identity(event.market_id, event.token_id)
        self._validate_timestamp(event.observed_at)
        if event.old_tick_size != self._tick_size:
            raise TickSizeStateMismatch("tick-size state does not match event")
        self._tick_size = event.new_tick_size
        self._status = LocalBookStatus.STALE
        self._last_observed_at = event.observed_at
        self._status_reason = "tick-size change requires snapshot"

    def _apply_disconnection(self, event: MarketDataDisconnected) -> None:
        """Mark the retained observation stale after a transport interruption."""
        self._validate_timestamp(event.observed_at)
        self._status = LocalBookStatus.STALE
        self._last_observed_at = event.observed_at
        self._status_reason = "market-data disconnection requires snapshot"

    def _apply_reconnection(self, event: MarketDataReconnected) -> None:
        """Keep state stale because reconnecting alone cannot restore continuity."""
        self._validate_timestamp(event.observed_at)
        self._status = LocalBookStatus.STALE
        self._last_observed_at = event.observed_at
        self._status_reason = "market-data reconnection requires snapshot"

    def _validate_identity(self, market_id: MarketId, token_id: TokenId) -> None:
        """Require every market-specific event to match this fixed instance."""
        if market_id != self._market_id or token_id != self._token_id:
            raise MarketDataIdentityMismatch("event identity does not match local book")

    def _validate_timestamp(self, observed_at: datetime) -> None:
        """Reject only observations strictly before the applied event timestamp."""
        if self._last_observed_at is not None and observed_at < self._last_observed_at:
            raise OutOfOrderMarketDataEvent("event timestamp is out of order")

    def _levels_to_mapping(
        self, levels: tuple[OrderBookLevel, ...]
    ) -> dict[Price, Quantity]:
        """Copy and tick-validate one immutable side of a snapshot."""
        copied_levels: dict[Price, Quantity] = {}
        for level in levels:
            self._validate_price(level.price)
            copied_levels[level.price] = level.quantity
        return copied_levels

    def _validate_price(self, price: Price) -> None:
        """Require exact alignment with the currently tracked tick size."""
        if not is_tick_aligned(price, self._tick_size):
            raise InvalidLocalOrderBook("price is not aligned with tick size")

    @staticmethod
    def _build_book(
        bids: dict[Price, Quantity], asks: dict[Price, Quantity]
    ) -> OrderBook:
        """Build an immutable snapshot from private aggregate mappings."""
        return OrderBook(
            (OrderBookLevel(price, quantity) for price, quantity in bids.items()),
            (OrderBookLevel(price, quantity) for price, quantity in asks.items()),
        )

    def _commit(
        self,
        *,
        bids: dict[Price, Quantity],
        asks: dict[Price, Quantity],
        book: OrderBook,
        status: LocalBookStatus,
        observed_at: datetime,
        reason: str | None,
    ) -> None:
        """Commit a fully validated candidate state in one assignment sequence."""
        self._bids = bids
        self._asks = asks
        self._book = book
        self._status = status
        self._last_observed_at = observed_at
        self._status_reason = reason
