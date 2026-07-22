"""Single-lifecycle coordination of normalized market data and local book state."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from horus_engine.domain import OrderBook, TickSize

from .errors import (
    LocalOrderBookStateError,
    MarketDataBootstrapError,
    MarketDataSessionAlreadyStarted,
    MarketDataSessionError,
    MarketDataSynchronizationLost,
)
from .events import (
    MarketDataDisconnected,
    MarketDataEvent,
    MarketDataReconnected,
    PriceLevelChanged,
    TickSizeChanged,
    TradeObserved,
)
from .gateways import MarketDataStreamGateway
from .models import MarketId, TokenId
from .order_book_state import LocalBookStatus, LocalOrderBookState, LocalOrderBookView


@dataclass(frozen=True)
class MarketDataSessionUpdate:
    """One consumed normalized event and the immutable state after processing it."""

    event: MarketDataEvent
    book_view: LocalOrderBookView
    book_changed: bool


class MarketDataSession:
    """Consume one normalized market-data lifecycle for one market token pair."""

    def __init__(
        self,
        stream_gateway: MarketDataStreamGateway,
        market_id: MarketId,
        token_id: TokenId,
        tick_size: TickSize,
    ) -> None:
        """Create a session that waits for its stream's authoritative snapshot."""
        self._stream_gateway = stream_gateway
        self._market_id = market_id
        self._token_id = token_id
        self._state = LocalOrderBookState(market_id, token_id, tick_size)
        self._started = False

    @property
    def view(self) -> LocalOrderBookView:
        """Return the latest immutable local order-book view."""
        return self._state.view

    def stream_updates(self) -> AsyncIterator[MarketDataSessionUpdate]:
        """Return this session's sole async iterator of processed updates."""
        if self._started:
            raise MarketDataSessionAlreadyStarted("market-data session already started")
        self._started = True
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketDataSessionUpdate]:
        """Subscribe once and process normalized events in their arrival order."""
        stream: AsyncIterator[MarketDataEvent] | None = None
        try:
            stream = self._stream_gateway.stream_market_data(
                self._market_id, (self._token_id,)
            )
            async for event in stream:
                update, terminal = self._process_event(event)
                yield update
                if terminal:
                    return
        except asyncio.CancelledError:
            raise
        except (MarketDataBootstrapError, MarketDataSynchronizationLost):
            raise
        except Exception as error:
            self._state.require_snapshot("market-data stream failed")
            raise MarketDataSessionError("market-data stream failed") from error
        else:
            self._state.require_snapshot("market-data stream ended")
        finally:
            if stream is not None:
                await _close_stream(stream)

    def _process_event(
        self, event: MarketDataEvent
    ) -> tuple[MarketDataSessionUpdate, bool]:
        """Apply one event or stop safely when the stream cannot stay synchronized."""
        if self._state.view.status is LocalBookStatus.AWAITING_SNAPSHOT:
            return self._process_bootstrap_event(event)
        return self._process_synchronized_event(event)

    def _process_bootstrap_event(
        self, event: MarketDataEvent
    ) -> tuple[MarketDataSessionUpdate, bool]:
        """Accept only a complete snapshot as proof of initial synchronization."""
        if isinstance(event, TradeObserved):
            return self._apply_event(event, terminal=False)
        if isinstance(event, MarketDataDisconnected):
            return self._apply_event(event, terminal=True)
        if isinstance(
            event, (PriceLevelChanged, TickSizeChanged, MarketDataReconnected)
        ):
            self._state.require_snapshot("authoritative snapshot required")
            raise MarketDataBootstrapError("received incremental data before snapshot")
        return self._apply_event(event, terminal=False)

    def _process_synchronized_event(
        self, event: MarketDataEvent
    ) -> tuple[MarketDataSessionUpdate, bool]:
        """Apply synchronized observations and stop at known terminal boundaries."""
        terminal = isinstance(
            event,
            (MarketDataDisconnected, MarketDataReconnected, TickSizeChanged),
        )
        update, _ = self._apply_event(event, terminal=terminal)
        return update, terminal or update.book_view.status is LocalBookStatus.INVALID

    def _apply_event(
        self, event: MarketDataEvent, *, terminal: bool
    ) -> tuple[MarketDataSessionUpdate, bool]:
        """Delegate event validation and mutation to the local state machine."""
        previous_book: OrderBook | None = self._state.view.book
        try:
            view = self._state.apply(event)
        except LocalOrderBookStateError as error:
            self._state.require_snapshot("market-data synchronization lost")
            raise MarketDataSynchronizationLost(
                "market-data synchronization lost"
            ) from error
        return (
            MarketDataSessionUpdate(event, view, previous_book != view.book),
            terminal,
        )


async def _close_stream(stream: AsyncIterator[MarketDataEvent]) -> None:
    """Close a caller-provided async iterator when it exposes ``aclose``."""
    close = getattr(stream, "aclose", None)
    if close is None:
        return
    try:
        await close()
    except asyncio.CancelledError:
        raise
    except Exception:
        return
