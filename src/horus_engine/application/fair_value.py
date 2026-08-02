"""Small, exchange-neutral estimates derived from valid top-of-book state."""

from decimal import Decimal
from typing import Protocol

from horus_engine.domain import OrderBook, Price


class FairValueModel(Protocol):
    """Estimate a price from one observed order book when it is usable."""

    @property
    def name(self) -> str:
        """Return the stable model name used in experiment output."""
        ...

    def estimate(self, book: OrderBook) -> Price | None:
        """Return a fair-value estimate, or ``None`` for an invalid top of book."""
        ...


def _has_valid_two_sided_top(book: OrderBook) -> bool:
    """Require a strictly positive spread before estimating a fair value."""
    return (
        book.best_bid is not None
        and book.best_ask is not None
        and not book.is_locked
        and not book.is_crossed
    )


class MidpointFairValueModel:
    """Estimate fair value as the exact midpoint of a valid best bid and ask."""

    @property
    def name(self) -> str:
        """Return the stable midpoint model identifier."""
        return "midpoint"

    def estimate(self, book: OrderBook) -> Price | None:
        """Return the unrounded best-price midpoint for a valid two-sided book."""
        if not _has_valid_two_sided_top(book):
            return None
        assert book.best_bid is not None
        assert book.best_ask is not None
        return Price((book.best_bid.value + book.best_ask.value) / Decimal("2"))


class TopLevelMicropriceFairValueModel:
    """Estimate top-of-book imbalance, without asserting it provides an edge.

    This intentionally uses only the best-level quantities. It is a simple
    imbalance estimator for empirical comparison, not a proven edge.
    """

    @property
    def name(self) -> str:
        """Return the stable top-level microprice model identifier."""
        return "microprice"

    def estimate(self, book: OrderBook) -> Price | None:
        """Return the exact quantity-weighted best-level estimate if valid."""
        if not _has_valid_two_sided_top(book):
            return None
        best_bid = book.bids[0]
        best_ask = book.asks[0]
        numerator = (
            best_ask.price.value * best_bid.quantity.value
            + best_bid.price.value * best_ask.quantity.value
        )
        denominator = best_bid.quantity.value + best_ask.quantity.value
        return Price(numerator / denominator)
