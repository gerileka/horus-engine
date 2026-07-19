"""Immutable aggregate order-book snapshots."""

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from .errors import DuplicatePriceLevel
from .values import NonNegativeQuantity, Price, Quantity


@dataclass(frozen=True)
class OrderBookLevel:
    """Aggregate resting liquidity available at one price."""

    price: Price
    quantity: Quantity


@dataclass(frozen=True, init=False)
class OrderBook:
    """An observed, immutable snapshot of aggregate bid and ask liquidity.

    The snapshot deliberately permits locked and crossed books so consumers can
    inspect the state reported by an exchange instead of losing that observation.
    """

    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]

    def __init__(
        self,
        bids: Iterable[OrderBookLevel] = (),
        asks: Iterable[OrderBookLevel] = (),
    ) -> None:
        """Copy inputs, reject duplicate side prices, and normalize ordering."""
        bid_levels = tuple(bids)
        ask_levels = tuple(asks)
        self._ensure_unique_prices(bid_levels, "bid")
        self._ensure_unique_prices(ask_levels, "ask")
        object.__setattr__(
            self,
            "bids",
            tuple(sorted(bid_levels, key=lambda level: level.price, reverse=True)),
        )
        object.__setattr__(
            self,
            "asks",
            tuple(sorted(ask_levels, key=lambda level: level.price)),
        )

    @staticmethod
    def _ensure_unique_prices(levels: tuple[OrderBookLevel, ...], side: str) -> None:
        """Raise when a single order-book side repeats a price."""
        if len({level.price for level in levels}) != len(levels):
            raise DuplicatePriceLevel(f"duplicate {side} price level")

    @property
    def best_bid(self) -> Price | None:
        """Return the highest bid price, if a bid is available."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Price | None:
        """Return the lowest ask price, if an ask is available."""
        return self.asks[0].price if self.asks else None

    @property
    def midpoint(self) -> Price | None:
        """Return the Decimal-safe midpoint when both sides are available."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return Price((self.best_bid.value + self.best_ask.value) / Decimal("2"))

    @property
    def spread(self) -> Decimal | None:
        """Return the signed Decimal spread (best ask minus best bid), if known."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask.value - self.best_bid.value

    @property
    def is_crossed(self) -> bool:
        """Return whether the best bid is greater than the best ask."""
        return (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid > self.best_ask
        )

    @property
    def is_locked(self) -> bool:
        """Return whether the best bid equals the best ask."""
        return self.best_bid is not None and self.best_bid == self.best_ask

    @property
    def total_bid_quantity(self) -> NonNegativeQuantity:
        """Return aggregate visible bid quantity, including zero for no bids."""
        return self._sum_quantities(self.bids)

    @property
    def total_ask_quantity(self) -> NonNegativeQuantity:
        """Return aggregate visible ask quantity, including zero for no asks."""
        return self._sum_quantities(self.asks)

    def cumulative_bid_quantity(self, price: Price) -> NonNegativeQuantity:
        """Return bid quantity available at or above ``price``."""
        return self._sum_quantities(
            level for level in self.bids if level.price >= price
        )

    def cumulative_ask_quantity(self, price: Price) -> NonNegativeQuantity:
        """Return ask quantity available at or below ``price``."""
        return self._sum_quantities(
            level for level in self.asks if level.price <= price
        )

    @staticmethod
    def _sum_quantities(levels: Iterable[OrderBookLevel]) -> NonNegativeQuantity:
        """Sum aggregate quantities without float arithmetic."""
        return NonNegativeQuantity(
            sum((level.quantity.value for level in levels), Decimal("0"))
        )
