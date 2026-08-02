"""Deterministic hypothetical maker quotes over recorded replay steps only."""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from horus_engine.domain import (
    OrderBook,
    Price,
    Quantity,
    TickAlignmentError,
    TickSize,
    is_tick_aligned,
    round_price_down_to_tick,
    round_price_up_to_tick,
)

from .fair_value import FairValueModel
from .market_data_replay import MarketDataReplayResult, MarketDataReplayStep
from .order_book_state import LocalBookStatus, LocalOrderBookView


@dataclass(frozen=True)
class ShadowQuoteConfig:
    """Fixed two-sided quote settings for one recorded-session experiment."""

    half_spread_ticks: int
    quantity: Quantity

    def __post_init__(self) -> None:
        """Require a positive integer width and the existing quantity value type."""
        if (
            isinstance(self.half_spread_ticks, bool)
            or not isinstance(self.half_spread_ticks, int)
            or self.half_spread_ticks <= 0
        ):
            raise ValueError("half spread ticks must be a strictly positive integer")
        if not isinstance(self.quantity, Quantity):
            raise ValueError("quantity must be a Quantity")


@dataclass(frozen=True)
class ShadowQuote:
    """A valid, tick-aligned two-sided quote that would rest as a maker."""

    bid: Price
    ask: Price
    quantity: Quantity

    def __post_init__(self) -> None:
        """Keep the quote explicitly two-sided and ordered."""
        if not isinstance(self.bid, Price) or not isinstance(self.ask, Price):
            raise ValueError("shadow quote prices must be Price values")
        if not isinstance(self.quantity, Quantity):
            raise ValueError("shadow quote quantity must be a Quantity")
        if not (Price("0") < self.bid < self.ask < Price("1")):
            raise ValueError("shadow quote prices must be strictly inside zero and one")


class ShadowQuoteAction(Enum):
    """The one lifecycle action implied by a desired quote at one replay step."""

    NONE = "none"
    PLACE = "place"
    HOLD = "hold"
    REPLACE = "replace"
    CANCEL = "cancel"


class ShadowQuoteSuppressionReason(Enum):
    """Stable reasons why a replay state has no desired two-sided quote."""

    BOOK_NOT_SYNCHRONIZED = "book_not_synchronized"
    BOOK_UNAVAILABLE = "book_unavailable"
    BOOK_ONE_SIDED = "book_one_sided"
    BOOK_LOCKED = "book_locked"
    BOOK_CROSSED = "book_crossed"
    FAIR_VALUE_UNAVAILABLE = "fair_value_unavailable"
    PRICE_BOUNDARY = "price_boundary"
    NO_QUOTING_ROOM = "no_quoting_room"


@dataclass(frozen=True)
class ShadowQuoteDecision:
    """Deterministic desired state and lifecycle action after one replay event."""

    replay_step: MarketDataReplayStep
    fair_value: Price | None
    desired_quote: ShadowQuote | None
    action: ShadowQuoteAction
    suppression_reason: ShadowQuoteSuppressionReason | None

    def __post_init__(self) -> None:
        """Keep quoteability and typed suppression evidence mutually exclusive."""
        if not isinstance(self.action, ShadowQuoteAction):
            raise ValueError("shadow quote action must be a ShadowQuoteAction")
        if self.desired_quote is None and self.suppression_reason is None:
            raise ValueError("suppressed decisions require a suppression reason")
        if self.desired_quote is not None and self.suppression_reason is not None:
            raise ValueError("quoteable decisions cannot have a suppression reason")

    @property
    def quoteable(self) -> bool:
        """Return whether this replay state supports a valid desired quote."""
        return self.desired_quote is not None


@dataclass(frozen=True)
class ShadowQuoteAnalysis:
    """Complete empirical result for one model and one recorded session."""

    replay: MarketDataReplayResult
    model_name: str
    config: ShadowQuoteConfig
    decisions: tuple[ShadowQuoteDecision, ...]

    def __post_init__(self) -> None:
        """Require one decision per replay step and reconciled lifecycle counts."""
        if not isinstance(self.decisions, tuple):
            raise ValueError("shadow quote decisions must be a tuple")
        if len(self.decisions) != len(self.replay.steps):
            raise ValueError("shadow quote decisions must match replay steps")
        if (
            self.placements
            + self.replacements
            + self.holds
            + self.cancellations
            + self.none_actions
            != self.total_steps
        ):
            raise ValueError("shadow quote action counts must match total steps")

    @property
    def total_steps(self) -> int:
        """Return the number of recorded post-event states evaluated."""
        return len(self.decisions)

    @property
    def quoteable_steps(self) -> int:
        """Return how often the recorded state allowed a desired two-sided quote."""
        return sum(decision.quoteable for decision in self.decisions)

    @property
    def unquoteable_steps(self) -> int:
        """Return how often the recorded state could not support a desired quote."""
        return self.total_steps - self.quoteable_steps

    @property
    def suppressed_steps(self) -> int:
        """Return the number of replay states without a desired quote."""
        return self.unquoteable_steps

    @property
    def quoteable_ratio(self) -> Decimal:
        """Return the Decimal fraction of evaluated states that were quoteable."""
        if self.total_steps == 0:
            return Decimal("0")
        return Decimal(self.quoteable_steps) / Decimal(self.total_steps)

    @property
    def placements(self) -> int:
        """Return the number of desired quotes placed from an inactive state."""
        return self._action_count(ShadowQuoteAction.PLACE)

    @property
    def replacements(self) -> int:
        """Return the number of active quotes replaced with a new desired quote."""
        return self._action_count(ShadowQuoteAction.REPLACE)

    @property
    def holds(self) -> int:
        """Return the number of active quotes retained unchanged."""
        return self._action_count(ShadowQuoteAction.HOLD)

    @property
    def cancellations(self) -> int:
        """Return the number of active quotes removed after suppression."""
        return self._action_count(ShadowQuoteAction.CANCEL)

    @property
    def none_actions(self) -> int:
        """Return suppressed states where no active quote existed to cancel."""
        return self._action_count(ShadowQuoteAction.NONE)

    def _action_count(self, action: ShadowQuoteAction) -> int:
        """Count one lifecycle action without exposing mutable aggregation state."""
        return sum(decision.action is action for decision in self.decisions)


class ShadowQuoteAnalyzer:
    """Evaluate a fixed maker-quote rule over a deterministic journal replay."""

    def __init__(self, model: FairValueModel, config: ShadowQuoteConfig) -> None:
        """Retain the model and immutable quote configuration for one experiment."""
        self._model = model
        self._config = config

    def analyze(self, replay: MarketDataReplayResult) -> ShadowQuoteAnalysis:
        """Generate desired quotes and lifecycle actions for every replay step."""
        active_quote: ShadowQuote | None = None
        decisions: list[ShadowQuoteDecision] = []
        for step in replay.steps:
            fair_value, desired_quote, suppression_reason = self._desired_quote(
                step.view
            )
            action = self._action(active_quote, desired_quote)
            if action is ShadowQuoteAction.CANCEL:
                active_quote = None
            elif desired_quote is not None:
                active_quote = desired_quote
            decisions.append(
                ShadowQuoteDecision(
                    step, fair_value, desired_quote, action, suppression_reason
                )
            )
        return ShadowQuoteAnalysis(
            replay, self._model.name, self._config, tuple(decisions)
        )

    def _desired_quote(
        self, view: LocalOrderBookView
    ) -> tuple[Price | None, ShadowQuote | None, ShadowQuoteSuppressionReason | None]:
        """Estimate and construct a maker quote only from synchronized state."""
        if view.status is not LocalBookStatus.SYNCHRONIZED:
            return None, None, ShadowQuoteSuppressionReason.BOOK_NOT_SYNCHRONIZED
        if view.book is None:
            return None, None, ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE
        book_reason = self._book_suppression_reason(view.book)
        if book_reason is not None:
            return None, None, book_reason
        fair_value = self._model.estimate(view.book)
        if fair_value is None:
            return None, None, ShadowQuoteSuppressionReason.FAIR_VALUE_UNAVAILABLE
        quote, quote_reason = self._quote_from_fair_value(
            fair_value, view.tick_size, view.book
        )
        if quote is None:
            return fair_value, None, quote_reason
        return fair_value, quote, None

    @staticmethod
    def _book_suppression_reason(
        book: OrderBook,
    ) -> ShadowQuoteSuppressionReason | None:
        """Classify unusable observed top-of-book states before model evaluation."""
        if book.best_bid is None or book.best_ask is None:
            return (
                ShadowQuoteSuppressionReason.BOOK_ONE_SIDED
                if book.bids or book.asks
                else ShadowQuoteSuppressionReason.FAIR_VALUE_UNAVAILABLE
            )
        if book.is_locked:
            return ShadowQuoteSuppressionReason.BOOK_LOCKED
        if book.is_crossed:
            return ShadowQuoteSuppressionReason.BOOK_CROSSED
        return None

    def _quote_from_fair_value(
        self, fair_value: Price, tick_size: TickSize, book: OrderBook
    ) -> tuple[ShadowQuote | None, ShadowQuoteSuppressionReason | None]:
        """Round a symmetric fair-value band outward to valid maker price ticks."""
        offset = tick_size.value * Decimal(self._config.half_spread_ticks)
        bid_value = fair_value.value - offset
        ask_value = fair_value.value + offset
        if bid_value <= Decimal("0") or ask_value >= Decimal("1"):
            return None, ShadowQuoteSuppressionReason.PRICE_BOUNDARY
        bid = round_price_down_to_tick(Price(bid_value), tick_size)
        try:
            ask = round_price_up_to_tick(Price(ask_value), tick_size)
        except TickAlignmentError:
            return None, ShadowQuoteSuppressionReason.PRICE_BOUNDARY
        if bid <= Price("0") or ask >= Price("1"):
            return None, ShadowQuoteSuppressionReason.PRICE_BOUNDARY
        if (
            bid >= ask
            or not is_tick_aligned(bid, tick_size)
            or not is_tick_aligned(ask, tick_size)
            or not self._rests_as_maker(bid, ask, book)
        ):
            return None, ShadowQuoteSuppressionReason.NO_QUOTING_ROOM
        return ShadowQuote(bid, ask, self._config.quantity), None

    @staticmethod
    def _rests_as_maker(bid: Price, ask: Price, book: OrderBook) -> bool:
        """Require that neither quote would take the observed opposite best level."""
        assert book.best_bid is not None
        assert book.best_ask is not None
        return bid < book.best_ask and ask > book.best_bid

    @staticmethod
    def _action(
        active_quote: ShadowQuote | None, desired_quote: ShadowQuote | None
    ) -> ShadowQuoteAction:
        """Choose the minimal lifecycle action that reaches the desired state."""
        if desired_quote is None:
            return (
                ShadowQuoteAction.CANCEL
                if active_quote is not None
                else ShadowQuoteAction.NONE
            )
        if active_quote is None:
            return ShadowQuoteAction.PLACE
        return (
            ShadowQuoteAction.HOLD
            if active_quote == desired_quote
            else ShadowQuoteAction.REPLACE
        )
