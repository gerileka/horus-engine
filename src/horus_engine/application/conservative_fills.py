"""Conservative maker-fill inference using recorded public market data only.

The simulator deliberately prefers missed fills to invented fills.  Quotes join
behind all displayed liquidity, book decreases never advance their queue, and
only later public trades can fill them.  A trade beyond a resting quote's price
is treated as strong evidence that its complete remaining quantity filled.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from horus_engine.domain import NonNegativeQuantity, OrderBook, Price, Quantity, Side

from .errors import ConservativeFillIntegrityError
from .events import BookSnapshotReceived, PriceLevelChanged, TradeObserved
from .market_data_journal import MarketDataSessionId
from .shadow_quotes import (
    ShadowQuote,
    ShadowQuoteAction,
    ShadowQuoteAnalysis,
    ShadowQuoteDecision,
)


class ConservativeFillTrigger(Enum):
    """The only public evidence types considered sufficient for a maker fill."""

    EXACT_PRICE_AFTER_QUEUE = "exact_price_after_queue"
    TRADE_THROUGH = "trade_through"


def _require_aware(timestamp: datetime) -> None:
    """Reject timestamps without an explicit UTC offset."""
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("simulated fill timestamp must be timezone-aware")


@dataclass(frozen=True)
class SimulatedMakerFill:
    """One evidence-backed hypothetical maker execution at the quote price."""

    session_id: MarketDataSessionId
    sequence_number: int
    observed_at: datetime
    order_side: Side
    quote_price: Price
    fill_quantity: Quantity
    aggressor_side: Side
    observed_trade_price: Price
    observed_trade_quantity: Quantity
    trigger: ConservativeFillTrigger
    queue_ahead_before: NonNegativeQuantity
    queue_ahead_after: NonNegativeQuantity
    order_remaining_after: NonNegativeQuantity

    def __post_init__(self) -> None:
        """Keep serialized evidence typed, positive, and directionally coherent."""
        if not isinstance(self.session_id, MarketDataSessionId):
            raise ValueError("simulated fill session_id must be a MarketDataSessionId")
        if (
            isinstance(self.sequence_number, bool)
            or not isinstance(self.sequence_number, int)
            or self.sequence_number < 1
        ):
            raise ValueError("simulated fill sequence number must be positive")
        _require_aware(self.observed_at)
        if not isinstance(self.order_side, Side) or not isinstance(
            self.aggressor_side, Side
        ):
            raise ValueError("simulated fill sides must be Side values")
        if self.aggressor_side is self.order_side:
            raise ValueError("simulated fill aggressor must oppose resting order")
        if not all(
            isinstance(value, expected)
            for value, expected in (
                (self.quote_price, Price),
                (self.fill_quantity, Quantity),
                (self.observed_trade_price, Price),
                (self.observed_trade_quantity, Quantity),
                (self.queue_ahead_before, NonNegativeQuantity),
                (self.queue_ahead_after, NonNegativeQuantity),
                (self.order_remaining_after, NonNegativeQuantity),
            )
        ):
            raise ValueError("simulated fill values must use domain value types")
        if not isinstance(self.trigger, ConservativeFillTrigger):
            raise ValueError("simulated fill trigger must be ConservativeFillTrigger")
        if self.queue_ahead_after.value > self.queue_ahead_before.value:
            raise ValueError("simulated fill queue ahead cannot increase")
        if self.trigger is ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE:
            if self.observed_trade_price != self.quote_price:
                raise ValueError("exact-price fill trade price must equal quote price")
        elif (
            self.order_side is Side.BUY
            and self.observed_trade_price >= self.quote_price
        ) or (
            self.order_side is Side.SELL
            and self.observed_trade_price <= self.quote_price
        ):
            raise ValueError("trade-through fill must pass through quote price")


@dataclass(frozen=True)
class SimulatedMakerOrder:
    """Private immutable queue position for one resting simulated quote side."""

    side: Side
    price: Price
    original_quantity: Quantity
    remaining_quantity: NonNegativeQuantity
    queue_ahead: NonNegativeQuantity

    def __post_init__(self) -> None:
        """Require an unfilled amount within the initially requested quantity."""
        if not isinstance(self.side, Side) or not isinstance(self.price, Price):
            raise ValueError("simulated order side and price must be valid")
        if not isinstance(self.original_quantity, Quantity):
            raise ValueError("simulated order original quantity must be Quantity")
        if not isinstance(
            self.remaining_quantity, NonNegativeQuantity
        ) or not isinstance(self.queue_ahead, NonNegativeQuantity):
            raise ValueError("simulated order state must use NonNegativeQuantity")
        if self.remaining_quantity.value > self.original_quantity.value:
            raise ValueError(
                "simulated order remaining quantity exceeds original quantity"
            )


@dataclass(frozen=True)
class SimulatedMakerOrderView:
    """Immutable diagnostic view of an active order side after one replay event."""

    side: Side
    price: Price
    original_quantity: Quantity
    remaining_quantity: NonNegativeQuantity
    queue_ahead: NonNegativeQuantity

    @classmethod
    def from_order(cls, order: SimulatedMakerOrder) -> "SimulatedMakerOrderView":
        """Copy internal state without giving callers an order lifecycle handle."""
        return cls(
            order.side,
            order.price,
            order.original_quantity,
            order.remaining_quantity,
            order.queue_ahead,
        )


@dataclass(frozen=True)
class ConservativeFillSimulationStep:
    """One post-event simulation state, including fills by previously active orders."""

    decision: ShadowQuoteDecision
    fills: tuple[SimulatedMakerFill, ...]
    active_bid: SimulatedMakerOrderView | None
    active_ask: SimulatedMakerOrderView | None

    def __post_init__(self) -> None:
        """Prevent mutable collections and malformed diagnostic state from escaping."""
        if not isinstance(self.fills, tuple):
            raise ValueError("simulation step fills must be a tuple")
        if self.active_bid is not None and self.active_bid.side is not Side.BUY:
            raise ValueError("simulation step active bid must be BUY")
        if self.active_ask is not None and self.active_ask.side is not Side.SELL:
            raise ValueError("simulation step active ask must be SELL")


@dataclass(frozen=True)
class ConservativeFillAnalysis:
    """Immutable conservative fill evidence and Decimal-only aggregate metrics."""

    shadow_analysis: ShadowQuoteAnalysis
    steps: tuple[ConservativeFillSimulationStep, ...]
    fills: tuple[SimulatedMakerFill, ...]
    _qualifying_trade_events: int
    _fully_filled_buy_orders: int
    _fully_filled_sell_orders: int

    def __post_init__(self) -> None:
        """Reconcile all externally visible aggregate data with its source fills."""
        if len(self.steps) != self.shadow_analysis.total_steps:
            raise ValueError("simulation steps must match shadow decisions")
        if tuple(fill for step in self.steps for fill in step.fills) != self.fills:
            raise ValueError("simulation fills must reconcile with simulation steps")
        if any(
            value < 0
            for value in (
                self._qualifying_trade_events,
                self._fully_filled_buy_orders,
                self._fully_filled_sell_orders,
            )
        ):
            raise ValueError("simulation counts must be non-negative")
        if self.buy_fill_events + self.sell_fill_events != self.fill_events:
            raise ValueError("simulation fill-side counts must reconcile")

    @property
    def total_steps(self) -> int:
        """Return the number of replay decisions simulated."""
        return len(self.steps)

    @property
    def trade_events_observed(self) -> int:
        """Return recorded public trade events, regardless of eligibility."""
        return sum(
            isinstance(step.decision.replay_step.event, TradeObserved)
            for step in self.steps
        )

    @property
    def qualifying_trade_events(self) -> int:
        """Return active-order trades with matching aggressor and price direction."""
        return self._qualifying_trade_events

    @property
    def fill_events(self) -> int:
        """Return evidence-backed simulated fill records."""
        return len(self.fills)

    @property
    def buy_fill_events(self) -> int:
        """Return fills of our resting bids."""
        return self._fill_count(Side.BUY)

    @property
    def sell_fill_events(self) -> int:
        """Return fills of our resting asks."""
        return self._fill_count(Side.SELL)

    @property
    def buy_filled_quantity(self) -> Decimal:
        """Return total contracts bought by filled resting bids."""
        return self._quantity(Side.BUY)

    @property
    def sell_filled_quantity(self) -> Decimal:
        """Return total contracts sold by filled resting asks."""
        return self._quantity(Side.SELL)

    @property
    def buy_notional(self) -> Decimal:
        """Return quoted-price Decimal notional paid for buys."""
        return self._notional(Side.BUY)

    @property
    def sell_notional(self) -> Decimal:
        """Return quoted-price Decimal notional received for sells."""
        return self._notional(Side.SELL)

    @property
    def net_token_position(self) -> Decimal:
        """Return bought contracts minus sold contracts, without valuation."""
        return self.buy_filled_quantity - self.sell_filled_quantity

    @property
    def net_cash_flow(self) -> Decimal:
        """Return sell notional minus buy notional; this is not profit or P&L."""
        return self.sell_notional - self.buy_notional

    @property
    def fully_filled_buy_orders(self) -> int:
        """Return completed simulated resting-bid order sides."""
        return self._fully_filled_buy_orders

    @property
    def fully_filled_sell_orders(self) -> int:
        """Return completed simulated resting-ask order sides."""
        return self._fully_filled_sell_orders

    @property
    def remaining_bid_quantity(self) -> Decimal:
        """Return terminal unfilled resting bid quantity."""
        return self._remaining(Side.BUY)

    @property
    def remaining_ask_quantity(self) -> Decimal:
        """Return terminal unfilled resting ask quantity."""
        return self._remaining(Side.SELL)

    def _fill_count(self, side: Side) -> int:
        """Count fills for one simulated resting side."""
        return sum(fill.order_side is side for fill in self.fills)

    def _quantity(self, side: Side) -> Decimal:
        """Sum fills for one resting side without binary arithmetic."""
        return sum(
            (
                fill.fill_quantity.value
                for fill in self.fills
                if fill.order_side is side
            ),
            Decimal("0"),
        )

    def _notional(self, side: Side) -> Decimal:
        """Sum quote-price multiplied fill quantity for one resting side."""
        return sum(
            (
                fill.quote_price.value * fill.fill_quantity.value
                for fill in self.fills
                if fill.order_side is side
            ),
            Decimal("0"),
        )

    def _remaining(self, side: Side) -> Decimal:
        """Read final active state for one side, allowing a fully filled absence."""
        if not self.steps:
            return Decimal("0")
        order = (
            self.steps[-1].active_bid if side is Side.BUY else self.steps[-1].active_ask
        )
        return Decimal("0") if order is None else order.remaining_quantity.value


class ConservativeFillSimulator:
    """Simulate conservative public-data maker fills over shadow decisions.

    Orders are activated only after each recorded event and its shadow action.
    The next event may use trades as fill evidence; displayed size reductions do
    not reduce queue ahead.  This intentionally omits hidden liquidity, venue
    acknowledgements, fees, rebates, rewards, and any profitability conclusion.
    """

    def simulate(
        self, shadow_analysis: ShadowQuoteAnalysis
    ) -> ConservativeFillAnalysis:
        """Return deterministic fill evidence without I/O, mutation, or clocks."""
        active_bid: SimulatedMakerOrder | None = None
        active_ask: SimulatedMakerOrder | None = None
        previous_book: OrderBook | None = None
        steps: list[ConservativeFillSimulationStep] = []
        fills: list[SimulatedMakerFill] = []
        qualifying_trades = 0
        fully_filled_buy_orders = 0
        fully_filled_sell_orders = 0
        for index, decision in enumerate(shadow_analysis.decisions):
            self._validate_decision(shadow_analysis, decision, index)
            step = decision.replay_step
            event_fills: tuple[SimulatedMakerFill, ...] = ()
            if isinstance(step.event, TradeObserved):
                target = (
                    active_bid if step.event.aggressor_side is Side.SELL else active_ask
                )
                if target is not None and self._is_qualifying(target, step.event):
                    qualifying_trades += 1
                    fill, replacement = self._apply_trade(
                        target, step.session_id, step.sequence_number, step.event
                    )
                    event_fills = () if fill is None else (fill,)
                    if target.side is Side.BUY:
                        active_bid = replacement
                    else:
                        active_ask = replacement
                    if fill is not None and replacement is None:
                        if target.side is Side.BUY:
                            fully_filled_buy_orders += 1
                        else:
                            fully_filled_sell_orders += 1
            active_bid, active_ask = self._update_queue_bounds(
                decision, previous_book, active_bid, active_ask
            )
            active_bid, active_ask = self._apply_lifecycle(
                decision, active_bid, active_ask
            )
            fills.extend(event_fills)
            steps.append(
                ConservativeFillSimulationStep(
                    decision,
                    event_fills,
                    None
                    if active_bid is None
                    else SimulatedMakerOrderView.from_order(active_bid),
                    None
                    if active_ask is None
                    else SimulatedMakerOrderView.from_order(active_ask),
                )
            )
            previous_book = step.view.book
        return ConservativeFillAnalysis(
            shadow_analysis,
            tuple(steps),
            tuple(fills),
            qualifying_trades,
            fully_filled_buy_orders,
            fully_filled_sell_orders,
        )

    @staticmethod
    def _validate_decision(
        analysis: ShadowQuoteAnalysis, decision: ShadowQuoteDecision, index: int
    ) -> None:
        """Require the simulation to process each recorded shadow decision once."""
        expected = analysis.replay.steps[index]
        if decision.replay_step != expected:
            raise ConservativeFillSimulator._integrity(
                analysis.replay.session.session_id,
                decision.replay_step.sequence_number,
                "decision replay step does not match analysis replay",
            )

    @staticmethod
    def _is_qualifying(order: SimulatedMakerOrder, trade: TradeObserved) -> bool:
        """Check direction and price evidence before modifying any queue state."""
        if order.side is Side.BUY:
            return trade.aggressor_side is Side.SELL and trade.price <= order.price
        return trade.aggressor_side is Side.BUY and trade.price >= order.price

    @staticmethod
    def _apply_trade(
        order: SimulatedMakerOrder,
        session_id: MarketDataSessionId,
        sequence_number: int,
        trade: TradeObserved,
    ) -> tuple[SimulatedMakerFill | None, SimulatedMakerOrder | None]:
        """Consume queue for an exact trade or fully fill on a trade-through."""
        is_exact = trade.price == order.price
        queue_before = order.queue_ahead
        if is_exact:
            queue_after = NonNegativeQuantity(
                max(Decimal("0"), queue_before.value - trade.quantity.value)
            )
            fill_value = min(
                order.remaining_quantity.value,
                max(Decimal("0"), trade.quantity.value - queue_before.value),
            )
            trigger = ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE
        else:
            queue_after = NonNegativeQuantity("0")
            fill_value = order.remaining_quantity.value
            trigger = ConservativeFillTrigger.TRADE_THROUGH
        if fill_value == 0:
            return None, SimulatedMakerOrder(
                order.side,
                order.price,
                order.original_quantity,
                order.remaining_quantity,
                queue_after,
            )
        remaining_after = NonNegativeQuantity(
            order.remaining_quantity.value - fill_value
        )
        fill = SimulatedMakerFill(
            session_id,
            sequence_number,
            trade.observed_at,
            order.side,
            order.price,
            Quantity(fill_value),
            trade.aggressor_side,
            trade.price,
            trade.quantity,
            trigger,
            queue_before,
            queue_after,
            remaining_after,
        )
        if remaining_after.value == 0:
            return fill, None
        return fill, SimulatedMakerOrder(
            order.side,
            order.price,
            order.original_quantity,
            remaining_after,
            queue_after,
        )

    def _update_queue_bounds(
        self,
        decision: ShadowQuoteDecision,
        previous_book: OrderBook | None,
        active_bid: SimulatedMakerOrder | None,
        active_ask: SimulatedMakerOrder | None,
    ) -> tuple[SimulatedMakerOrder | None, SimulatedMakerOrder | None]:
        """Only increase queue-ahead estimates from public book observations."""
        event = decision.replay_step.event
        if isinstance(event, PriceLevelChanged):
            if active_bid is not None:
                active_bid = self._apply_level_increase(
                    active_bid, event, previous_book, decision.replay_step.view.book
                )
            if active_ask is not None:
                active_ask = self._apply_level_increase(
                    active_ask, event, previous_book, decision.replay_step.view.book
                )
        if (
            isinstance(event, BookSnapshotReceived)
            and decision.action is ShadowQuoteAction.HOLD
        ):
            active_bid = self._raise_to_visible(
                active_bid, decision.replay_step.view.book
            )
            active_ask = self._raise_to_visible(
                active_ask, decision.replay_step.view.book
            )
        return active_bid, active_ask

    @staticmethod
    def _apply_level_increase(
        order: SimulatedMakerOrder,
        event: PriceLevelChanged,
        before: OrderBook | None,
        after: OrderBook | None,
    ) -> SimulatedMakerOrder:
        """Treat only same-price visible growth as newly queued ahead of us."""
        if event.side is not order.side or event.price != order.price:
            return order
        increase = max(
            Decimal("0"),
            ConservativeFillSimulator._visible_quantity(after, order.side, order.price)
            - ConservativeFillSimulator._visible_quantity(
                before, order.side, order.price
            ),
        )
        return SimulatedMakerOrder(
            order.side,
            order.price,
            order.original_quantity,
            order.remaining_quantity,
            NonNegativeQuantity(order.queue_ahead.value + increase),
        )

    @staticmethod
    def _raise_to_visible(
        order: SimulatedMakerOrder | None, book: OrderBook | None
    ) -> SimulatedMakerOrder | None:
        """Never let an authoritative snapshot lower a conservative queue bound."""
        if order is None:
            return None
        queue_ahead = max(
            order.queue_ahead.value,
            ConservativeFillSimulator._visible_quantity(book, order.side, order.price),
        )
        return SimulatedMakerOrder(
            order.side,
            order.price,
            order.original_quantity,
            order.remaining_quantity,
            NonNegativeQuantity(queue_ahead),
        )

    def _apply_lifecycle(
        self,
        decision: ShadowQuoteDecision,
        active_bid: SimulatedMakerOrder | None,
        active_ask: SimulatedMakerOrder | None,
    ) -> tuple[SimulatedMakerOrder | None, SimulatedMakerOrder | None]:
        """Activate quotes after their event, preserving partial fill state on HOLD."""
        action = decision.action
        if action is ShadowQuoteAction.PLACE:
            if active_bid is not None or active_ask is not None:
                raise self._integrity(
                    decision.replay_step.session_id,
                    decision.replay_step.sequence_number,
                    "PLACE requires no active simulated sides",
                )
            return self._new_quote(decision, decision.desired_quote)
        if action is ShadowQuoteAction.HOLD:
            if decision.desired_quote is None:
                raise self._integrity(
                    decision.replay_step.session_id,
                    decision.replay_step.sequence_number,
                    "HOLD requires a desired quote",
                )
            return active_bid, active_ask
        if action is ShadowQuoteAction.REPLACE:
            return self._new_quote(decision, decision.desired_quote)
        if action is ShadowQuoteAction.CANCEL:
            if decision.desired_quote is not None:
                raise self._integrity(
                    decision.replay_step.session_id,
                    decision.replay_step.sequence_number,
                    "CANCEL cannot have a desired quote",
                )
            return None, None
        if action is ShadowQuoteAction.NONE:
            if (
                decision.desired_quote is not None
                or active_bid is not None
                or active_ask is not None
            ):
                raise self._integrity(
                    decision.replay_step.session_id,
                    decision.replay_step.sequence_number,
                    "NONE requires no desired or active simulated sides",
                )
            return None, None

    def _new_quote(
        self, decision: ShadowQuoteDecision, quote: ShadowQuote | None
    ) -> tuple[SimulatedMakerOrder, SimulatedMakerOrder]:
        """Join each quote side behind visible same-price liquidity after the event."""
        if quote is None:
            raise self._integrity(
                decision.replay_step.session_id,
                decision.replay_step.sequence_number,
                "quote action requires a desired quote",
            )
        book = decision.replay_step.view.book
        return (
            SimulatedMakerOrder(
                Side.BUY,
                quote.bid,
                quote.quantity,
                NonNegativeQuantity(quote.quantity.value),
                NonNegativeQuantity(self._visible_quantity(book, Side.BUY, quote.bid)),
            ),
            SimulatedMakerOrder(
                Side.SELL,
                quote.ask,
                quote.quantity,
                NonNegativeQuantity(quote.quantity.value),
                NonNegativeQuantity(self._visible_quantity(book, Side.SELL, quote.ask)),
            ),
        )

    @staticmethod
    def _visible_quantity(book: OrderBook | None, side: Side, price: Price) -> Decimal:
        """Read exact-price aggregate liquidity, excluding the simulated order."""
        if book is None:
            return Decimal("0")
        levels = book.bids if side is Side.BUY else book.asks
        return next(
            (level.quantity.value for level in levels if level.price == price),
            Decimal("0"),
        )

    @staticmethod
    def _integrity(
        session_id: MarketDataSessionId, sequence_number: int, reason: str
    ) -> ConservativeFillIntegrityError:
        """Build concise failures without leaking full books or raw event payloads."""
        return ConservativeFillIntegrityError(
            "conservative fill integrity error for session "
            f"{session_id} sequence {sequence_number}: {reason}"
        )
