"""Deterministic unit tests for conservative public-trade maker-fill evidence."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    ConservativeFillAnalysis,
    ConservativeFillIntegrityError,
    ConservativeFillSimulationStep,
    ConservativeFillSimulator,
    ConservativeFillTrigger,
    LocalBookStatus,
    LocalOrderBookView,
    MarketDataEvent,
    MarketDataReplayResult,
    MarketDataReplayStep,
    MarketDataSessionId,
    MarketId,
    PersistedMarketDataSession,
    PriceLevelChanged,
    ShadowQuote,
    ShadowQuoteAction,
    ShadowQuoteAnalysis,
    ShadowQuoteConfig,
    ShadowQuoteDecision,
    ShadowQuoteSuppressionReason,
    SimulatedMakerFill,
    TokenId,
    TradeObserved,
)
from horus_engine.application.conservative_fills import (
    SimulatedMakerOrder,
    SimulatedMakerOrderView,
)
from horus_engine.domain import (
    NonNegativeQuantity,
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_SESSION = MarketDataSessionId("fill-session")
_MARKET = MarketId("market")
_TOKEN = TokenId("token")
_TIME = datetime(2026, 8, 2, tzinfo=UTC)
_QUOTE = ShadowQuote(Price("0.40"), Price("0.60"), Quantity("5"))


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "10"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "8"),),
) -> OrderBook:
    """Build a compact exact aggregate book."""
    return OrderBook(
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in bids),
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in asks),
    )


def _view(book: OrderBook | None, timestamp: datetime = _TIME) -> LocalOrderBookView:
    """Build a synchronized post-event replay view."""
    return LocalOrderBookView(
        _MARKET,
        _TOKEN,
        TickSize("0.01"),
        LocalBookStatus.SYNCHRONIZED,
        book,
        timestamp,
        None,
    )


def _analysis(
    rows: list[
        tuple[MarketDataEvent, OrderBook | None, ShadowQuoteAction, ShadowQuote | None]
    ],
) -> ShadowQuoteAnalysis:
    """Build a shadow analysis with explicit lifecycle states for one event stream."""
    steps = tuple(
        MarketDataReplayStep(_SESSION, index, event, _view(book), False)
        for index, (event, book, _, _) in enumerate(rows, start=1)
    )
    session = PersistedMarketDataSession(
        _SESSION,
        _MARKET,
        _TOKEN,
        TickSize("0.01"),
        _TIME,
        None,
        None,
        None,
        None,
        None,
        len(steps),
    )
    replay = MarketDataReplayResult(session, steps, _view(None), _view(None), False)
    decisions = tuple(
        ShadowQuoteDecision(
            step,
            None,
            quote,
            action,
            None
            if quote is not None
            else ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
        )
        for step, (_, _, action, quote) in zip(steps, rows, strict=True)
    )
    return ShadowQuoteAnalysis(
        replay, "test", ShadowQuoteConfig(1, Quantity("5")), decisions
    )


def _trade(side: Side, price: str, quantity: str) -> TradeObserved:
    """Build one public trade with the fixture's fixed identity and timestamp."""
    return TradeObserved(_MARKET, _TOKEN, side, Price(price), Quantity(quantity), _TIME)


def test_exact_queue_fills_no_lookahead_partial_full_and_summary() -> None:
    """Only later exact public trades deplete queue and fill at our quote price."""
    snapshot = _book()
    analysis = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, snapshot, _TIME),
                snapshot,
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (_trade(Side.SELL, "0.40", "10"), snapshot, ShadowQuoteAction.HOLD, _QUOTE),
            (_trade(Side.SELL, "0.40", "3"), snapshot, ShadowQuoteAction.HOLD, _QUOTE),
            (_trade(Side.SELL, "0.40", "2"), snapshot, ShadowQuoteAction.HOLD, _QUOTE),
        ]
    )
    result = ConservativeFillSimulator().simulate(analysis)

    assert result.total_steps == 4
    assert result.trade_events_observed == 3
    assert result.qualifying_trade_events == 3
    assert result.fill_events == 2
    assert [fill.fill_quantity for fill in result.fills] == [
        Quantity("3"),
        Quantity("2"),
    ]
    assert all(fill.quote_price == Price("0.40") for fill in result.fills)
    assert all(
        fill.trigger is ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE
        for fill in result.fills
    )
    assert result.steps[0].fills == ()
    assert result.steps[1].active_bid is not None
    assert result.steps[1].active_bid.queue_ahead == NonNegativeQuantity("0")
    assert result.buy_filled_quantity == Decimal("5")
    assert result.buy_notional == Decimal("2.00")
    assert result.sell_filled_quantity == Decimal("0")
    assert result.net_token_position == Decimal("5")
    assert result.net_cash_flow == Decimal("-2.00")
    assert result.fully_filled_buy_orders == 1
    assert result.remaining_bid_quantity == Decimal("0")
    assert result.remaining_ask_quantity == Decimal("5")
    with pytest.raises(FrozenInstanceError):
        result.fills[0].quote_price = Price("0.5")  # type: ignore[misc]


def test_trade_through_queue_changes_snapshots_and_lifecycle() -> None:
    """Conservative queue bounds rise only on visible increases; through fills fully."""
    before = _book((("0.40", "1"),), (("0.60", "2"),))
    increased = _book((("0.40", "4"),), (("0.60", "2"),))
    larger_snapshot = _book((("0.40", "9"),), (("0.60", "2"),))
    analysis = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, before, _TIME),
                before,
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (
                PriceLevelChanged(
                    _MARKET,
                    _TOKEN,
                    Side.BUY,
                    Price("0.40"),
                    NonNegativeQuantity("4"),
                    _TIME,
                ),
                increased,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
            (
                PriceLevelChanged(
                    _MARKET,
                    _TOKEN,
                    Side.BUY,
                    Price("0.40"),
                    NonNegativeQuantity("1"),
                    _TIME,
                ),
                before,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
            (
                BookSnapshotReceived(_MARKET, _TOKEN, larger_snapshot, _TIME),
                larger_snapshot,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
            (
                _trade(Side.BUY, "0.61", "1"),
                larger_snapshot,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
            (
                _trade(Side.SELL, "0.39", "1"),
                larger_snapshot,
                ShadowQuoteAction.REPLACE,
                _QUOTE,
            ),
            (
                _trade(Side.BUY, "0.59", "1"),
                larger_snapshot,
                ShadowQuoteAction.CANCEL,
                None,
            ),
            (
                BookSnapshotReceived(_MARKET, _TOKEN, before, _TIME),
                before,
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (_trade(Side.BUY, "0.59", "1"), before, ShadowQuoteAction.CANCEL, None),
        ]
    )
    result = ConservativeFillSimulator().simulate(analysis)

    assert result.steps[1].active_bid is not None
    assert result.steps[1].active_bid.queue_ahead == NonNegativeQuantity("4")
    assert result.steps[2].active_bid is not None
    assert result.steps[2].active_bid.queue_ahead == NonNegativeQuantity("4")
    assert result.steps[3].active_bid is not None
    assert result.steps[3].active_bid.queue_ahead == NonNegativeQuantity("9")
    assert result.fills[0].order_side is Side.SELL
    assert result.fills[0].trigger is ConservativeFillTrigger.TRADE_THROUGH
    assert result.fills[0].fill_quantity == Quantity("5")
    assert result.fills[1].order_side is Side.BUY
    assert result.fills[1].trigger is ConservativeFillTrigger.TRADE_THROUGH
    assert result.steps[5].active_bid is not None
    assert result.steps[5].active_bid.queue_ahead == NonNegativeQuantity("9")
    assert result.steps[5].active_ask is not None
    assert result.steps[5].active_ask.queue_ahead == NonNegativeQuantity("2")
    assert result.steps[-1].active_bid is None
    assert result.steps[-1].active_ask is None
    assert result.sell_filled_quantity == Decimal("5")
    assert result.buy_filled_quantity == Decimal("5")
    assert result.net_cash_flow == Decimal("1.00")
    assert result.fully_filled_buy_orders == 1
    assert result.fully_filled_sell_orders == 1


def test_nonqualifying_and_other_book_changes_do_not_fill_or_advance_queue() -> None:
    """Wrong aggressors, wrong prices, other levels, and sides are no evidence."""
    book = _book((("0.40", "2"), ("0.30", "1")), (("0.60", "2"),))
    changed = _book((("0.40", "2"), ("0.30", "9")), (("0.60", "2"),))
    analysis = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, book, _TIME),
                book,
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (_trade(Side.BUY, "0.40", "9"), book, ShadowQuoteAction.HOLD, _QUOTE),
            (_trade(Side.SELL, "0.41", "9"), book, ShadowQuoteAction.HOLD, _QUOTE),
            (_trade(Side.SELL, "0.40", "1"), book, ShadowQuoteAction.HOLD, _QUOTE),
            (
                PriceLevelChanged(
                    _MARKET,
                    _TOKEN,
                    Side.BUY,
                    Price("0.30"),
                    NonNegativeQuantity("9"),
                    _TIME,
                ),
                changed,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
            (
                PriceLevelChanged(
                    _MARKET,
                    _TOKEN,
                    Side.SELL,
                    Price("0.40"),
                    NonNegativeQuantity("5"),
                    _TIME,
                ),
                changed,
                ShadowQuoteAction.HOLD,
                _QUOTE,
            ),
        ]
    )
    result = ConservativeFillSimulator().simulate(analysis)
    assert result.fills == ()
    assert result.qualifying_trade_events == 1
    assert result.steps[-1].active_bid is not None
    assert result.steps[-1].active_bid.queue_ahead == NonNegativeQuantity("1")


def test_public_models_and_integrity_errors_reject_invalid_state() -> None:
    """Keep typed fill evidence immutable and reject impossible lifecycle inputs."""
    valid = SimulatedMakerFill(
        _SESSION,
        1,
        _TIME,
        Side.BUY,
        Price("0.4"),
        Quantity("1"),
        Side.SELL,
        Price("0.4"),
        Quantity("2"),
        ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
        NonNegativeQuantity("1"),
        NonNegativeQuantity("0"),
        NonNegativeQuantity("0"),
    )
    assert valid.queue_ahead_after == NonNegativeQuantity("0")
    invalids = (
        (
            cast(MarketDataSessionId, "x"),
            1,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            0,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            1,
            datetime(2026, 8, 2),
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            1,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.BUY,
            Price("0.4"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            1,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("0"),
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            1,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.5"),
            Quantity("2"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
        (
            _SESSION,
            1,
            _TIME,
            Side.SELL,
            Price("0.6"),
            Quantity("1"),
            Side.BUY,
            Price("0.5"),
            Quantity("2"),
            ConservativeFillTrigger.TRADE_THROUGH,
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        ),
    )
    for values in invalids:
        with pytest.raises(ValueError):
            SimulatedMakerFill(*values)

    bad = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, _book(), _TIME),
                _book(),
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (
                BookSnapshotReceived(_MARKET, _TOKEN, _book(), _TIME),
                _book(),
                ShadowQuoteAction.NONE,
                None,
            ),
        ]
    )
    with pytest.raises(ConservativeFillIntegrityError, match="NONE"):
        ConservativeFillSimulator().simulate(bad)
    with pytest.raises(ValueError, match="steps"):
        ConservativeFillAnalysis(bad, (), (), 0, 0, 0)
    with pytest.raises(ValueError, match="fills"):
        ConservativeFillSimulationStep(
            bad.decisions[0], cast(tuple[SimulatedMakerFill, ...], []), None, None
        )


def test_private_state_guards_cover_unrepresentable_simulation_inputs() -> None:
    """Fail closed when callers manually construct states normal flow cannot create."""
    bad = _analysis(
        [
            (
                BookSnapshotReceived(_MARKET, _TOKEN, _book(), _TIME),
                _book(),
                ShadowQuoteAction.PLACE,
                _QUOTE,
            ),
            (
                BookSnapshotReceived(_MARKET, _TOKEN, _book(), _TIME),
                _book(),
                ShadowQuoteAction.NONE,
                None,
            ),
        ]
    )
    with pytest.raises(ValueError, match="sides"):
        SimulatedMakerFill(
            _SESSION,
            1,
            _TIME,
            cast(Side, "BUY"),
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("1"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        )
    with pytest.raises(ValueError, match="values"):
        SimulatedMakerFill(
            _SESSION,
            1,
            _TIME,
            Side.BUY,
            cast(Price, "0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("1"),
            ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        )
    with pytest.raises(ValueError, match="trigger"):
        SimulatedMakerFill(
            _SESSION,
            1,
            _TIME,
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            Side.SELL,
            Price("0.4"),
            Quantity("1"),
            cast(ConservativeFillTrigger, "exact"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
            NonNegativeQuantity("0"),
        )
    for values in (
        (
            cast(Side, "BUY"),
            Price("0.4"),
            Quantity("1"),
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
        ),
        (
            Side.BUY,
            Price("0.4"),
            cast(Quantity, "1"),
            NonNegativeQuantity("1"),
            NonNegativeQuantity("0"),
        ),
        (
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            cast(NonNegativeQuantity, "1"),
            NonNegativeQuantity("0"),
        ),
        (
            Side.BUY,
            Price("0.4"),
            Quantity("1"),
            NonNegativeQuantity("2"),
            NonNegativeQuantity("0"),
        ),
    ):
        with pytest.raises(ValueError):
            SimulatedMakerOrder(*values)
    bad_view = SimulatedMakerOrderView(
        Side.SELL,
        Price("0.6"),
        Quantity("1"),
        NonNegativeQuantity("1"),
        NonNegativeQuantity("0"),
    )
    with pytest.raises(ValueError, match="bid"):
        ConservativeFillSimulationStep(bad.decisions[0], (), bad_view, None)
    with pytest.raises(ValueError, match="ask"):
        ConservativeFillSimulationStep(
            bad.decisions[0],
            (),
            None,
            SimulatedMakerOrderView.from_order(
                SimulatedMakerOrder(
                    Side.BUY,
                    Price("0.4"),
                    Quantity("1"),
                    NonNegativeQuantity("1"),
                    NonNegativeQuantity("0"),
                )
            ),
        )
    step = ConservativeFillSimulationStep(bad.decisions[0], (), None, None)
    with pytest.raises(ValueError, match="fills must reconcile"):
        ConservativeFillAnalysis(
            bad,
            (step, step),
            (
                SimulatedMakerFill(
                    _SESSION,
                    1,
                    _TIME,
                    Side.BUY,
                    Price("0.4"),
                    Quantity("1"),
                    Side.SELL,
                    Price("0.4"),
                    Quantity("1"),
                    ConservativeFillTrigger.EXACT_PRICE_AFTER_QUEUE,
                    NonNegativeQuantity("0"),
                    NonNegativeQuantity("0"),
                    NonNegativeQuantity("0"),
                ),
            ),
            0,
            0,
            0,
        )
    with pytest.raises(ValueError, match="counts"):
        ConservativeFillAnalysis(bad, (step, step), (), -1, 0, 0)

    class _FakeFill:
        order_side = object()

    fake_fill = cast(SimulatedMakerFill, _FakeFill())
    fake_step = ConservativeFillSimulationStep(
        bad.decisions[0], cast(tuple[SimulatedMakerFill, ...], (fake_fill,)), None, None
    )
    with pytest.raises(ValueError, match="fill-side"):
        ConservativeFillAnalysis(
            bad,
            (fake_step, step),
            cast(tuple[SimulatedMakerFill, ...], (fake_fill,)),
            0,
            0,
            0,
        )
    empty = _analysis([])
    empty_result = ConservativeFillAnalysis(empty, (), (), 0, 0, 0)
    assert empty_result.remaining_bid_quantity == Decimal("0")
    assert ConservativeFillSimulator._raise_to_visible(None, None) is None
    assert ConservativeFillSimulator._visible_quantity(
        None, Side.BUY, Price("0.4")
    ) == Decimal("0")
    with pytest.raises(ConservativeFillIntegrityError, match="PLACE"):
        ConservativeFillSimulator()._apply_lifecycle(
            bad.decisions[0],
            SimulatedMakerOrder(
                Side.BUY,
                Price("0.4"),
                Quantity("1"),
                NonNegativeQuantity("1"),
                NonNegativeQuantity("0"),
            ),
            None,
        )
    hold_without_quote = ShadowQuoteDecision(
        bad.decisions[0].replay_step,
        None,
        None,
        ShadowQuoteAction.HOLD,
        ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
    )
    with pytest.raises(ConservativeFillIntegrityError, match="HOLD"):
        ConservativeFillSimulator()._apply_lifecycle(hold_without_quote, None, None)
    cancel_with_quote = ShadowQuoteDecision(
        bad.decisions[0].replay_step, None, _QUOTE, ShadowQuoteAction.CANCEL, None
    )
    with pytest.raises(ConservativeFillIntegrityError, match="CANCEL"):
        ConservativeFillSimulator()._apply_lifecycle(cancel_with_quote, None, None)
    with pytest.raises(ConservativeFillIntegrityError, match="quote action"):
        ConservativeFillSimulator()._new_quote(hold_without_quote, None)
    with pytest.raises(ConservativeFillIntegrityError, match="does not match"):
        ConservativeFillSimulator._validate_decision(bad, bad.decisions[1], 0)
    assert ConservativeFillSimulator()._apply_lifecycle(
        bad.decisions[1], None, None
    ) == (
        None,
        None,
    )
