"""Tests for fair-value estimates and recorded-session shadow quote analysis."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from horus_engine.application import (
    LocalBookStatus,
    LocalOrderBookView,
    MarketDataReplayResult,
    MarketDataReplayStep,
    MarketDataSessionId,
    MarketId,
    MidpointFairValueModel,
    PersistedMarketDataSession,
    ShadowQuote,
    ShadowQuoteAction,
    ShadowQuoteAnalysis,
    ShadowQuoteAnalyzer,
    ShadowQuoteConfig,
    ShadowQuoteDecision,
    ShadowQuoteSuppressionReason,
    TokenId,
    TopLevelMicropriceFairValueModel,
    TradeObserved,
)
from horus_engine.domain import (
    OrderBook,
    OrderBookLevel,
    Price,
    Quantity,
    Side,
    TickSize,
)

_SESSION_ID = MarketDataSessionId("shadow-session")
_MARKET_ID = MarketId("market-1")
_TOKEN_ID = TokenId("token-1")
_TIME = datetime(2026, 8, 2, tzinfo=UTC)


def _book(
    bids: tuple[tuple[str, str], ...] = (("0.40", "2"),),
    asks: tuple[tuple[str, str], ...] = (("0.60", "3"),),
) -> OrderBook:
    """Build an immutable order book with compact Decimal string inputs."""
    return OrderBook(
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in bids),
        (OrderBookLevel(Price(price), Quantity(quantity)) for price, quantity in asks),
    )


def _view(
    book: OrderBook | None = None,
    *,
    status: LocalBookStatus = LocalBookStatus.SYNCHRONIZED,
    tick: str = "0.01",
) -> LocalOrderBookView:
    """Build a minimal replay view for deterministic analyzer tests."""
    return LocalOrderBookView(
        _MARKET_ID, _TOKEN_ID, TickSize(tick), status, book or _book(), _TIME, None
    )


def _step(sequence_number: int, view: LocalOrderBookView) -> MarketDataReplayStep:
    """Build an immutable replay step with an otherwise irrelevant trade event."""
    return MarketDataReplayStep(
        _SESSION_ID,
        sequence_number,
        TradeObserved(
            _MARKET_ID, _TOKEN_ID, Side.BUY, Price("0.5"), Quantity("1"), _TIME
        ),
        view,
        False,
    )


def _replay(*views: LocalOrderBookView) -> MarketDataReplayResult:
    """Build a minimal coherent replay result from supplied post-event views."""
    initial = _view()
    session = PersistedMarketDataSession(
        _SESSION_ID,
        _MARKET_ID,
        _TOKEN_ID,
        TickSize("0.01"),
        _TIME,
        None,
        None,
        None,
        None,
        None,
        len(views),
    )
    steps = tuple(_step(index, view) for index, view in enumerate(views, start=1))
    final = views[-1] if views else initial
    return MarketDataReplayResult(session, steps, final, final, False)


def test_fair_value_models_use_exact_top_of_book_decimal_arithmetic() -> None:
    """Compute the requested midpoint and best-level imbalance estimates only."""
    book = _book((("0.20", "3"), ("0.10", "8")), (("0.80", "1"),))
    midpoint = MidpointFairValueModel()
    microprice = TopLevelMicropriceFairValueModel()

    assert midpoint.name == "midpoint"
    assert midpoint.estimate(book) == Price("0.50")
    assert microprice.name == "microprice"
    assert microprice.estimate(book) == Price("0.65")


@pytest.mark.parametrize(
    "book",
    (
        _book((), ()),
        _book((("0.20", "1"),), ()),
        _book((), (("0.80", "1"),)),
        _book((("0.50", "1"),), (("0.50", "1"),)),
        _book((("0.80", "1"),), (("0.20", "1"),)),
    ),
)
def test_fair_value_models_reject_non_quoteable_books(book: OrderBook) -> None:
    """Do not assign fair value to missing, locked, or crossed top-of-book data."""
    assert MidpointFairValueModel().estimate(book) is None
    assert TopLevelMicropriceFairValueModel().estimate(book) is None


def test_shadow_configuration_and_quote_models_are_immutable_and_validated() -> None:
    """Preserve exact domain values and reject malformed public model inputs."""
    config = ShadowQuoteConfig(2, Quantity("1.5"))
    assert config.half_spread_ticks == 2
    with pytest.raises(FrozenInstanceError):
        config.quantity = Quantity("2")  # type: ignore[misc]
    for invalid in (0, -1, True, "1"):
        with pytest.raises(ValueError, match="half spread"):
            ShadowQuoteConfig(cast(int, invalid), Quantity("1"))
    with pytest.raises(ValueError, match="quantity"):
        ShadowQuoteConfig(1, cast(Quantity, "1"))
    with pytest.raises(ValueError, match="prices"):
        ShadowQuote(cast(Price, "0.4"), Price("0.6"), Quantity("1"))
    with pytest.raises(ValueError, match="quantity"):
        ShadowQuote(Price("0.4"), Price("0.6"), cast(Quantity, "1"))
    with pytest.raises(ValueError, match="strictly"):
        ShadowQuote(Price("0.6"), Price("0.4"), Quantity("1"))
    with pytest.raises(ValueError, match="strictly"):
        ShadowQuote(Price("0"), Price("0.6"), Quantity("1"))
    with pytest.raises(ValueError, match="strictly"):
        ShadowQuote(Price("0.4"), Price("1"), Quantity("1"))


def test_shadow_analysis_tracks_place_hold_replace_cancel_and_quoteability() -> None:
    """Evaluate every recorded state, retaining deterministic desired quote details."""
    first = _view(_book((("0.40", "2"),), (("0.60", "3"),)))
    replacement = _view(_book((("0.45", "2"),), (("0.65", "3"),)))
    stale = _view(status=LocalBookStatus.STALE)
    analysis = ShadowQuoteAnalyzer(
        MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("2"))
    ).analyze(_replay(first, first, replacement, stale, stale))

    assert [decision.action for decision in analysis.decisions] == [
        ShadowQuoteAction.PLACE,
        ShadowQuoteAction.HOLD,
        ShadowQuoteAction.REPLACE,
        ShadowQuoteAction.CANCEL,
        ShadowQuoteAction.NONE,
    ]
    assert analysis.decisions[0].desired_quote == ShadowQuote(
        Price("0.49"), Price("0.51"), Quantity("2")
    )
    assert (
        analysis.decisions[3].suppression_reason
        is ShadowQuoteSuppressionReason.BOOK_NOT_SYNCHRONIZED
    )
    assert analysis.total_steps == 5
    assert analysis.quoteable_steps == 3
    assert analysis.suppressed_steps == 2
    assert analysis.quoteable_ratio == Decimal("0.6")
    assert analysis.placements == 1
    assert analysis.replacements == 1
    assert analysis.holds == 1
    assert analysis.cancellations == 1
    assert analysis.none_actions == 1
    assert (
        analysis.placements
        + analysis.replacements
        + analysis.holds
        + analysis.cancellations
        + analysis.none_actions
        == analysis.total_steps
    )
    assert (
        all(decision.quoteable for decision in analysis.decisions[:3])
        and not analysis.decisions[3].quoteable
    )


def test_shadow_analysis_classifies_each_suppression_reason() -> None:
    """Expose focused stable categories for every non-quoteable replay state."""
    analyzer = ShadowQuoteAnalyzer(
        MidpointFairValueModel(), ShadowQuoteConfig(2, Quantity("1"))
    )
    no_book = _view(None)
    object.__setattr__(no_book, "book", None)
    one_sided = _view(_book((("0.50", "1"),), ()))
    locked = _view(_book((("0.50", "1"),), (("0.50", "1"),)))
    crossed = _view(_book((("0.60", "1"),), (("0.40", "1"),)))
    empty = _view(_book((), ()))
    raw_bid_zero = _view(_book((("0.00", "1"),), (("0.02", "1"),)))
    raw_ask_one = _view(_book((("0.98", "1"),), (("1.00", "1"),)))
    rounded_bid_zero = _view(_book((("0.06", "1"),), (("0.08", "1"),)), tick="0.05")
    rounded_ask_one = _view(_book((("0.92", "1"),), (("0.94", "1"),)), tick="0.05")
    analysis = analyzer.analyze(
        _replay(no_book, one_sided, locked, crossed, empty, raw_bid_zero, raw_ask_one)
    )

    assert [decision.suppression_reason for decision in analysis.decisions] == [
        ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
        ShadowQuoteSuppressionReason.BOOK_ONE_SIDED,
        ShadowQuoteSuppressionReason.BOOK_LOCKED,
        ShadowQuoteSuppressionReason.BOOK_CROSSED,
        ShadowQuoteSuppressionReason.FAIR_VALUE_UNAVAILABLE,
        ShadowQuoteSuppressionReason.PRICE_BOUNDARY,
        ShadowQuoteSuppressionReason.PRICE_BOUNDARY,
    ]
    decision = ShadowQuoteDecision(
        _step(9, no_book),
        None,
        None,
        ShadowQuoteAction.NONE,
        ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
    )
    assert not decision.quoteable
    rounding = ShadowQuoteAnalyzer(
        MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("1"))
    ).analyze(_replay(rounded_bid_zero, rounded_ask_one))
    assert [decision.suppression_reason for decision in rounding.decisions] == [
        ShadowQuoteSuppressionReason.PRICE_BOUNDARY,
        ShadowQuoteSuppressionReason.PRICE_BOUNDARY,
    ]
    tick_rounding = ShadowQuoteAnalyzer(
        MidpointFairValueModel(), ShadowQuoteConfig(1, Quantity("1"))
    ).analyze(_replay(_view(_book((("0.20", "1"),), (("0.80", "1"),)), tick="0.4")))
    assert (
        tick_rounding.decisions[0].suppression_reason
        is ShadowQuoteSuppressionReason.PRICE_BOUNDARY
    )
    assert analyzer._quote_from_fair_value(
        Price("0.50"), TickSize("0.01"), _book((("0.80", "1"),), (("0.90", "1"),))
    ) == (None, ShadowQuoteSuppressionReason.NO_QUOTING_ROOM)


def test_shadow_decision_and_analysis_models_reject_incoherent_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate decision evidence and explicit action-count reconciliation."""
    step = _step(1, _view())
    quote = ShadowQuote(Price("0.49"), Price("0.51"), Quantity("1"))
    with pytest.raises(ValueError, match="action"):
        ShadowQuoteDecision(
            step,
            Price("0.5"),
            quote,
            cast(ShadowQuoteAction, "place"),
            None,
        )
    with pytest.raises(ValueError, match="suppression"):
        ShadowQuoteDecision(step, None, None, ShadowQuoteAction.NONE, None)
    with pytest.raises(ValueError, match="quoteable"):
        ShadowQuoteDecision(
            step,
            Price("0.5"),
            quote,
            ShadowQuoteAction.PLACE,
            ShadowQuoteSuppressionReason.BOOK_UNAVAILABLE,
        )
    decision = ShadowQuoteDecision(
        step, Price("0.5"), quote, ShadowQuoteAction.PLACE, None
    )
    replay = _replay(_view())
    with pytest.raises(ValueError, match="tuple"):
        ShadowQuoteAnalysis(
            replay,
            "midpoint",
            ShadowQuoteConfig(1, Quantity("1")),
            cast(tuple[ShadowQuoteDecision, ...], [decision]),
        )
    with pytest.raises(ValueError, match="match replay"):
        ShadowQuoteAnalysis(replay, "midpoint", ShadowQuoteConfig(1, Quantity("1")), ())
    monkeypatch.setattr(ShadowQuoteAnalysis, "_action_count", lambda _self, _action: 0)
    with pytest.raises(ValueError, match="action counts"):
        ShadowQuoteAnalysis(
            replay, "midpoint", ShadowQuoteConfig(1, Quantity("1")), (decision,)
        )


def test_shadow_analysis_uses_fair_value_unavailable_for_model_suppression() -> None:
    """Retain a stable fallback when a model declines an otherwise usable book."""

    class _UnavailableModel:
        @property
        def name(self) -> str:
            return "unavailable"

        def estimate(self, book: OrderBook) -> Price | None:
            return None

    analysis = ShadowQuoteAnalyzer(
        _UnavailableModel(), ShadowQuoteConfig(1, Quantity("1"))
    ).analyze(_replay(_view()))
    assert (
        analysis.decisions[0].suppression_reason
        is ShadowQuoteSuppressionReason.FAIR_VALUE_UNAVAILABLE
    )
