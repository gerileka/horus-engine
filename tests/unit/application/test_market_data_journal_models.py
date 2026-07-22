"""Tests for exchange-neutral market-data journal application contracts."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from horus_engine.application import (
    BookSnapshotReceived,
    InvalidMarketDataJournal,
    InvalidMarketDataSessionId,
    LocalBookStatus,
    MarketDataJournalSession,
    MarketDataSessionId,
    MarketId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
    TokenId,
)
from horus_engine.domain import OrderBook, TickSize


def test_market_data_session_identifier_is_distinct_immutable_and_hashable() -> None:
    """Keep session identity explicit instead of conflating it with market identity."""
    identifier = MarketDataSessionId(" session-1 ")
    assert identifier.value == " session-1 "
    assert str(identifier) == " session-1 "
    assert identifier == MarketDataSessionId(" session-1 ")
    assert hash(identifier) == hash(MarketDataSessionId(" session-1 "))
    assert identifier != cast(object, MarketId(" session-1 "))
    with pytest.raises(FrozenInstanceError):
        identifier.value = "changed"  # type: ignore[misc]
    for value in ("", "   ", "session\x00"):
        with pytest.raises(InvalidMarketDataSessionId):
            MarketDataSessionId(value)


def test_journal_models_require_aware_consistent_metadata() -> None:
    """Require immutable, timezone-aware metadata without database row concepts."""
    started_at = datetime(2026, 7, 22, 12, tzinfo=UTC)
    session = MarketDataJournalSession(
        MarketDataSessionId("session-1"),
        MarketId("market-1"),
        TokenId("token-1"),
        TickSize("0.01"),
        started_at,
    )
    persisted = PersistedMarketDataSession(
        session.session_id,
        session.market_id,
        session.token_id,
        session.initial_tick_size,
        started_at,
        None,
        None,
        None,
        None,
        None,
        0,
    )
    event = BookSnapshotReceived(
        session.market_id, session.token_id, OrderBook(), started_at
    )
    update = PersistedMarketDataUpdate(
        session.session_id,
        1,
        event,
        True,
        LocalBookStatus.SYNCHRONIZED,
        TickSize("0.01"),
        None,
        None,
    )
    assert persisted.last_sequence_number == 0
    assert update.event == event
    with pytest.raises(FrozenInstanceError):
        persisted.last_sequence_number = 1  # type: ignore[misc]
    with pytest.raises(InvalidMarketDataJournal):
        MarketDataJournalSession(
            session.session_id,
            session.market_id,
            session.token_id,
            session.initial_tick_size,
            datetime(2026, 7, 22, 12),
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataSession(
            session.session_id,
            session.market_id,
            session.token_id,
            session.initial_tick_size,
            started_at,
            started_at,
            None,
            None,
            None,
            None,
            0,
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataSession(
            session.session_id,
            session.market_id,
            session.token_id,
            session.initial_tick_size,
            started_at,
            None,
            LocalBookStatus.SYNCHRONIZED,
            None,
            None,
            None,
            0,
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataSession(
            session.session_id,
            session.market_id,
            session.token_id,
            session.initial_tick_size,
            started_at,
            None,
            LocalBookStatus.SYNCHRONIZED,
            TickSize("0.01"),
            None,
            None,
            -1,
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataSession(
            session.session_id,
            session.market_id,
            session.token_id,
            session.initial_tick_size,
            started_at,
            started_at - timedelta(microseconds=1),
            LocalBookStatus.SYNCHRONIZED,
            TickSize("0.01"),
            None,
            None,
            0,
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataUpdate(
            session.session_id,
            0,
            event,
            True,
            LocalBookStatus.SYNCHRONIZED,
            TickSize("0.01"),
            None,
            None,
        )
    with pytest.raises(InvalidMarketDataJournal):
        PersistedMarketDataUpdate(
            session.session_id,
            1,
            event,
            1,  # type: ignore[arg-type]
            LocalBookStatus.SYNCHRONIZED,
            TickSize("0.01"),
            None,
            None,
        )
