"""Evidence-limited, fail-closed session eligibility; no strategy results used."""

import asyncio
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal

from .errors import MarketDataReplayError
from .events import BookSnapshotReceived, TradeObserved
from .market_data_journal import (
    MarketDataJournalReader,
    MarketDataSessionId,
    PersistedMarketDataSession,
    PersistedMarketDataUpdate,
)
from .market_data_replay import MarketDataSessionReplayer
from .order_book_state import LocalBookStatus

_MIN_UPDATES = 2
_MIN_SNAPSHOTS = 1
_MIN_SYNCHRONIZED_FRACTION = Decimal("0.5")
_POLICY = {
    "id": "horus-session-quality-v1",
    "minimum_updates": _MIN_UPDATES,
    "minimum_snapshots": _MIN_SNAPSHOTS,
    "minimum_synchronized_fraction": str(_MIN_SYNCHRONIZED_FRACTION),
    "maximum_invalid_observations": 0,
    "maximum_nonterminal_stale_observations": 0,
    "maximum_timestamp_regressions": 0,
    "maximum_source_timestamps_outside_capture_bounds": 0,
    "requires_finished_and_verified_replay": True,
}


def unavailable_quality_report(
    session_id: MarketDataSessionId, reason: str
) -> dict[str, object]:
    """Report unreadable evidence as unavailable, never as measured zero."""
    return {
        "session_id": session_id.value,
        "policy": dict(_POLICY),
        "eligible": False,
        "reasons": [reason],
        "measurements": None,
        "sequence_consistent": None,
        "replay_integrity": "unavailable",
        "terminal_complete": None,
        "unavailable_measurements": {
            "synchronized_wall_clock_uptime": "no per-event local receipt clock",
            "message_loss_rate": "no venue sequence or completeness guarantee",
            "receipt_latency": "no comparable source and local receipt clocks",
        },
    }


@dataclass(frozen=True)
class _SnapshotReader:
    session: PersistedMarketDataSession
    updates: tuple[PersistedMarketDataUpdate, ...]

    async def get_session(
        self, session_id: MarketDataSessionId
    ) -> PersistedMarketDataSession:
        return self.session

    async def list_updates(
        self, session_id: MarketDataSessionId
    ) -> tuple[PersistedMarketDataUpdate, ...]:
        return self.updates


async def assess_session_quality(
    journal: MarketDataJournalReader, session_id: MarketDataSessionId
) -> dict[str, object]:
    """Assess a single frozen read of the evidence against the versioned policy.

    Status periods are contiguous post-event sequence runs, not uptime. Raw
    post-state measurements remain explicitly unverified if replay fails.
    """
    try:
        session = await journal.get_session(session_id)
        if session is None:
            return unavailable_quality_report(session_id, "session_not_found")
        updates = await journal.list_updates(session_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        return unavailable_quality_report(session_id, "journal_read_failed")

    report = unavailable_quality_report(session_id, "not_assessed")
    reasons: list[str] = []
    count = len(updates)
    statuses = Counter(update.post_status.name for update in updates)
    events = Counter(type(update.event).__name__ for update in updates)
    times = [update.event.observed_at for update in updates]
    regressions = sum(later < earlier for earlier, later in zip(times, times[1:]))
    equal_times = sum(later == earlier for earlier, later in zip(times, times[1:]))
    before_start = sum(stamp < session.started_at for stamp in times)
    after_finish = (
        None
        if session.finished_at is None
        else sum(stamp > session.finished_at for stamp in times)
    )
    synchronized = statuses[LocalBookStatus.SYNCHRONIZED.name]
    fraction = Decimal(synchronized) / count if count else None
    nonterminal_stale = sum(
        update.post_status is LocalBookStatus.STALE for update in updates[:-1]
    )
    periods: list[dict[str, object]] = []
    for update in updates:
        status = update.post_status.name
        stamp = update.event.observed_at.isoformat()
        if not periods or periods[-1]["status"] != status:
            periods.append(
                {
                    "status": status,
                    "first_sequence": update.sequence_number,
                    "last_sequence": update.sequence_number,
                    "first_event_timestamp": stamp,
                    "last_event_timestamp": stamp,
                }
            )
        else:
            periods[-1]["last_sequence"] = update.sequence_number
            periods[-1]["last_event_timestamp"] = stamp

    sequence_ok = session.last_sequence_number == count and [
        update.sequence_number for update in updates
    ] == list(range(1, count + 1))
    report["sequence_consistent"] = sequence_ok
    report["measurements"] = {
        "updates": count,
        "snapshots": sum(
            isinstance(update.event, BookSnapshotReceived) for update in updates
        ),
        "trades": sum(isinstance(update.event, TradeObserved) for update in updates),
        "event_counts": dict(sorted(events.items())),
        "post_status_counts": {
            status.name: statuses[status.name] for status in LocalBookStatus
        },
        "synchronized_observation_fraction": None
        if fraction is None
        else str(fraction),
        "post_status_periods": periods,
        "timestamp_regressions": regressions,
        "equal_adjacent_timestamps": equal_times,
        "source_timestamps_before_capture_start": before_start,
        "source_timestamps_after_capture_finish": after_finish,
        "nonterminal_stale_observations": nonterminal_stale,
        "terminal_record_present": session.finished_at is not None,
        "recorded_final_status": None
        if session.final_status is None
        else session.final_status.name,
        "status_basis": (
            "recorded_post_event_metadata; verified only if replay_integrity=verified"
        ),
    }
    if session.session_id != session_id:
        reasons.append("session_identity_mismatch")
    if not sequence_ok:
        reasons.append("sequence_inconsistent")
    try:
        replay = await MarketDataSessionReplayer(
            _SnapshotReader(session, updates)
        ).replay(session_id)
        report["replay_integrity"] = "verified"
        report["terminal_complete"] = replay.is_finished
    except asyncio.CancelledError:
        raise
    except MarketDataReplayError:
        report["replay_integrity"] = "failed"
        reasons.append("replay_integrity_failed")
    except Exception:
        reasons.append("replay_verification_unavailable")

    checks = (
        (count < _MIN_UPDATES, "insufficient_updates"),
        (events["BookSnapshotReceived"] < _MIN_SNAPSHOTS, "missing_snapshot"),
        (
            fraction is None or fraction < _MIN_SYNCHRONIZED_FRACTION,
            "insufficient_synchronized_fraction",
        ),
        (statuses[LocalBookStatus.INVALID.name] > 0, "invalid_observations"),
        (nonterminal_stale > 0, "nonterminal_stale_observations"),
        (regressions > 0, "timestamp_regressions"),
        (
            before_start > 0 or (after_finish is not None and after_finish > 0),
            "source_timestamp_outside_capture_bounds",
        ),
        (report["terminal_complete"] is not True, "terminal_completeness_unverified"),
    )
    reasons.extend(reason for failed, reason in checks if failed)
    report["reasons"] = reasons
    report["eligible"] = not reasons
    return report
