"""Exceptions raised when application contracts receive invalid data."""


class ApplicationContractError(ValueError):
    """Base exception for invalid application-contract values or events."""


class InvalidMarketId(ApplicationContractError):
    """Raised when a market identifier is invalid."""


class InvalidTokenId(ApplicationContractError):
    """Raised when a tradable outcome token identifier is invalid."""


class InvalidExchangeOrderId(ApplicationContractError):
    """Raised when an exchange-issued order identifier is invalid."""


class InvalidClientOrderId(ApplicationContractError):
    """Raised when a client-generated order identifier is invalid."""


class InvalidMarketDataSessionId(ApplicationContractError):
    """Raised when a recorded market-data session identifier is invalid."""


class InvalidMarketDataJournal(ApplicationContractError):
    """Raised when market-data journal metadata is internally inconsistent."""


class InvalidMarket(ApplicationContractError):
    """Raised when exchange-neutral market metadata is invalid."""


class InvalidEventTimestamp(ApplicationContractError):
    """Raised when an event timestamp is missing timezone information."""


class InvalidEventText(ApplicationContractError):
    """Raised when event text that must be human-readable is blank."""


class InvalidTickSizeChange(ApplicationContractError):
    """Raised when an observed tick-size change does not actually change."""


class LocalOrderBookStateError(ApplicationContractError):
    """Base exception for unsafe local order-book state transitions."""


class MarketDataIdentityMismatch(LocalOrderBookStateError):
    """Raised when an event belongs to a different market or token."""


class OutOfOrderMarketDataEvent(LocalOrderBookStateError):
    """Raised when an event predates the last successfully applied event."""


class SnapshotRequired(LocalOrderBookStateError):
    """Raised when an incremental update needs a fresh authoritative snapshot."""


class TickSizeStateMismatch(LocalOrderBookStateError):
    """Raised when a tick-size event does not match tracked tick state."""


class InvalidLocalOrderBook(LocalOrderBookStateError):
    """Raised when a candidate local book is unsafe to synchronize."""


class MarketDataSessionError(RuntimeError):
    """Base exception for an application market-data session failure."""


class MarketDataSessionAlreadyStarted(MarketDataSessionError):
    """Raised when a single-use market-data session is started more than once."""


class MarketDataBootstrapError(MarketDataSessionError):
    """Raised when a stream cannot safely establish its initial book snapshot."""


class MarketDataSynchronizationLost(MarketDataSessionError):
    """Raised when an applied stream can no longer safely reconstruct its book."""


class InvalidMarketDataRecording(ApplicationContractError):
    """Raised when market-data recording result metadata is inconsistent."""


class MarketDataRecordingError(RuntimeError):
    """Raised when recording a market-data session cannot complete."""


class MarketDataRecorderAlreadyStarted(MarketDataRecordingError):
    """Raised when a single-use market-data recorder runs more than once."""


class MarketDataRecordingClockError(MarketDataRecordingError):
    """Raised when a recorder clock returns an invalid lifecycle timestamp."""


class MarketDataRecordingFinalizationError(MarketDataRecordingError):
    """Raised when an already-started journal session cannot be finalized."""


class MarketDataReplayError(RuntimeError):
    """Base exception for deterministic market-data replay failures."""


class MarketDataReplaySessionNotFound(MarketDataReplayError):
    """Raised when replay is requested for a session absent from the journal."""


class MarketDataReplaySequenceError(MarketDataReplayError):
    """Raised when journal updates do not form an arrival sequence."""


class MarketDataReplayIntegrityError(MarketDataReplayError):
    """Raised when recorded metadata disagrees with reconstructed state."""
