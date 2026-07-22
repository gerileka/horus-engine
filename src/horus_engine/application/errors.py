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
