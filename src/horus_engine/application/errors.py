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
