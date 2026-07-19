"""Exceptions raised by Horus Engine domain primitives."""


class DomainError(ValueError):
    """Base exception for invalid domain operations or values."""


class InvalidPrice(DomainError):
    """Raised when a price is not a valid binary-market price."""


class InvalidQuantity(DomainError):
    """Raised when a quantity is not strictly positive and finite."""


class InvalidMoney(DomainError):
    """Raised when a monetary amount is not finite."""


class InvalidTickSize(DomainError):
    """Raised when a tick size is outside the valid range."""


class TickAlignmentError(DomainError):
    """Raised when a price cannot be represented by a requested tick operation."""
