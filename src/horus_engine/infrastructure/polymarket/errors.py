"""Errors raised while translating Polymarket Gamma responses."""


class PolymarketInfrastructureError(RuntimeError):
    """Base error for Polymarket infrastructure operations."""


class PolymarketHttpError(PolymarketInfrastructureError):
    """Raised when Gamma cannot be reached or returns an HTTP error."""


class PolymarketPayloadError(PolymarketInfrastructureError):
    """Raised when a Gamma response is malformed or internally inconsistent."""
