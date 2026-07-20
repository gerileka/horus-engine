"""Errors raised while translating public Polymarket Gamma and CLOB responses."""


class PolymarketInfrastructureError(RuntimeError):
    """Base error for Polymarket infrastructure operations."""


class PolymarketHttpError(PolymarketInfrastructureError):
    """Raised when a public Polymarket endpoint cannot be reached or errors."""


class PolymarketPayloadError(PolymarketInfrastructureError):
    """Raised when a public Polymarket response is malformed or inconsistent."""
