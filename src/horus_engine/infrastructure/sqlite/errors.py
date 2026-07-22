"""Focused errors raised by the SQLite market-data journal adapter."""


class SQLiteMarketDataJournalError(RuntimeError):
    """Base error for SQLite market-data journal operations."""


class SQLiteSchemaVersionError(SQLiteMarketDataJournalError):
    """Raised when the supplied database has an unsupported schema version."""


class SQLiteSessionAlreadyExists(SQLiteMarketDataJournalError):
    """Raised when a caller attempts to start an existing session identifier."""


class SQLiteSessionNotFound(SQLiteMarketDataJournalError):
    """Raised when a requested journal session is unknown."""


class SQLiteSessionAlreadyFinished(SQLiteMarketDataJournalError):
    """Raised when an update or finish targets a completed session."""


class SQLiteSequenceError(SQLiteMarketDataJournalError):
    """Raised when an update does not use the session's next arrival sequence."""


class SQLiteJournalPayloadError(SQLiteMarketDataJournalError):
    """Raised when stored or supplied journal data is malformed or inconsistent."""
