"""Private schema definitions for the SQLite market-data journal."""

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE market_data_sessions (
    session_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    initial_tick_size TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    final_status TEXT,
    final_tick_size TEXT,
    final_last_observed_at TEXT,
    final_reason TEXT,
    last_sequence_number INTEGER NOT NULL DEFAULT 0
        CHECK (last_sequence_number >= 0)
);

CREATE TABLE market_data_events (
    event_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES market_data_sessions(session_id),
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_type TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    book_changed INTEGER NOT NULL CHECK (book_changed IN (0, 1)),
    post_status TEXT NOT NULL,
    post_tick_size TEXT NOT NULL,
    post_last_observed_at TEXT,
    post_status_reason TEXT,
    UNIQUE (session_id, sequence_number)
);

CREATE TABLE book_snapshots (
    event_id INTEGER PRIMARY KEY REFERENCES market_data_events(event_id)
);

CREATE TABLE book_snapshot_levels (
    event_id INTEGER NOT NULL REFERENCES book_snapshots(event_id),
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    level_position INTEGER NOT NULL CHECK (level_position >= 0),
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    PRIMARY KEY (event_id, side, level_position),
    UNIQUE (event_id, side, price)
);

CREATE TABLE price_level_changes (
    event_id INTEGER PRIMARY KEY REFERENCES market_data_events(event_id),
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price TEXT NOT NULL,
    quantity TEXT NOT NULL
);

CREATE TABLE trades (
    event_id INTEGER PRIMARY KEY REFERENCES market_data_events(event_id),
    aggressor_side TEXT NOT NULL CHECK (aggressor_side IN ('BUY', 'SELL')),
    price TEXT NOT NULL,
    quantity TEXT NOT NULL
);

CREATE TABLE tick_size_changes (
    event_id INTEGER PRIMARY KEY REFERENCES market_data_events(event_id),
    old_tick_size TEXT NOT NULL,
    new_tick_size TEXT NOT NULL
);

CREATE TABLE connection_events (
    event_id INTEGER PRIMARY KEY REFERENCES market_data_events(event_id),
    reason TEXT
);
"""
