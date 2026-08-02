# Horus Engine

Horus Engine is a research-oriented prediction-market market-making system.
Live trading is disabled by default.

## Development status

Horus Engine models exchange-independent orders, immutable order-book
snapshots, application-level exchange contracts, and recorded-session replay.
It can retrieve and
normalize public Polymarket market metadata through the read-only Gamma API,
retrieve public CLOB order-book snapshots, stream public CLOB order-book and
trade data into exchange-independent application events, and reconstruct one
local outcome-token order book from those normalized events. The reconstruction
detects stale and crossed state and requires safe resynchronization with an
authoritative snapshot.

It cannot reconnect automatically, authenticate, submit orders, cancel orders,
or trade. Live trading remains unavailable.

## Collect public market data

Record one selected Polymarket outcome token to a local SQLite journal:

```bash
uv run horus collect \
  --market-id "<condition-id>" \
  --outcome yes \
  --database data/horus.db \
  --duration-seconds 300
```

`market-id` is the Polymarket condition ID. The selected `yes` or `no` outcome
determines which normalized token stream is recorded. The command resolves
public, unauthenticated market metadata, then stores normalized events in
SQLite. Reaching the requested duration is successful; Ctrl+C attempts safe
journal finalization before the process exits.

The collector cannot reconnect automatically, records one token per run, and
does not simulate fills, authenticate, submit or cancel orders, or trade.

## Analyze recorded shadow quotes

Evaluate a fixed, hypothetical two-sided maker quote over one already recorded
session:

```bash
uv run horus shadow \
  --database data/horus.db \
  --session-id "<recorded-session-id>" \
  --decisions-output data/shadow-decisions.jsonl
```

`shadow` opens an existing journal, deterministically replays its recorded
events, and emits one compact JSON summary to stdout. It reports total,
quoteable, and suppressed states; a Decimal quoteable ratio; and `place`,
`hold`, `replace`, `cancel`, and inactive `none` action counts. A desired maker
quote is only valid inside the strict price bounds `0 < bid < ask < 1`.

The default model is `microprice`, with a two-tick half spread and quantity 10.
`midpoint` uses the best bid and ask; `microprice` uses only their displayed
quantities as a simple top-of-book imbalance estimate, not a proven edge. Pass
`--decisions-output <path>` to write detailed deterministic JSONL records with
stable suppression reasons, fair values, desired prices, and quantities.

The command does not initialize or alter the journal, submit orders, simulate
fills, estimate queue position, track inventory, calculate fees, markouts, or
P&L, or trade.

## SQLite market-data journal

Horus Engine can initialize a versioned SQLite market-data journal, record
market-data session metadata, persist normalized market-data events and complete
authoritative snapshots, and read sessions and events back without Decimal
loss. The journal is deliberately not connected automatically to
`MarketDataSession`; a future integration runner will own that lifecycle.

Supply and retain ownership of the `aiosqlite` connection:

```python
import aiosqlite

from horus_engine.infrastructure.sqlite import SQLiteMarketDataJournal


async with aiosqlite.connect("horus.db") as connection:
    journal = SQLiteMarketDataJournal(connection)
    await journal.initialize()
```

The journal records normalized snapshots, level changes, trades, tick-size
changes, and connection lifecycle events in arrival sequence. It does not store
raw venue payloads, credentials, or any trading capability. `MarketDataSessionReplayer`
can load one recorded session, replay its normalized events deterministically,
reconstruct every post-event local-book state, verify recorded metadata, and
detect sequence gaps or internally inconsistent history. A read-only
shadow-quote experiment can calculate midpoint or top-level microprice fair
values from synchronized, strictly two-sided books, then track hypothetical
maker-quote lifecycle decisions over that reconstructed history.

Replay is intentionally empirical: it has no fill simulation, queue-position
estimation, inventory, fees, markouts, P&L, authentication, execution, or
trading.

## Public Polymarket catalog

The Polymarket market catalog is intentionally read-only. Supply and retain
ownership of an `httpx.AsyncClient`; the adapter does not close it:

```python
import httpx

from horus_engine.infrastructure.polymarket import PolymarketMarketCatalogGateway


async with httpx.AsyncClient() as client:
    catalog = PolymarketMarketCatalogGateway(client)
    markets = await catalog.list_markets()
```

This adapter only retrieves public metadata. It includes no authentication,
order-book streaming, order submission, cancellation, or live-trading support.

## Public Polymarket order-book snapshots

The CLOB snapshot adapter retrieves one public `/book` response for a supplied
token and validates that the returned market and token identifiers match the
request. Supply and retain ownership of an `httpx.AsyncClient`; the adapter
does not close it:

```python
import httpx

from horus_engine.application import MarketId, TokenId
from horus_engine.infrastructure.polymarket import PolymarketOrderBookSnapshotGateway


async with httpx.AsyncClient() as client:
    snapshots = PolymarketOrderBookSnapshotGateway(client)
    book = await snapshots.get_order_book(
        MarketId("condition-id"), TokenId("outcome-token-id")
    )
```

The returned `OrderBook` is exchange-independent and immutable. This adapter
does not stream data, authenticate, submit or cancel orders, or trade.

## Public Polymarket market-data stream

The WebSocket adapter streams unauthenticated public market data from
`wss://ws-subscriptions-clob.polymarket.com/ws/market`. It subscribes once to
the requested outcome token IDs and normalizes `book`, `price_change`,
`tick_size_change`, and `last_trade_price` messages into application events.
It sends the required text `PING` heartbeat every 10 seconds by default; `PONG`
does not produce an event.

```python
from horus_engine.application import MarketId, TokenId
from horus_engine.infrastructure.polymarket import PolymarketMarketDataStreamGateway

stream = PolymarketMarketDataStreamGateway()
async for event in stream.stream_market_data(
    MarketId("condition-id"), (TokenId("yes-token"), TokenId("no-token"))
):
    print(event)
```

This is a single, read-only connection lifecycle. It has no authentication,
order management, or trading API; it does not reconstruct a mutable local book
and does not automatically reconnect.

## Local order-book reconstruction

`LocalOrderBookState` is an application-layer state machine for exactly one
market and outcome token. Apply a normalized authoritative snapshot before
applying price-level updates:

```python
from horus_engine.application import LocalOrderBookState, MarketId, TokenId
from horus_engine.domain import TickSize

state = LocalOrderBookState(
    MarketId("condition-id"), TokenId("outcome-token-id"), TickSize("0.01")
)

# Apply BookSnapshotReceived, PriceLevelChanged, TickSizeChanged, or connection
# events from application-level adapters. Every call returns an immutable view.
view = state.apply(snapshot_event)
```

The state machine retains immutable observed books for diagnostics. A transport
disconnection, reconnection, or tick-size change makes the state stale; a
crossed level-update result is retained but marked invalid. Neither condition
accepts more incremental updates: only a valid authoritative snapshot restores
synchronization. It has no network calls, startup orchestration, reconnection
loop, persistence, strategy, authentication, execution, or trading behavior.

## Local setup

Horus Engine requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv python install 3.12
make install
```

Run the full local verification suite with:

```bash
make check
```

Individual checks are also available:

```bash
make format
make format-check
make lint
make typecheck
make test
```
