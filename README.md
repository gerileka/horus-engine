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

For opt-in, short public compatibility recordings with private provenance and
explicit access blockers, see the [bounded public API smoke procedure](docs/public-api-smoke.md).
It is separate from normal CI and does not enable trading.

For a network-free, synthetic collect → shadow → fills walkthrough first, see
the [reproducible offline demo](docs/offline-demo.md). It creates a real SQLite
journal, checks hand-calculated outcomes in an integration test, and verifies
repeatable analysis without changing the input journal.

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

For the execution-model audit, hand calculations and quality-gated scenario
comparison, see [fill-assumption sensitivity](docs/fill-sensitivity.md).

For research use, first run the [session-quality eligibility gate](docs/session-quality.md)
and honor its exit status before generating strategy outputs. The standalone
analysis commands below remain exploratory and do not imply quality approval.

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

## Simulate conservative maker fills

Estimate only strongly evidenced hypothetical maker fills from a recorded
session and its shadow-quote decisions:

```bash
uv run horus simulate-fills \
  --database data/horus.db \
  --session-id "<recorded-session-id>" \
  --model microprice \
  --half-spread-ticks 2 \
  --quantity 10 \
  --fills-output data/fills.jsonl
```

The simulator is intentionally conservative. A new bid or ask joins behind the
visible quantity at precisely its quote price, becomes active only after the
recorded event that created it, and is considered filled only by a later public
`TradeObserved` event. An exact-price trade first consumes queue ahead; a trade
through a better-priced resting quote fully fills its remaining simulated
quantity. Book-size reductions, cancellations, and disappearing displayed
liquidity never create fills, while same-price visible increases make queue
position more conservative. Filled sides are not automatically replenished.

The compact JSON output reports fill quantities, quote-price notionals, net
token position, and net cash flow. Net cash flow is **not profit or P&L**: it
excludes settlement and mark-to-market value, fees, rebates, rewards, gas, and
capital requirements. `--fills-output` writes atomic JSONL evidence records;
the command only reads the existing journal and makes no network calls.

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
