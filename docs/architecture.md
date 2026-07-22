# Architecture

## Dependency direction

Horus Engine keeps financial concepts and application boundaries separate:

```text
domain
  ↑
application contracts
  ↑
infrastructure adapters
```

The domain package contains immutable financial primitives and order models.
The application package defines exchange-neutral market metadata, normalized
events, and gateway protocols. Future concrete exchange adapters belong in
infrastructure and may depend on both lower layers; neither the domain nor the
application layers may depend on an exchange SDK or venue payload type.

The infrastructure layer contains read-only public Polymarket adapters:
`PolymarketMarketCatalogGateway`, `PolymarketOrderBookSnapshotGateway`, and
`PolymarketMarketDataStreamGateway`.
They translate Gamma market payloads and CLOB book payloads at the
infrastructure boundary, so raw Polymarket models never enter application or
domain code. For these adapters, the Polymarket condition ID is the application
`MarketId`; Gamma's internal market ID and other venue metadata are
intentionally not exposed.

The catalog adapter is read-only and retrieves public market metadata only. Its
status mapping is conservative: a closed Gamma market is `CLOSED`, an active
market accepting orders is `ACTIVE`, and every other state is `SUSPENDED`. It
does not infer `RESOLVED` because Gamma's public market response does not
provide an authoritative resolution signal used by this adapter.

The CLOB snapshot adapter retrieves one unauthenticated `GET /book` response
for a requested token. Although the endpoint is queried by token, it validates
both returned `market` and `asset_id` against the request before returning a
book; an observed book must never be attributed to another market or token. It
parses numeric JSON with `Decimal`, validates the response tick size and every
price's exact tick alignment, and maps levels to the immutable domain
`OrderBook`. Empty, locked, and crossed books remain representable because they
are observed venue state rather than adapter errors.

`PolymarketMarketDataStreamGateway` provides a separate unauthenticated public
market-data capability through
`wss://ws-subscriptions-clob.polymarket.com/ws/market`. Each invocation opens
one connection, sends one token-ID subscription (`assets_ids` plus `type` of
`market`), and emits normalized `BookSnapshotReceived`, `PriceLevelChanged`,
`TickSizeChanged`, and `TradeObserved` events. Raw JSON parsing is isolated in
`websocket_parsing.py`, while `market_stream.py` owns only connection lifecycle
and the application-level text `PING` heartbeat (ten seconds by default).
`PONG` is ignored.

The parser decodes JSON financial numbers as `Decimal`, interprets timestamps
strictly as nonnegative Unix milliseconds in UTC, and verifies that every
message market and token belongs to the requested subscription before emitting
an event. WebSocket book snapshots deliberately do not apply tick alignment:
the public payload has no authoritative tick size. The adapter never stores or
reconstructs mutable order-book state. It has no automatic reconnection; an
unexpected transport end produces one `MarketDataDisconnected` event and then
the stream ends.

Gateway boundaries use `typing.Protocol` so adapters can satisfy them through
structural typing without requiring shared base classes. `OrderBookSnapshotGateway`
and `MarketDataStreamGateway` are separate capabilities: retrieving an
authoritative point-in-time book does not imply an adapter can produce an event
stream. `MarketDataGateway` composes both protocols for consumers that require
both capabilities. Their asynchronous methods expose only immutable domain and
application values. `AccountGateway` is deliberately deferred until the domain
has a small, coherent collateral and position model.

Order-book snapshots and market-data events are distinct: a snapshot is an
authoritative immutable view of aggregate liquidity at one point in time, while
events communicate a snapshot, a price-level update, a trade, or connection
state as it is observed. Events contain normalized values rather than raw
exchange payloads, preserving a stable application contract across adapters.
The snapshot adapter and stream adapter remain separate capabilities: a
snapshot does not imply streaming, and streaming does not imply a locally
reconstructed order book. There is no background polling, authentication, order
management, or live-trading lifecycle in either adapter.

## Local order-book reconstruction

`LocalOrderBookState` belongs in the application layer because it consumes only
the normalized `MarketDataEvent` contract and exchange-neutral domain values.
It does not know about Polymarket payloads, HTTP, WebSockets, or another venue.
Conversely, the domain `OrderBook` remains an immutable observed snapshot; the
application state machine privately reconstructs fresh immutable snapshots from
mutable aggregate level mappings and never exposes those mappings.

Each instance has one fixed `MarketId` and `TokenId` and starts in
`AWAITING_SNAPSHOT`. A valid `BookSnapshotReceived` atomically replaces all
levels and enters `SYNCHRONIZED`. Only then can a `PriceLevelChanged` replace a
side's aggregate quantity at one price; zero removes that price. Empty,
one-sided, and locked books remain valid synchronized observations. A valid
update that makes the book crossed is retained for diagnosis but moves the
machine to `INVALID`; crossed snapshots are rejected before replacing the
previous state.

`MarketDataDisconnected`, `MarketDataReconnected`, and a compatible
`TickSizeChanged` make the machine `STALE` while retaining its last immutable
book for diagnostics. A tick-size change records the new tick without rounding
or rewriting existing levels. `STALE` and `INVALID` never recover from an
incremental update: a new tick-aligned, uncrossed authoritative snapshot is
required. All rejected validation failures preserve the complete prior state;
the committed crossed-update observation is the intentional exception.

The machine rejects market or token identity mismatches and timestamps strictly
earlier than the last successfully applied event. Equal timestamps are accepted
in arrival order. This timestamp rule is a safety check, not proof that no
exchange message was missed; it intentionally provides neither buffering nor
sequence-number inference. Trades are explicit no-ops for book state.

No network or reconnection logic lives in this state machine. A caller decides
how and when to obtain a REST snapshot, consume a stream, reconnect transport,
and apply normalized events. The state machine has no persistence, background
tasks, strategy, execution, authentication, or trading responsibility.

## Domain model

The domain layer uses immutable, Decimal-backed financial value objects.
`Price` represents a binary-market price, `Quantity` represents a strictly
positive number of contracts, and `NonNegativeQuantity` represents quantities
that may be zero, including fills and aggregate visible depth. `TickSize`
provides exact tick-alignment validation without float arithmetic.

Orders are immutable domain records. `OrderRequest.create` (and its validating
constructor) require a tick size to validate price alignment, but do not retain
that validation input as state.
Tracked `Order` records carry a validated internal identifier, lifecycle status,
and fill quantities. Pending, open, and rejected orders cannot have fills;
cancelled orders may be unfilled or partially filled but cannot be completely
filled; fully filled orders use the `FILLED` status.

`OrderBook` is an immutable snapshot of aggregate `OrderBookLevel` values. It
copies and normalizes input levels into price-ordered tuples rather than acting
as a mutable exchange cache. This makes every snapshot deterministic and safe
to share while retaining the exact state that was observed. Locked and crossed
books are represented, rather than rejected, because they are useful evidence
of an exchange state that downstream consumers may need to inspect or discard.
