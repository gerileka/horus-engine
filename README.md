# Horus Engine

Horus Engine is a research-oriented prediction-market market-making system.
Live trading is disabled by default.

## Development status

Horus Engine models exchange-independent orders, immutable order-book
snapshots, and application-level exchange contracts. It can retrieve and
normalize public Polymarket market metadata through the read-only Gamma API,
retrieve public CLOB order-book snapshots, and stream public CLOB order-book
and trade data into exchange-independent application events without passing raw
venue payloads into the application or domain layers.

It cannot maintain a continuously reconstructed local order book, automatically
reconnect, authenticate, submit orders, cancel orders, or trade. Live trading
remains unavailable.

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
