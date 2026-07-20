# Horus Engine

Horus Engine is a research-oriented prediction-market market-making system.
Live trading is disabled by default.

## Development status

Horus Engine models exchange-independent orders, immutable order-book
snapshots, and application-level exchange contracts. It can retrieve and
normalize public Polymarket market metadata through the read-only Gamma API,
without passing raw venue payloads into the application or domain layers.

It still does not retrieve live order books, cannot authenticate, and cannot
submit or cancel orders. Live trading remains unavailable.

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
