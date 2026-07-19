# Horus Engine

Horus Engine is a research-oriented prediction-market market-making system.
Live trading is disabled by default.

## Development status

Horus Engine models exchange-independent orders and immutable order-book
snapshots. It has no exchange connectivity, order submission, or live trading
capability.

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
