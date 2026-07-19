# Project instructions

## Purpose

This repository implements a research-oriented prediction-market
market-making system.

Live trading must remain disabled by default.

## Engineering rules

- Python 3.12.
- Use `uv` for dependency management.
- Use `src/` layout.
- Use type annotations on all public functions.
- Use `Decimal` for prices, quantities and monetary values.
- Do not use floating-point values for financial calculations.
- Keep exchange-specific code behind interfaces.
- Do not introduce microservices.
- SQLite is the initial persistence layer.
- Never log credentials, signatures or private keys.
- Never add live-order functionality without an explicit feature flag.
- Do not modify unrelated files.

## Required checks

Run before completing a task:

```bash
make format-check
make lint
make typecheck
make test
```
