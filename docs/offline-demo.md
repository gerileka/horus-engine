# Reproducible offline demonstration

All inputs are **synthetic**. This is an orchestration and reproducibility demo,
not public-venue evidence, a profitable strategy, or permission to trade.
No credentials, public API requests, live orders, or binary journals are needed.

## Run from the repository root

After installing Python 3.12, uv and the locked dependencies:

```sh
uv sync --locked --all-groups
uv run --offline python -m examples.offline_demo --output /tmp/horus-offline-demo
```

Choose a new output directory each time. Existing directories are refused, even
if empty. A failed run retains its partial output for diagnosis; select a fresh
path for a retry. The example uses the existing public CLI parser/entry function
for `collect`, `shadow`, and `simulate-fills`. Only collection's existing
dependency-injection seam is replaced with a synthetic catalog, fixed clock and
finite event stream. Persistence, session processing and analysis are real.
The HTTP client uses a rejecting mock transport; no venue adapters are invoked.
This is a repository-local example, not a new installed `horus` subcommand.

Two sessions are collected into a new real SQLite journal. Each is analyzed
twice with midpoint fair value, a one-tick half spread, and quote quantity 2.
The example exits with an error if any command fails, analysis changes the input
journal bytes, or the two sets of analysis artifacts differ. `report.json`
labels synthetic provenance and reports `journal_unchanged: true`, two analysis
passes and eight matching JSON/JSONL artifacts.

## Independent expected results

Both sessions use synthetic market/token IDs, fixed UTC timestamps on
2026-01-01, a tick of 0.01, and this initial book:

- Bid: 0.48, quantity 2; ask: 0.52, quantity 3.
- Midpoint: (0.48 + 0.52) / 2 = 0.50.
- Hypothetical bid: 0.49; ask: 0.51; size: 2 each.
- Neither quote joins behind visible size at its exact price.

### `synthetic-fills`

1. Snapshot places the quote, with no same-event fill.
2. Sell-aggressor trade at 0.49, size 1.25, fills 1.25 of our bid.
3. Buy-aggressor trade at 0.52, size 1, trades through our 0.51 ask.
   **The current simulator fills the entire remaining ask size 2**, not size 1.
   This is an existing execution-model assumption, not proof of realistic fills.
4. Disconnect marks the book stale and cancels the hypothetical quote.

Expected quote actions: `place, hold, hold, cancel`. Fills occur only on
sequences 2 and 3. Buy notional = 1.25 × 0.49 = **0.6125**; sell notional =
2 × 0.51 = **1.02**. Net token position = **-0.75**; net cash flow = **0.4075**.
That cash flow is **not profit**: the short token position is unvalued, and fees,
settlement, inventory constraints and other costs are not modeled here.

### `synthetic-no-fill`

1. A sell trade arrives before any snapshot: no quote or fill is permitted.
2. Snapshot places the same 0.49/0.51 quote.
3. Sell-aggressor trade at 0.50 does not reach the 0.49 bid: no fill.
4. Disconnect leaves stale state and cancels the quote.

Expected quote actions: `none, place, hold, cancel`. The first decision is
`AWAITING_SNAPSHOT`; the last is `STALE`. Fill output is empty and all filled
quantities/notionals are zero. The collector terminates on disconnect; this
example does not demonstrate automatic reconnection or resumed quoting.

## Artifacts and manual inspection

The output contains `synthetic-journal.db`, `report.json`, one collection
summary per scenario, and `pass-1/` and `pass-2/`. Each pass has shadow and fill
summaries plus decision and fill JSONL for both scenarios. Financial amounts
are Decimal strings, not JSON floats. Only the `database` field in summaries is
normalized to `<synthetic-journal>`; analytical values, timestamps and ordering
are preserved. No binary SQLite journal is committed to the repository.

You can invoke the installed analysis commands yourself after the example:

```sh
uv run --offline horus shadow \
  --database /tmp/horus-offline-demo/synthetic-journal.db \
  --session-id synthetic-fills --model midpoint --half-spread-ticks 1 --quantity 2
uv run --offline horus simulate-fills \
  --database /tmp/horus-offline-demo/synthetic-journal.db \
  --session-id synthetic-fills --model midpoint --half-spread-ticks 1 --quantity 2
uv run --offline pytest tests/integration/test_offline_demo.py -q
```

The integration test checks these hand-calculated values, rejects DNS/socket
connection attempts, compares a second fresh collection's normalized artifacts,
and verifies refusal to overwrite existing output. Existing lower-level replay
and no-lookahead tests remain the baseline; this adds the missing user journey.
