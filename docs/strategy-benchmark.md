# Fixed strategy benchmark v1 — 2026-11-16

## Decision and evidence boundary

**Collect more data; no empirical edge demonstrated.** This change supplies a
fixed, reproducible benchmark and synthetic accounting oracle, not a genuine
venue out-of-sample result. The three synthetic sessions deliberately repeat a
hand-calculated history. They are not independent observations and are never a
statistical sample. Live capture remains unverified after the DNS failure in
[public-api-smoke.md](public-api-smoke.md). Issue #22 remains
open for actual held-out data and verified venue-specific costs.

## Predeclared protocol

`application.strategy_benchmark.benchmark_plan()` is the immutable v1 protocol:

- Midpoint and top-level microprice; half-spreads one and two ticks; size two.
- Legacy fill baseline, exact-price-only, and combined execution stress from
  [fill-sensitivity.md](fill-sensitivity.md); plus a separate no-quote control.
- Sort the complete supplied manifest by session start before filtering.
  First floor(n/3) train, next floor(n/3) validation, remainder held-out. At least
  three distinct, finalized, nonoverlapping sessions; equal boundary times
  reject. Caller order cannot change splits. Quality-rejected sessions remain
  in their original split and count as unusable. Missing/unreadable/unfinished
  headers invalidate the whole split because chronology cannot be established.
- Every arm is evaluated separately in every eligible session. No training,
  parameter selection, cross-session position carry, ranking or held-out tuning.
  For a future empirical run, freeze and archive the code commit, plan digest
  and full session manifest **before inspecting held-out results**. Software
  cannot prevent the operator from repeatedly selecting a favorable manifest.
- Reuse existing fail-closed quality/replay checks on one frozen read per
  session. Quotes only use current reconstructed state; existing simulator
  processes a trade before activating that event's new quotes. Future marks
  are evaluation labels only and cannot feed quote generation.
- Report all 12 model/width/execution combinations plus a zero-activity control.
  Observation quoteability is not wall-clock availability or fill probability.

## Accounting and observations

All prices, money, quantities, elapsed-time arithmetic and means use Decimal.
JSON serializes financial values as strings; missing values are `null`, never
zero-fill successes. Per-session initial inventory and initial cash are zero.
Unconstrained hypothetical short inventory is allowed; collateral, funding,
capital requirements and implementability are **not** established.

- Turnover is buy notional + sell notional, not return.
- Cash flow is sells minus buys, not realized P&L. No cost-basis lots are matched.
- Position is buys minus sells; peak absolute position and integral of absolute
  position in token-seconds run from first observation through session finish.
  Position is constant between observed fills, including the terminal gap.
- Terminal position is marked to the last synchronized, valid two-sided midpoint
  only when its observation is within one second of session finish. A terminal
  disconnect or older observation produces unavailable valuation. Midpoint is
  not an executable liquidation quote or settlement price. No outcome settlement
  is inferred; observations can reflect a reconstructed book older than the
  most recent trade. No real-time market freshness claim is made.
- One- and five-second markouts use the first observed state at or after the
  fill time plus horizon, no later than one second beyond that target. Missing,
  unsynchronized, invalid or out-of-session marks stay unavailable. Buy sign +1,
  sell sign -1: sign × (future midpoint − fill price). Negative is adverse.
  Report available/unavailable fill counts, quantity-weighted mean and signed
  value sum. Sparse observations can censor the sample non-randomly; never
  extrapolate the available mean to missing fills.
- Cost hypotheses are net zero, +10 basis points cost and −1 basis point rebate,
  applied linearly to traded quote notional. Cash after cost and marked value
  after cost are separate. These are deliberately **not venue rates**.

### Dated Polymarket fee assumptions — 2026-11-16

Official source attempted: <https://docs.polymarket.com/trading/fees>.
Fetch failed `CERT_HAS_EXPIRED`; no TLS bypass was used. Current maker schedule,
market-specific fee parameters, eligibility and rebates remain **unverified**.
The journal does not persist these inputs. No rewards, fee exemptions or rebates
are earned by assumption. Before any empirical cost conclusion, attach the
applicable dated official schedule and market-specific parameters and replace
this generic sensitivity with an appropriate predeclared venue model. Unknown
costs are not a claim of zero fees.

## Run

From a clean locked Python 3.12 setup (`uv sync --all-groups`):

```sh
uv run --offline python -m examples.strategy_benchmark --synthetic > synthetic-benchmark.json
```

No network, database writes, credentials or trading. This emits the complete
fixed plan, plan SHA256, per-session evidence digests, chronological groups,
quality results, accounting and observation availability. Exit 0 means only
that all manifest sessions passed the quality gate, **not research readiness**.
The verdict remains `insufficient_evidence_no_edge_claim`.

For an existing SQLite journal containing at least three nonoverlapping
sessions, use explicit IDs; the runner opens `mode=ro` and never initializes it:

```sh
uv run --offline python -m examples.strategy_benchmark \
  --database /path/to/journal.db \
  --session-id chronological-a --session-id chronological-b \
  --session-id chronological-c > benchmark.json
```

Do not mix `--synthetic` with a database. Nonzero exit means missing/rejected
input, not an investment loss. Raw journals are not uploaded automatically.
Archive the supplied manifest and report privately with capture provenance.
Evidence hashes bind normalized headers/updates; they do not authenticate the
venue or prove independent sampling. Keep the exact Python/code version too.

## Independently calculated synthetic oracle

Every synthetic session starts with 0.48 bid / 0.52 ask, sizes two each, midpoint
0.50. Width-one quotes are 0.49 / 0.51. At second one a sell of one at 0.49 buys
one for 0.49; at second two a buy at 0.53 triggers the legacy full sell of two
at 0.51. Later snapshots at seconds six and seven preserve midpoint 0.50.

- Cash = −0.49 + 1.02 = **+0.53**; position = **−1**; marked inventory = **−0.50**.
- Marked value before costs = **+0.03**, not 0.53 profit.
- Turnover = **1.51**; 10bps assumed cost = **0.00151**; marked value = **0.02849**.
- Hypothetical 1bps rebate = **0.000151**; marked value = **0.030151**.
- Peak absolute inventory = **1**; absolute-position exposure = **6 token-seconds**.
- 1s markouts: first fill available at +0.01; second unavailable (next observation
  too late). 5s: both available; weighted mean +0.01, signed value sum +0.03.
- Exact-only width-one: buy one, sell zero; marked value before costs +0.01.
- Combined stress and no-quote control have no fills and zero cash/inventory.
  Wider bands can fill differently; all are retained, no winner is selected.

Unit oracles also retain negative markouts, stale/missing terminal valuations,
read failure/cancellation, duplicate/overlapping manifests, held-out rejection
without split shifting, and unchanged earlier rows when held-out input changes.
The real SQLite integration test checks repeatability, unchanged input bytes,
CLI results and corrupted-history rejection.

## Existing-solutions review

Checked 2026-11-16: upstream `nkaz001/hftbacktest` API metadata reports MIT,
unarchived, last push 2025-12-23. Its queue/latency backtester is a candidate for
a separate calibrated execution study, not a drop-in for Horus's normalized
SQLite quality contract; no compatible evidence or measured benefit justifies
migration here. Reuse Horus's existing midpoint/microprice, shadow quotes,
quality/replay and fill engines; this change adds reporting only, no dependencies
or model weights. Free official Polymarket fee documentation was attempted but
unavailable as above. No paid service or new API/MCP is needed for offline work.

## Limits and resume condition

Three sessions is a structural minimum for three groups, not an adequate sample
size. Results are descriptive per session: no confidence intervals, p-values,
annualization or aggregate claims. Sessions may share a market/token and be
dependent. Instantaneous simulated cancellations, unknown queue priority and
uncalibrated trade-through fills remain; the baseline is not a guaranteed lower
bound. Large samples, independent capture provenance, market-specific fees,
capital/inventory constraints and execution calibration are still necessary.

Owner of the empirical blocker: Horus research/data workflow (agent). Resume
when authorized public capture access works normally and yields enough eligible,
nonoverlapping, provenance-backed real sessions plus verified fee metadata.
Do not retune v1 after inspecting held-out outcomes. No live-order functionality
or deployment is introduced. Approval accepts the protocol/tooling, not an edge
or completion of the real-data portion of #22.
