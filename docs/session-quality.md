# Session quality and fail-closed research eligibility

`horus quality` reads an existing SQLite journal without creating, initializing,
repairing or finishing it. It emits one JSON report and exits **0 only when the
session meets the versioned research policy**. An unsuitable or unreadable
session returns 1; invalid command input returns 2. No journal schema changes,
migrations, new dependencies, network calls or live-trading features are added.

## Research workflow

Run the quality gate **before** requesting strategy outputs, using shell `&&`
so a failed assessment cannot silently continue into analysis:

```sh
uv run --offline python -m examples.offline_demo --output /tmp/horus-quality-demo
uv run --offline horus quality \
  --database /tmp/horus-quality-demo/synthetic-journal.db \
  --session-id synthetic-fills > /tmp/horus-quality-demo/quality.json &&
uv run --offline horus shadow \
  --database /tmp/horus-quality-demo/synthetic-journal.db \
  --session-id synthetic-fills --model midpoint --half-spread-ticks 1 --quantity 2 &&
uv run --offline horus simulate-fills \
  --database /tmp/horus-quality-demo/synthetic-journal.db \
  --session-id synthetic-fills --model midpoint --half-spread-ticks 1 --quantity 2
```

Choose a fresh directory for the demo. The standalone lower-level `shadow` and
`simulate-fills` commands retain their existing exploratory behavior: **they do
not automatically invoke this gate**. A research/benchmark pipeline must use
`quality` and honor its exit code; calling those commands directly is not an
eligibility assertion. Approval of this PR accepts that explicit workflow, not
an invisible change to the existing commands' behavior.

For real recordings, keep the report private alongside the input and its
provenance manifest; it includes the session identifier. No raw recording is
published by this command. Issue #19's DNS/access blocker remains unresolved:
the examples and adversarial fixtures here are synthetic, not venue evidence.

## Policy `horus-session-quality-v1`

These thresholds are defined independently of and before strategy results;
neither quote/fill returns nor P&L are inputs to the assessor. There are no
per-run threshold overrides to tune after seeing an attractive result.

- At least **two updates**, including **one authoritative snapshot**.
- At least **0.5 of recorded post-event observations** are SYNCHRONIZED.
  This is an event-count fraction, **not synchronized time or uptime**.
- **Zero INVALID** observations and **zero nonterminal STALE** observations.
  One final STALE event is allowed as the normal disconnect boundary. A stale
  observation followed by more history is rejected even if a snapshot restores
  synchronization later. AWAITING_SNAPSHOT observations remain visible and
  reduce the synchronized fraction; they are not silently removed.
- **Zero source-timestamp regressions** between adjacent stored events. Equal
  timestamps are counted separately and allowed. All event types are examined,
  including trades whose timestamps do not update the book-state clock.
- **Zero source timestamps outside the recorded start/finish bounds**. This is
  a conservative clock-consistency policy, not proof that source/local clocks
  agree. Clock skew can cause a valid feed to be rejected; do not rewrite the
  times to make it pass. An unfinished session has no measured after-finish
  count, rather than a fabricated zero.
- A present, **verified terminal recording state**; contiguous local arrival
  sequences starting at 1; a header agreeing with its update count; and success
  from the existing full replay integrity checks. Unknown verification is a
  rejection, never an implicit pass. A finished header alone is insufficient.

Policy changes require a new version and rationale before subsequent strategy
evaluation. Eligibility is necessary for this workflow, not sufficient to prove
that public order-book history is complete, realistically executable or useful
for a given research question. There is no minimum-trade threshold: measured
zero trades differs from unavailable data and may still be useful for quote-only
analysis, but is not evidence supporting fill-performance conclusions.

## What the JSON establishes

The report includes `eligible`, a stable list of rejection `reasons`, policy ID
and thresholds, `sequence_consistent`, `replay_integrity` and
`terminal_complete`. Measurements include total/event-type/snapshot/trade counts,
counts for all four post-event statuses, contiguous status periods identified
by **first/last recorded sequence** and their source event timestamps, timestamp
ties/regressions/bounds anomalies, and terminal-record evidence.

Status periods describe the post-state recorded for each event. They do not
pretend that state persisted continuously until the next event. Finalization
metadata can mark a session stale without adding an event; that terminal state
is reported separately, not invented as an extra observation. Raw post-status
counts/periods are explicitly labelled as unverified unless replay succeeds.

For the `synthetic-fills` demo: four updates, one snapshot, two trades, three
synchronized observations and one terminal stale observation give fraction
`"0.75"`; the report is eligible under v1. The `synthetic-no-fill` session has
fraction `"0.5"`, including its initial awaiting-snapshot observation, and is
also eligible. Neither output implies that their synthetic fills are realistic.

Unavailable uptime, message-loss and receipt-latency measurements carry explicit
reasons: the journal has no per-event local receipt clock, no comparable clock
pair and no venue sequence/completeness guarantee. A contiguous local sequence
does **not** establish that no venue messages were lost. An empty readable
session has measured counts of zero but an undefined synchronized fraction.
An absent/unreadable session has `measurements: null`, not zero counts. A replay
failure leaves terminal completeness unknown and eligibility false.

## Integrity and compatibility

The assessor reads the header and updates once, freezes those immutable objects,
then reuses the existing replayer against that evidence. It does not reread a
different version during verification, sort gaps away or repair persisted
post-state metadata. The shared journal contract now names its read side
explicitly; existing adapters still satisfy the full journal protocol.

SQLite version 1 is unchanged. The CLI opens a URI with `mode=ro`, including
proper URI escaping for paths with spaces, and never calls schema initialization
or finalization. Adversarial coverage includes sequence/header corruption,
post-state disagreement, timestamp regressions and bounds, empty/unfinished/
missing data, invalid/stale histories, verification failure and cancellation.
A real SQLite integration test runs the two synthetic sessions, corrupts a
separate copy, and verifies that the assessor neither changes nor repairs either
input file. No real venue recording is required in normal CI.
