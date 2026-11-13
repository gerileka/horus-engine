# Fill-assumption audit and sensitivity

This is an execution-model experiment, not a strategy benchmark or a fill
guarantee. Live orders remain unavailable. The legacy default is unchanged.
The name `ConservativeFillSimulator` is historical: **trade-through full fills
are not a guaranteed lower bound on achievable executions**.

## Hand audit

Each row below was independently calculated from the event/lifecycle rules,
then encoded as adversarial assertions. Prices and quantities use Decimal.

| Case | Calculation / decision |
|---|---|
| Exact queue, partial fills | Bid 0.40, order 5, displayed queue 10. SELL prints at 0.40 of 10, 3, 2 consume queue to 0, then fill 3 and 2; buy notional 2.00. |
| Aggressor side | A BUY print at 0.40 never consumes a resting bid, even if size is 99. BUY trades can reach our ask, SELL trades our bid. |
| Trade-through | A SELL print at 0.39 of only 0.01 fills the entire baseline bid of 5 at 0.40, regardless of queue. This is an assumption, not observed execution. Exact-only stress gives no fill and leaves queue unchanged. |
| No lookahead | A trade that triggers PLACE cannot fill that new quote. REPLACE first lets the observed trade affect the old order, then resets both new sides behind displayed liquidity. |
| Cancellation | CANCEL removes simulated sides after the event; later trades cannot fill them. No venue acknowledgement or real cancel-in-flight is modelled. |
| Queue uncertainty | Doubling displayed queue from 10 to 20 gives zero fills on exact prints totalling 15; queue remains 5. Visible size decreases never advance queue. Visible increases are multiplied too; snapshots can only raise the bound. |
| Activation delay | One-event stress ignores the first event after PLACE/REPLACE for fills **and queue consumption**. It resets on every replacement and is not a millisecond latency estimate. |
| Repeated trade | With queue 1 and size 5, two identical exact prints of 3 produce baseline fills 2 + 3. Repeated-payload abstention processes only the first and gives 2, but identical legitimate trades could be discarded. |
| Snapshot discontinuity | A later snapshot on HOLD suspends both existing sides under snapshot stress until the next PLACE/REPLACE. It does not manufacture a corrected queue or claim a real cancellation. |

Existing adversarial cases additionally cover queue increases/decreases,
snapshots, wrong prices, complete/partial orders and both sides. The new
assumptions preserve that lifecycle rather than implement a second simulator.

## Fixed comparison

The application freezes one header/update read, applies quality policy v1 and
replay validation to that same evidence, then creates **one** midpoint shadow
analysis reused across all scenarios. Rejected/unavailable history yields
`scenarios: null`, never successful zero-fill results. The repository example
opens SQLite with `mode=ro`; it does not initialize, finalize or repair it.

Generate the existing synthetic demo into a new directory, then run:

```bash
uv run --offline python -m examples.offline_demo --output /tmp/horus-demo
uv run --offline python -m examples.fill_sensitivity \
  --database /tmp/horus-demo/synthetic-journal.db --session-id synthetic-fills
uv run --offline python -m examples.fill_sensitivity \
  --database /tmp/horus-demo/synthetic-journal.db --session-id synthetic-no-fill
```

The example fixes midpoint, one-tick half spread and quantity 2; the application
function accepts another validated quote config. The fixed matrix is declared
before outcomes, not selected to improve a strategy result:

| Scenario | Assumption | Synthetic fills-session buy / sell quantity |
|---|---|---|
| legacy_baseline | displayed queue, next event, full through fill | 1.25 / 2 |
| exact_price_only | no through fills or through queue depletion | 1.25 / 0 |
| double_displayed_queue | 2× displayed starting queue and observed increases | 1.25 / 2 |
| one_event_activation_delay | skip first later event after each placement | 0 / 2 |
| repeated_trade_abstention | skip repeated full normalized trade payloads | 1.25 / 2 |
| snapshot_abstention | suspend after later snapshot until replacement | 1.25 / 2 |
| combined_stress | all five stresses | 0 / 0 |

The baseline bid/ask are 0.49/0.51. Buying 1.25 costs 0.6125; selling 2 receives
1.02. Cash flow is +0.4075 **with a short position of 0.75**, not profit.
All seven scenarios give zero fills in the synthetic no-fill session. Queue
stress has no effect in the fills session because the quotes are inside the
displayed spread; that does not establish queue irrelevance. Dedicated hand
cases above isolate queue, repeats and snapshots that this tiny demo lacks.

Integration assertions verify both eligible sessions, deterministic reports,
input byte immutability and fail-closed rejection of a deliberately gapped copy.
These numbers are synthetic oracle values, not evidence of live fills. Local
verification results and exact-commit CI evidence belong in the PR.

## Unsupported evidence and interpretation

- No persisted venue trade identifier or source sequence: equal normalized
  payloads cannot distinguish retransmission from distinct trades. Baseline
  counts every event; stress abstains globally on repeated payloads. Neither
  proves deduplication correctness. The source journal is never rewritten.
- Public prices/sizes do not establish our order acknowledgement, hidden
  priority, matching rules, routing or queue rank. A zero displayed queue is
  not a promise of immediate execution. Multipliers cannot model hidden size
  when displayed size is zero.
- Whole-event activation delays do not measure network delay, wall-clock time,
  partial queue progress while pending, or cancellation races. Replacements
  cancel old simulated sides immediately; this can understate real exposure.
- Snapshot abstention is deliberately pessimistic, not an inference that every
  snapshot lost priority. Eligibility verifies recorded consistency, not venue
  message completeness or correct market microstructure.
- None of these scenarios is a statistical confidence interval or a proven
  ordering/bound on fills or profit. No fees, rebates, rewards, inventory
  valuation, collateral limits or actual execution are included.
- Live public captures remain access-blocked (see public-api-smoke.md). This
  PR completes an eligible **synthetic** comparison, not a venue calibration.
  Re-run the same declared matrix on traceable, eligible venue sessions once
  ordinary DNS/egress is available; do not tune assumptions to desired results.

## Existing-solution preflight (2026-11-13)

Checked upstream GitHub metadata for
[hftbacktest](https://github.com/nkaz001/hftbacktest): MIT, repository reports
last push 2025-12-23, and describes tick-data queue/latency simulation with
Binance/Bybit examples. README retrieval was unavailable through this connector;
this is not a complete dependency audit or a measured head-to-head comparison.
It is a relevant future calibration candidate, not a drop-in replacement for
Horus's typed Decimal journal, current replay invariants or unavailable venue
acknowledgements. Reusing the existing simulator with explicit small research
knobs avoids an unvalidated engine/data migration. No new dependency, service,
model, paid API or MCP is needed for this offline deterministic calculation.

Approval accepts the unchanged baseline plus explicit, uncalibrated stress
comparisons and these evidence limits. It does not approve trading or establish
research readiness; inventory/cost/out-of-sample benchmarking remains issue #22.
