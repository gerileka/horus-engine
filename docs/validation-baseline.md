# Horus restart: baseline and validation plan

Date: 2026-11-03. Source inspected: `33c8c7083f28d92fc63bdfea73fac251db491951`.

## Verdict

Research scaffold implemented; research readiness is **not yet demonstrated**.
No live trading, profitability, or current Polymarket compatibility has been verified.
This baseline is deliberately partial: the local full suite did not finish.

Tracking hub: [#16](https://github.com/gerileka/horus-engine/issues/16).
Baseline work: [#17](https://github.com/gerileka/horus-engine/issues/17).

## Reproduce in a clean environment

Use Python 3.12 and uv:

```sh
uv sync --locked --all-groups
make format-check
make lint
make typecheck
make test
horus --help
horus collect --help
horus shadow --help
horus simulate-fills --help
```

After installation, use `uv run horus ...` if the environment is not activated.
Keep the lockfile unchanged; do not upgrade packages to make a failing baseline pass.

## Observed environment

- Linux, Python 3.12.3; project-local virtual environment.
- uv 0.13.7 obtained from a public PyPI wheel and checked against its published SHA256.
- Locked packages: pytest 9.1.1, ruff 0.15.22, mypy 2.3.0, coverage 7.15.2,
  aiosqlite 0.22.1, httpx 0.28.1, websockets 16.1.1.
- uv.lock SHA256:
  `b0f09a6687fa5b15a4bea2c75304289d23f74c0d473fdf68558474e1a8677078`.
- Terminal GitHub DNS was unavailable (`Could not resolve host: github.com`).
  All 73 tracked blob files were therefore retrieved through the authenticated
  GitHub connector at the pinned source commit. This is a source snapshot,
  not a local clone or inspection of the owner's other working copies.
- Locked dependencies were installed through managed execution. Checks below ran
  in the restricted local execution environment without changing security settings.

## Actual check results

The equivalent aggregate command was:

```sh
make UV='/tmp/horus-tools-20261103/uv --offline --cache-dir /tmp/horus-tools-20261103/cache' check
```

The temporary tooling/cache paths are environment-specific, not project requirements.

| Check | Observed result |
|---|---|
| format-check | PASS: 64 files already formatted |
| lint | PASS: all checks passed |
| typecheck | PASS: no issues in 64 source files |
| full test/coverage | INCOMPLETE: 446 items collected; stalled in the first SQLite collection integration test; interrupted |
| selected synchronous tests | PASS: 185 tests in 0.30 seconds |
| CLI help | PASS: root, collect, shadow, simulate-fills help returned successfully |

The selected test command was:

```sh
.venv/bin/python -m pytest tests/unit/domain \
  tests/unit/application/test_models.py \
  tests/unit/application/test_events.py \
  tests/unit/application/test_order_book_state.py \
  tests/unit/application/test_fair_value_and_shadow_quotes.py \
  tests/unit/application/test_conservative_fills.py -q
```

These 185 tests are a **subset**, not a full-suite pass. No fresh full-suite
coverage percentage is available.

### Local async SQLite blocker

A minimal program importing only asyncio and aiosqlite, connecting to
`:memory:` and executing `select 1`, also did not finish. An 8-second
process timeout exited 124. A 3-second faulthandler dump showed the aiosqlite
worker in `core.py:59 _connection_worker_thread`, while the main thread
waited in `selectors.py:468 select` through the asyncio event loop.

This reproduces outside Horus application code but does not establish the
underlying cause. Do not weaken sandbox settings or change production code to
mask it. The full suite must be checked in the normal GitHub CI runner; if it
fails there, diagnose that failure before proceeding. If CI passes, retain this
local limitation in the evidence rather than claiming local success.

Historical evidence: the existing [Checks run on the inspected main commit](https://github.com/gerileka/horus-engine/actions/runs/30892744362)
succeeded on 2026-08-04. That is not a new run of this PR.
The PR's own Checks result is tracked in GitHub and must be reviewed before merge.

## Existing coverage map and gaps

- Integration: collection composition into a real SQLite journal; journal replay;
  shadow quotes and fills with a no-same-event-fill assertion.
- Units: financial primitives, books, local state transitions, replay, quotes,
  fill rules, CLI validation, public adapter parsing and SQLite persistence.
- Missing evidence is not the same as missing implementation: a full reproducible
  offline user demo, current public-API observations, session-quality assessment,
  independent fill-assumption challenges, and out-of-sample evaluation remain
  explicit work items. Do not duplicate existing tests merely to add counts.
- No public venue requests, real recordings, benchmarks, deployment checks or
  financial transactions were performed for this baseline.

## Ordered validation gates

| Gate | Issue | Acceptance focus |
|---|---|---|
| 1 | [#17](https://github.com/gerileka/horus-engine/issues/17) | Fresh checks, honest evidence, reproducible setup |
| 2 | [#18](https://github.com/gerileka/horus-engine/issues/18) | Deterministic offline collect → shadow → fills demo |
| 3 | [#19](https://github.com/gerileka/horus-engine/issues/19) | Bounded public-API compatibility and sample capture |
| 4 | [#20](https://github.com/gerileka/horus-engine/issues/20) | Quality diagnostics and analysis eligibility |
| 5 | [#21](https://github.com/gerileka/horus-engine/issues/21) | Fill-rule audit and uncertainty/sensitivity |
| 6 | [#22](https://github.com/gerileka/horus-engine/issues/22) | Held-out benchmark, inventory valuation, costs and uncertainty |
| 7 | [#23](https://github.com/gerileka/horus-engine/issues/23) | Reproducibility and proceed/pivot/stop verdict |

Each issue has acceptance criteria and dependencies. Deliver one focused PR per
issue (split if needed), link evidence and checks, and use `Closes #N`.
Issues remain open until reviewed/merged with evidence. Live execution is not
part of these gates. No automatic merge is implied.
