# Public API compatibility and bounded captures

Reviewed **2026-11-12**. This work adds a reproducible, opt-in read-only smoke
procedure. It does not certify current venue compatibility, execution quality,
profitability or live trading. Real evidence may be blocked; a blocker must
remain a blocker, not be replaced with synthetic data or a successful label.

## Official-source review

The following official documentation URLs were checked on 2026-11-12, but
retrieval failed with **`CERT_HAS_EXPIRED`**:

- <https://docs.polymarket.com/quickstart/reference/endpoints>
- <https://docs.polymarket.com/developers/gamma-markets-api/get-markets>
- <https://docs.polymarket.com/developers/CLOB/websocket/market-channel>

Search fallback timed out. Certificate validation was not disabled. Therefore
these pages cannot be represented as freshly verified schemas for this review.
An accessible official source was checked separately: the publisher's maintained
[Python SDK](https://github.com/Polymarket/py-sdk), which was not archived and
reported a push on 2026-11-12. Source comparison is pinned to
`0661c24b0ad565c4a1d6ea7380f803f599facf92`:

- [Gamma model](https://github.com/Polymarket/py-sdk/blob/0661c24b0ad565c4a1d6ea7380f803f599facf92/src/polymarket/models/gamma/market.py):
  its Gamma normalization still reads `conditionId`, `outcomes`, `clobTokenIds`,
  `orderMinSize`, `orderPriceMinTickSize`, `active`, `closed`, `acceptingOrders`
  and `enableOrderBook`. These overlap Horus's current parser. Horus requires
  explicit YES/NO labels, maps tokens by label, and deliberately rejects
  unsupported or inconsistent records. SDK model overlap is not a live response.
- [Order-book model](https://github.com/Polymarket/py-sdk/blob/0661c24b0ad565c4a1d6ea7380f803f599facf92/src/polymarket/models/clob/order_book.py):
  `market`, `asset_id`, `tick_size`, `bids`/`asks`, and Decimal level `price`/`size`
  overlap Horus's REST snapshot parser. The SDK also accepts `token_id` as an
  alias for `asset_id`; Horus currently requires `asset_id`. This is a source-level
  tolerance difference, not proof that the live endpoint changed its fields.
- [Market subscription](https://github.com/Polymarket/py-sdk/blob/0661c24b0ad565c4a1d6ea7380f803f599facf92/src/polymarket/_internal/streams/clob/market_protocol.py):
  initial frames still use `type: market` and `assets_ids`. SDK custom events
  are controlled by `custom_feature_enabled`; Horus does not request them and
  accepts only book, price-change, tick-size-change and last-trade events. Horus
  fails on unknown event types rather than silently treating them as compatible.
- [Heartbeat](https://github.com/Polymarket/py-sdk/blob/0661c24b0ad565c4a1d6ea7380f803f599facf92/src/polymarket/_internal/streams/clob/heartbeat.py):
  application `PING`/`PONG` and a ten-second heartbeat overlap Horus. The SDK also
  tracks a thirty-second stale-PONG interval; Horus does not implement that
  watchdog or automatic reconnect. This is an implementation boundary, not an
  observed transport failure in a recording.

The old [py-clob-client](https://github.com/Polymarket/py-clob-client) is archived;
its README explicitly says it is no longer functional and points to `py-sdk`.
Do not introduce the retired client. The maintained SDK is a future migration
candidate, but adopting its larger public/account/trading surface and different
validation behavior is not necessary for this small smoke procedure. Existing
httpx/websockets adapters are retained; no dependency or protocol logic changes
are made without live compatibility evidence and a measured migration benefit.

## Explicitly opt in

From a clean checkout with Python 3.12 and locked dependencies installed:

```sh
uv sync --locked --all-groups
uv run --offline python -m examples.public_api_smoke \
  --live --revision "$(git rev-parse HEAD)" --duration 10 \
  --output /tmp/horus-public-smoke-private
```

`--offline` above prevents uv dependency downloads; **`--live` explicitly permits
the application's public network reads**. Without `--live`, no request or output
directory is created. No login, private key, account endpoint, order operation,
regional bypass or certificate override is used. This command is not added to
normal CI. CI exercises only mocked public responses and child processes.

The output must be a new directory outside the repository. It is mode 0700;
files are finalized as 0600. Existing paths are refused. A partial run is retained
for diagnosis, not overwritten on retry. `--revision` must be the full source
commit being executed; record actual dirty-overlay details separately if a
clean checkout is not used. The manifest also hashes the executed collector,
adapters, smoke module and lockfile rather than relying only on the commit label.

### Bounds and selection

- Discovery reads **one** Gamma `/markets` page (`active=true`, `closed=false`,
  limit 50, offset 0), with no pagination or retries. Eligible records must parse
  through Horus and have ACTIVE status. Malformed records are recorded privately
  as rejected, not silently interpreted as successful compatibility.
- Select the first three **distinct condition IDs** in response order. Record
  outcomes YES, NO, YES, including their mapped token IDs privately. Fewer than
  three eligible markets is an explicit selection blocker; do not fabricate IDs.
- Probe CLOB `/book` for each selection using the existing parser. Results and raw
  replies stay private. Snapshot parsing is not proof of WebSocket compatibility;
  an unsuccessful probe remains in the manifest even if collection later works.
- Discovery/probes run in a child with a **15-second OS wall limit**, fixed
  five-second HTTP operation timeouts and a two-MiB decoded response ceiling.
- Each collector child gets the requested duration (integer 1–30 seconds) plus
  **ten seconds** for startup/finalization. The parent kills and reaps a child
  that exceeds its wall limit, including one stuck in SQLite/asyncio. There are
  exactly three session slots and no reconnect or retry loop. Default child
  budgets total at most 75 seconds, excluding local startup/hash/file overhead.
- Process timeout/failure, empty data, mismatched identities and unavailable
  markets are not counted as recordings. A recorded session requires a successful
  collector exit, matching finalized nonempty summary and an actual journal file.
  Early `stream_ended` is retained with its actual termination and timestamps; it
  is not described as a full requested-duration capture.

## Evidence and publishing

`manifest.json` holds commit/config/source and dependency versions, actual UTC
process times, requested and wall-limit durations, selected market/token IDs,
collector terminal summaries, snapshot-probe outcomes and artifact SHA256/size
entries. Raw Gamma/book replies, SQLite files (including partial files and
sidecars) and child stdout/stderr stay in the same private directory. The
manifest excludes its own checksum; it is provenance, not a signed attestation.

Only stdout and `sanitized-summary.json` are intended for publication: requested,
recorded, failed and not-started counts, requested duration, revision and discovery
exit/timeout status. They contain no market/token IDs, raw errors or recordings.
Exit 0 requires three recorded sessions; other outcomes return exit 1 with a
summary, not success. Inspect the private manifest to diagnose access, schema or
transport issues. Publish only the sanitized summary unless separately authorized.

No raw live artifacts are committed or uploaded to an issue. The dated actual
run summary and blockers belong in the PR's acceptance evidence. Source-level
overlap and mocked scenarios must never be presented as three successful live
captures. Reconnect/watchdog work should be prioritized only after actual
recordings demonstrate a relevant reliability failure; no such failure is
established by this source review or documentation certificate error alone.
