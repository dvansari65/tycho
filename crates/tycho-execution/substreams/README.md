# TychoRouter trades substreams

Substreams package that extracts every trade routed through the deployed TychoRouter contracts
and sinks it into Postgres with [`substreams-sink-sql`](https://github.com/streamingfast/substreams-sink-sql).
It is a standalone WASM workspace (like `protocols/substreams/`) and is excluded from the root
Cargo workspace.

## What it extracts

The router emits no swap event, so trades are recovered from **EVM call traces**: every call
(top-level or internal, successful or reverted) whose target is a configured router and whose
selector is one of the swap entry points. This requires Extended (Firehose-instrumented) blocks.

Per trade (`trades` table): router + ABI generation, strategy (`single`/`sequential`/`split`),
funding mode (`transfer_from`/`permit2`/`vault`/`none`), EOA (`tx.from`), `msg.sender`, receiver,
token in/out, amount in, expected/min amount out and the implied slippage tolerance, settled
amount out (decoded from the call return value), gross amount out and positive slippage,
`ClientFeeParams`, the router fee configuration in effect, fees actually taken (`FeesTaken`),
executors and protocol systems per hop, splits, watermark (trailing calldata), revert selector,
decoded reason, and gas used.

Related tables: `trade_hops` (one row per executor hop), `fees_taken` (one row per fee
recipient), `fee_config_events` (FeeCalculator admin events and fee-calculator rotations), and
`router_call_errors` (selector-matched calls that could not be decoded safely).

### Router generations

| Version | ABI | Deployments |
|---|---|---|
| `v2` | `singleSwap(amountIn, tokenIn, tokenOut, minAmountOut, wrapEth, unwrapEth, receiver, transferFromNeeded, swap)` (+ `Permit2`, `sequentialSwap`, `splitSwap`) | ethereum, base, unichain (2025) |
| `v3_0` | `ClientFeeParams` (`uint16` bps), `minAmountOut` only, `UsingVault` variants | all chains, Mar–Jul 2026 |
| `v3_1` | adds `expectedAmountOut`; `ClientFeeParams` uses `uint32` fee units (`MAX_BPS = 100_000_000`) | all chains, from Jul 31 2026 |

Addresses and the FeeCalculator each router was constructed with live in the per-chain
manifests under `tycho-router-trades/chains/`. The FeeCalculator constructor emits no event, so
the initial pairing is a parameter; rotations (`FeeCalculatorActivated` / `FeeCalculatorUpdated`)
and all fee-rate changes are replayed from events into `store_fee_config`.

### Volume in ETH

Pricing is a post-ingestion step, not part of the substreams. The Tycho indexer database keeps
the raw token units equivalent to 1 ETH in `token_price`, overwritten roughly hourly. Pricing
converts that value into ETH per whole token using the token decimals.
`pricing/price_trades.sql` stamps each *fresh* trade (`block_time > now() - 1 hour`,
configurable) with the current `price_in_eth` / `price_out_eth`, token decimals and the resulting
`volume_eth`. Older unpriced rows stay `NULL` rather than getting a wrong price, so a lagging sink
or a backfill never pollutes the data.

The trades live in their own database (all chains in one table). Each chain has its own Tycho
database, reached through `postgres_fdw`: one server and one `tycho_<chain>` schema per chain,
priced in independent passes. An unavailable source pauses only its chain; other chains continue.

```bash
for chain in ethereum base ...; do                              # once per chain
  psql "$DSN" -v chain=$chain -v tycho_host=... -v tycho_port=5432 -v tycho_db=... \
       -v tycho_user=... -v tycho_password=... -f pricing/tycho_foreign_tables.sql
done
make price-once CHAIN=ethereum  # one chain; MAX_AGE='2 hours' widens the window
make price-loop                 # every chain, independently, every 60 seconds
```

For the local docker database, `psql "$DSN" -v chain=ethereum -f pricing/dev_stub.sql` creates
stand-in tables in `tycho_ethereum` instead of the foreign tables.

## Container

`Dockerfile` (build from the repository root) compiles the wasm, packs one `.spkg` per chain and
ships them with `substreams-sink-sql` and `psql`. The image has two modes:

```bash
docker build -f crates/tycho-execution/substreams/Dockerfile -t tycho-router-trades .
# one container per chain, all writing to the same database
docker run -e CHAIN=ethereum -e DSN='psql://...' -e SUBSTREAMS_API_TOKEN=... tycho-router-trades sink
# one container running the pricing loop
docker run -e DSN='postgres://...' tycho-router-trades price
```

Both modes wait until the database accepts connections. `sink` runs `substreams-sink-sql setup`
(idempotent) and then `run` with `--batch-block-flush-interval 100`; optional
`SUBSTREAMS_ENDPOINT`, `START_BLOCK`, `STOP_BLOCK`, `FLUSH_INTERVAL`, `METRICS_ADDR` (Prometheus,
default `:9102`). `price` registers one `postgres_fdw` server per `TYCHO_<CHAIN>_DATABASE_URL`
variable (`scripts/fdw_setup.sh`, re-run on every start). It runs `price_trades.sql` separately
for each chain every `INTERVAL` seconds (default 60,
`MAX_AGE` default `1 hour`). A failed chain is logged without blocking the others.
`docker-compose.yaml` wires the database, the eight sinks and the pricer for a local run
(`docker compose --profile sinks up`).

### Kubernetes

`main-workflow.yaml` builds the image as `tycho-router-trades:<release version>` on every
monorepo release and promotes the tag to `helmwave/dev/versions.yml` in `helm-configuration`. The
release `router-trades-db` there (namespace `dev-tycho`) is a Postgres 16 StatefulSet; release
`router-trades` is one pod with a `sink` container per chain and one `price` container, all
writing to `router-trades-db:5432`. Grafana reaches the database through the same service with
the read-only `grafana` role.

## Updating a deployed sink

Two properties of `substreams-sink-sql` decide what an update costs, and both bite silently.

**The cursor is keyed by the output module hash.** That hash covers every module definition —
including `initialBlock` and `params` — and the compiled wasm. The sinks run with the default
`--on-module-hash-mismatch=error`, so when the hash changes the container exits at startup with a
mismatch against the row in `cursors_<chain>`, and it keeps crashlooping until that row is
deleted. What changed decides the blast radius:

| change | new hash for |
|---|---|
| one chain's manifest (`initialBlock`, router `params`) | that chain only |
| anything under `src/`, `Cargo.lock`, the wasm build | **every chain** |

A local `cargo build` is not byte-identical to the Docker build, so production hashes cannot be
precomputed on a laptop; CI builds of the same source are stable, so an image rebuild without a
source change keeps every cursor valid.

**The sink does not upsert.** `db_out` uses `create_row`, which is `OPERATION_CREATE`, and the
postgres dialect turns that into a plain `INSERT` with no `ON CONFLICT`. Any block the sink reads
a second time fails on the `TEXT PRIMARY KEY` and takes the container down. So whenever a chain
restarts below its previous cursor, delete that chain's rows in the range that will be re-read —
in practice, all of them.

### Procedure

1. Merge the change and let the release build the image; `promote-to-dev` writes the tag into
   `helmwave/dev/versions.yml` and the dev deployment rolls the pod.
2. Chains whose hash changed exit on the mismatch; the others resume from their cursors. This is
   expected, and it is contained: the sinks have no readiness probe and Postgres is a separate
   release.
3. Clear the state of each affected chain, now that its sink is down and not flushing:

   ```sql
   DELETE FROM cursors_<chain>;
   DELETE FROM substreams_history_<chain>;
   -- only when the chain will re-read blocks it has already written
   DELETE FROM trades             WHERE chain = '<chain>';
   DELETE FROM trade_hops         WHERE chain = '<chain>';
   DELETE FROM fees_taken         WHERE chain = '<chain>';
   DELETE FROM fee_config_events  WHERE chain = '<chain>';
   DELETE FROM router_call_errors WHERE chain = '<chain>';
   ```

4. The container recovers on its next backoff restart. Confirm the new range in its log:
   `restarting_at: None` together with the expected `resolved_start_block`.

Never reach for `--on-module-hash-mismatch=ignore` to skip step 3: it resumes from the highest
cursor in the table, which is block 0 for a chain that has never produced data.

`cursors_<chain>` holds one bookmark row and `substreams_history_<chain>` is the reorg-undo
journal. Neither holds trade data, so clearing them loses no trade.

## Module graph

```
map_fee_config_events ─▶ store_fee_config ─▶ map_trades ─▶ db_out (DatabaseChanges)
        └──────────────────────────────────────────────────▶
```

## Running locally

```bash
rustup target add wasm32-unknown-unknown
brew install streamingfast/tap/substreams            # or see substreams.dev
# substreams-sink-sql: download the release binary for your platform from
# https://github.com/streamingfast/substreams-sink-sql/releases (tested with v4.13.1)
export SUBSTREAMS_API_TOKEN=$(curl -s https://auth.streamingfast.io/v1/auth/issue \
  -d "{\"api_key\":\"$STREAMINGFAST_KEY\"}" | jq -r .token)

docker compose up -d
make setup CHAIN=ethereum                              # applies schema.sql (idempotent)
make run   CHAIN=ethereum START=25654569 STOP=+50      # print decoded trades
make sink  CHAIN=ethereum                              # stream into Postgres
```

Run one `make sink` per chain against the same database. The `chain` column is part of every
business-table primary key. Deployed sinks use per-chain cursor and history tables so their block
positions and reorg bookkeeping remain independent.

`make sink` passes `--batch-block-flush-interval 100`: with the default (1000) the sink
v4.13.1 did not flush the trailing partial batch when a bounded run reached its stop block.
Live mode flushes every block regardless.

Backfilling from the router deployment blocks needs `substreams-sink-sql`'s production mode
(the `substreams run` CLI caps ad-hoc requests at 10k blocks because of the fee store).

Endpoints are resolved from the manifest `network` field; override with
`-e <host>:443` or `SUBSTREAMS_ENDPOINTS_CONFIG_<NETWORK>`.

## Development

```bash
make test    # unit tests (decoding fixtures from contracts/test/assets/calldata.txt)
make lint    # fmt + clippy
make build   # wasm
make test-pricing  # disposable two-chain PostgreSQL/FDW integration test
./scripts/test_pricing.sh --outage-check  # verifies one failed source does not block another
```

Lookup tables (`src/executors_table.rs`, `src/decode/error_table.rs`) are generated by
`scripts/gen_tables.py` from the docs/config git history and the ABIs under `abi/`; re-run it
after new executor deployments. ABIs come from the verified sources on Sourcify
(`TychoRouterV2` = `0xfD0b31d2…`, `TychoRouterV3_0` = `0x1f8dB310…`, `TychoRouterV3_1` =
`0xea290cE3…`, `FeeCalculator` = `0xA236E1F0…`); `FeeCalculatorV3_0.json` holds the `uint16`
event signatures of the earlier FeeCalculator.

Regenerate protobuf bindings after editing `proto/` with `make protogen`. The abigen output under
`src/abi/` is not committed; `build.rs` regenerates it from the minified JSON ABIs on every build.
