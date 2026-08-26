# TychoRouter trades substreams

Substreams package that extracts every trade routed through the deployed TychoRouter contracts
and sinks it into Postgres with [`substreams-sink-sql`](https://github.com/streamingfast/substreams-sink-sql).
It is a standalone WASM workspace (like `protocols/substreams/`) and is excluded from the root
Cargo workspace.

## What it extracts

The router emits no swap event, so trades are recovered from **EVM call traces**: every call
(top-level or internal, successful or reverted) whose target is a configured router and whose
selector is one of the swap entry points. This requires Extended (Firehose-instrumented) blocks.

Per trade (`trades` table): router + ABI generation, method (`single`/`sequential`/`split`),
funding mode (`transfer_from`/`permit2`/`vault`/`none`), EOA (`tx.from`), `msg.sender`, receiver,
token in/out, amount in, expected/min amount out and the implied slippage tolerance, settled
amount out (decoded from the call return value), gross amount out and positive slippage,
`ClientFeeParams`, the router fee configuration in effect, fees actually taken (`FeesTaken`),
executors and protocols per hop, splits, watermark (trailing calldata), revert selector + decoded
reason, gas used.

Related tables: `trade_hops` (one row per executor hop), `fees_taken` (one row per fee
recipient), `fee_config_events` (FeeCalculator admin events and fee-calculator rotations).

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
the current ETH price of (almost) every token in `token_price`, overwritten roughly hourly.
`pricing/price_trades.sql` stamps each *fresh* trade (`block_time > now() - 1 hour`,
configurable) with the current `price_in_eth` / `price_out_eth`, token decimals and the resulting
`volume_eth`. Older unpriced rows stay `NULL` rather than getting a wrong price, so a lagging sink
or a backfill never pollutes the data.

The trades live in their own database (all chains in one table). Each chain has its own Tycho
database, reached through `postgres_fdw`: one server and one `tycho_<chain>` schema per chain,
unioned by the `current_token_prices` view.

```bash
for chain in ethereum base ...; do                              # once per chain
  psql "$DSN" -v chain=$chain -v tycho_host=... -v tycho_port=5432 -v tycho_db=... \
       -v tycho_user=... -v tycho_password=... -f pricing/tycho_foreign_tables.sql
done
make price-setup       # (re)builds current_token_prices over all tycho_<chain> schemas
make price-once        # one pass; MAX_AGE='2 hours' make price-once widens the window
make price-loop        # every 60 s; or psql -f pricing/schedule_pg_cron.sql if pg_cron exists
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

`sink` runs `substreams-sink-sql setup` (idempotent) and then `run` with
`--batch-block-flush-interval 100`; optional `SUBSTREAMS_ENDPOINT`, `START_BLOCK`, `STOP_BLOCK`,
`FLUSH_INTERVAL`, `METRICS_ADDR` (Prometheus, default `:9102`). `docker-compose.yaml` wires the
database, the eight sinks and the pricer for a local run (`docker compose --profile sinks up`).

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

Run one `make sink` per chain against the same database; the `chain` column is part of every
primary key and params make each chain's module hash (and therefore sink cursor) distinct.

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
```

Lookup tables (`src/executors_table.rs`, `src/decode/error_table.rs`) are generated by
`scripts/gen_tables.py` from the docs/config git history and the ABIs under `abi/`; re-run it
after new executor deployments. ABIs come from the verified sources on Sourcify
(`TychoRouterV2` = `0xfD0b31d2…`, `TychoRouterV3_0` = `0x1f8dB310…`, `TychoRouterV3_1` =
`0xea290cE3…`, `FeeCalculator` = `0xA236E1F0…`); `FeeCalculatorV3_0.json` holds the `uint16`
event signatures of the earlier FeeCalculator.

Regenerate protobuf bindings after editing `proto/` with `make protogen`. The abigen output under
`src/abi/` is not committed; `build.rs` regenerates it from the minified JSON ABIs on every build.
