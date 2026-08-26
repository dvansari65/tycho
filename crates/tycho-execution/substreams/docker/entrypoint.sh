#!/usr/bin/env bash
# Runs one sink (per chain) or the pricing loop.
#
#   entrypoint sink   CHAIN, DSN, SUBSTREAMS_API_TOKEN; optional SUBSTREAMS_ENDPOINT,
#                     START_BLOCK, STOP_BLOCK, FLUSH_INTERVAL (default 100), METRICS_ADDR
#   entrypoint price  DSN; optional MAX_AGE (default "1 hour"), INTERVAL (default 60)
#
# `sink` applies schema.sql with `substreams-sink-sql setup` first; the command is idempotent.
set -euo pipefail

mode="${1:-sink}"
case "$mode" in
  sink)
    : "${CHAIN:?set CHAIN (ethereum, base, ...)}"
    : "${DSN:?set DSN, e.g. psql://user:pass@host:5432/db?sslmode=disable}"
    : "${SUBSTREAMS_API_TOKEN:?set SUBSTREAMS_API_TOKEN}"
    spkg="/opt/router-trades/spkg/${CHAIN}.spkg"
    [ -f "$spkg" ] || { echo "no package for chain '$CHAIN'" >&2; exit 1; }
    args=("$DSN" "$spkg")
    [ -n "${START_BLOCK:-}${STOP_BLOCK:-}" ] && args+=("${START_BLOCK:-}:${STOP_BLOCK:-}")
    [ -n "${SUBSTREAMS_ENDPOINT:-}" ] && args+=(-e "$SUBSTREAMS_ENDPOINT")
    substreams-sink-sql setup "$DSN" "$spkg"
    exec substreams-sink-sql run "${args[@]}" \
      --batch-block-flush-interval "${FLUSH_INTERVAL:-100}" \
      --metrics-listen-addr "${METRICS_ADDR:-:9102}"
    ;;
  price)
    : "${DSN:?set DSN}"
    exec /opt/router-trades/scripts/price_trades.sh "${INTERVAL:-60}"
    ;;
  *)
    exec "$@"
    ;;
esac
