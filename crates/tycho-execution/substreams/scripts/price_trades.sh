#!/usr/bin/env bash
# Prices newly ingested trades in a loop (alternative to pg_cron for local runs).
#   DSN=postgres://tycho:tycho@localhost:5433/router_trades ./scripts/price_trades.sh [interval_s]
set -euo pipefail

DSN="${DSN:?set DSN to a libpq connection string}"
INTERVAL="${1:-60}"
MAX_AGE="${MAX_AGE:-1 hour}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
SQL="$DIR/pricing/price_trades.sql"

while true; do
  psql "$DSN" -q -v ON_ERROR_STOP=1 -v max_age="$MAX_AGE" -f "$SQL"
  sleep "$INTERVAL"
done
