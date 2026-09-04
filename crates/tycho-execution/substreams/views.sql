-- Views derived from `trades`.
--
-- Every sink and the pricer apply this file on start, so it has to be idempotent and it has to
-- tolerate a database whose sinks have not created `trades` yet.
--
--   psql "$DSN" -f views.sql
BEGIN;

-- The same lock executors.sql takes, so the containers wait for each other instead of racing on
-- the same DDL.
DO $$ BEGIN PERFORM pg_advisory_xact_lock(6045170023); END $$;

DO $$
BEGIN
    IF to_regclass('public.trades') IS NULL THEN
        RAISE NOTICE 'trades table does not exist yet, skipping the views';
        RETURN;
    END IF;

    -- Set by the package since the release that reads `state_reverted` on the call. An existing
    -- database was created before it, and `schema.sql` only creates a table it does not find,
    -- so the column is added here.
    ALTER TABLE trades ADD COLUMN IF NOT EXISTS state_committed BOOLEAN;
    ALTER TABLE router_call_errors ADD COLUMN IF NOT EXISTS state_committed BOOLEAN;

    -- Looking up the trades of one transaction by hash, which no other index serves.
    CREATE INDEX IF NOT EXISTS trades_tx_idx ON trades (chain, tx_hash);

    -- The trades to sum volume over: one row per swap the chain actually kept.
    --
    -- A caller can quote the router on chain by calling it and reverting, and only then execute
    -- the route it liked. The router call of such a probe returns normally, so it carries
    -- tx_success and call_success like any fill and the package stores it as a trade. On
    -- robinhood about 9% of the stored trades are probes, and they hold 15.6% of the USD volume.
    --
    -- `state_committed` separates the two exactly: the chain threw away the state changes of a
    -- probe. A row indexed before that column existed carries NULL and stays out, because
    -- nothing in it says the call was kept; re-indexing the chain settles it either way.
    DROP VIEW IF EXISTS trades_settled;
    CREATE VIEW trades_settled AS
    SELECT t.*
    FROM trades t
    WHERE t.tx_success
      AND t.call_success
      AND t.state_committed;
END $$;

COMMIT;
