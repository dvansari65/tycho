-- Fixtures and assertions for the views in views.sql.
--
-- Run through scripts/test_views.sh, which applies schema.sql and views.sql first.
BEGIN;

-- A minimal trade. Only the columns the view reads carry meaning.
CREATE OR REPLACE FUNCTION add_trade(
    p_id TEXT, p_tx TEXT, p_call_index INTEGER, p_in TEXT, p_out TEXT, p_amount_in NUMERIC,
    p_tx_success BOOLEAN DEFAULT TRUE, p_call_success BOOLEAN DEFAULT TRUE,
    p_state_committed BOOLEAN DEFAULT NULL
) RETURNS VOID AS $$
    INSERT INTO trades (
        id, chain, block_number, block_time, tx_hash, tx_index, call_index, tx_success,
        call_success, router, router_version, strategy, funding, eoa, msg_sender, receiver,
        token_in, token_out, amount_in, min_amount_out, native_value, gas_used, n_tokens, n_hops,
        executors, wrap_eth, unwrap_eth, state_committed
    ) VALUES (
        p_id, 'robinhood', 1, '2026-09-01T00:00:00Z', p_tx, 0, p_call_index, p_tx_success,
        p_call_success, '0xr', 'v3_1', 'sequential', 'transfer_from', '0xe', '0xm', '0xr2',
        p_in, p_out, p_amount_in, 0, 0, 0, 2, 1, '{}', FALSE, FALSE, p_state_committed
    );
$$ LANGUAGE sql;

DO $$
BEGIN
    -- A call the chain kept, and one it threw away.
    PERFORM add_trade('committed', '0xaa', 10, '0xin', '0xout', 100, TRUE, TRUE, TRUE);
    PERFORM add_trade('discarded', '0xbb', 10, '0xin', '0xout', 100, TRUE, TRUE, FALSE);

    -- A probe and the fill its transaction went on to execute. Only the fill is kept, and the
    -- flag is what says so: the probe has the lower call index, not the higher one.
    PERFORM add_trade('probe', '0xcc', 96,  '0xin', '0xout', 100, TRUE, TRUE, FALSE);
    PERFORM add_trade('fill',  '0xcc', 201, '0xin', '0xout', 100, TRUE, TRUE, TRUE);

    -- A fill followed by a probe of the same swap, so the probe is the later call.
    PERFORM add_trade('fill-first',  '0xdd', 10, '0xin', '0xout', 100, TRUE, TRUE, TRUE);
    PERFORM add_trade('probe-after', '0xdd', 20, '0xin', '0xout', 100, TRUE, TRUE, FALSE);

    -- The same swap twice in one transaction, both kept by the chain. Both are trades: a route
    -- can legitimately hit one pair twice with the same input amount.
    PERFORM add_trade('twin-a', '0xee', 10, '0xin', '0xout', 100, TRUE, TRUE, TRUE);
    PERFORM add_trade('twin-b', '0xee', 20, '0xin', '0xout', 100, TRUE, TRUE, TRUE);

    -- A reverted call, and a failed transaction. Neither is a settled trade, and both carry the
    -- flag set here on purpose: a reverted call discards its own state and a failed transaction
    -- discards all of it, so the combination should not arise, but the view does not lean on
    -- that. Its three filters each stand on their own.
    PERFORM add_trade('reverted',  '0xff', 10, '0xin', '0xout', 100, TRUE,  FALSE, TRUE);
    PERFORM add_trade('tx-failed', '0xgg', 10, '0xin', '0xout', 100, FALSE, TRUE,  TRUE);

    -- A row indexed before the flag existed. Nothing in it says the chain kept the call, so it
    -- stays out until the chain is re-indexed.
    PERFORM add_trade('legacy', '0xhh', 10, '0xin', '0xout', 100, TRUE, TRUE, NULL);
END $$;

DO $$
DECLARE
    kept TEXT[];
BEGIN
    SELECT array_agg(id ORDER BY id) INTO kept FROM trades_settled;
    IF kept IS DISTINCT FROM ARRAY['committed', 'fill', 'fill-first', 'twin-a', 'twin-b'] THEN
        RAISE EXCEPTION 'trades_settled kept %', kept;
    END IF;

    -- The view carries every column of trades, so a caller can read volume straight off it.
    IF (SELECT count(*) FROM information_schema.columns WHERE table_name = 'trades_settled')
       IS DISTINCT FROM (SELECT count(*) FROM information_schema.columns WHERE table_name = 'trades')
    THEN
        RAISE EXCEPTION 'trades_settled does not carry every column of trades';
    END IF;
END $$;

DROP FUNCTION add_trade(TEXT, TEXT, INTEGER, TEXT, TEXT, NUMERIC, BOOLEAN, BOOLEAN, BOOLEAN);
COMMIT;
