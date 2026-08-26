-- (Re)builds the `current_token_prices` view as the union of every `tycho_<chain>` schema:
-- foreign tables created by tycho_foreign_tables.sql, or the stand-ins from dev_stub.sql.
-- Re-run after adding a chain.
--
-- token_price.price is the price of one whole token in ETH (WETH = 1.0) and is overwritten
-- roughly hourly by the Tycho pricing job; modified_ts tells how fresh a row is.
DO $$
DECLARE
    schema_name text;
    parts text[] := '{}';
BEGIN
    FOR schema_name IN
        SELECT nspname FROM pg_namespace WHERE nspname LIKE 'tycho\_%' ESCAPE '\' ORDER BY nspname
    LOOP
        parts := parts || format(
            $sql$SELECT %L AS chain, '0x' || encode(a.address, 'hex') AS token,
                    t.decimals AS decimals, tp.price AS price_eth, tp.modified_ts AS updated_at
             FROM %I.token_price tp
             JOIN %I.token t ON t.id = tp.token_id
             JOIN %I.account a ON a.id = t.account_id$sql$,
            substr(schema_name, length('tycho_') + 1), schema_name, schema_name, schema_name);
    END LOOP;
    IF array_length(parts, 1) IS NULL THEN
        RAISE EXCEPTION 'no tycho_<chain> schema found; run tycho_foreign_tables.sql first';
    END IF;
    EXECUTE 'CREATE OR REPLACE VIEW current_token_prices AS ' || array_to_string(parts, ' UNION ALL ');
END $$;
