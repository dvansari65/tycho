-- Prices freshly ingested trades with the *current* token prices. Only rows younger than
-- :max_age are touched, so a lagging or backfilling sink never gets today's price stamped on
-- old trades; those stay NULL for a later historical pass.
--
-- Native ETH (0xeeee…) is priced at 1.0 with 18 decimals; WETH comes from the price table.
--
-- Usage: psql "$DSN" -v max_age='1 hour' -f pricing/price_trades.sql
\set max_age_default '1 hour'
\if :{?max_age}
\else
\set max_age :max_age_default
\endif

WITH prices AS (
    SELECT chain, token, decimals, price_eth FROM current_token_prices
    UNION ALL
    SELECT DISTINCT chain, '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 18, 1.0
    FROM current_token_prices
),
priced AS (
    SELECT
        t.id,
        p_in.price_eth   AS price_in_eth,
        p_out.price_eth  AS price_out_eth,
        p_in.decimals    AS decimals_in,
        p_out.decimals   AS decimals_out,
        COALESCE(
            t.amount_in  / power(10::numeric, p_in.decimals)  * p_in.price_eth,
            t.amount_out / power(10::numeric, p_out.decimals) * p_out.price_eth
        ) AS volume_eth
    FROM trades t
    LEFT JOIN prices p_in  ON p_in.chain  = t.chain AND p_in.token  = t.token_in
    LEFT JOIN prices p_out ON p_out.chain = t.chain AND p_out.token = t.token_out
    WHERE t.priced_at IS NULL
      AND t.block_time > now() - :'max_age'::interval
      AND (p_in.token IS NOT NULL OR p_out.token IS NOT NULL)
)
UPDATE trades t
SET price_in_eth  = priced.price_in_eth,
    price_out_eth = priced.price_out_eth,
    decimals_in   = priced.decimals_in,
    decimals_out  = priced.decimals_out,
    volume_eth    = priced.volume_eth,
    priced_at     = now()
FROM priced
WHERE t.id = priced.id;
