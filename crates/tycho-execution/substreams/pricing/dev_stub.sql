-- Minimal stand-in for one chain's Tycho tables used by current_token_prices, for local
-- development against the docker-compose database, in place of the foreign tables.
-- Insert rows into token_price / token / account yourself, then apply current_token_prices.sql.
--
--   psql "$DSN" -v chain=ethereum -f pricing/dev_stub.sql
\set schema 'tycho_' :chain
CREATE SCHEMA IF NOT EXISTS :schema;
SET search_path TO :schema;
CREATE TABLE IF NOT EXISTS account (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL DEFAULT 1,
    address BYTEA NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS token (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL UNIQUE REFERENCES account(id),
    symbol VARCHAR(255) NOT NULL,
    decimals INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS token_price (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL UNIQUE REFERENCES token(id),
    price DOUBLE PRECISION NOT NULL,
    modified_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
RESET search_path;
