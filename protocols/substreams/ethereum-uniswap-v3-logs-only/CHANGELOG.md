# Changelog

## v0.1.4

- Take `protocol_type_name` as a `map_pools_created` parameter instead of hardcoding
  `uniswap_v3_pool`, so a fork indexed by this package emits its own protocol type. Every manifest
  now passes its parameters as a query string.

## v0.1.3

- Add the Robinhood Chain Uniswap V3 logs-only manifest.
- Remove a redundant reference in a store-key format argument.
