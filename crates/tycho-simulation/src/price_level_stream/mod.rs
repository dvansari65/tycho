//! Titan pAMM price level stream integration.
//!
//! Titan Builder exposes a WebSocket stream of complete per-pair quote snapshots (simulated and
//! interpolated price levels) for a subset of its supported pAMMs (see
//! <https://docs.titanbuilder.xyz/propamms/takers#pamm-price-level>). This module turns those
//! snapshots directly into [`Update`](crate::protocol::models::Update)s ready for consumption.
//!
//! Quotes target the block currently being built, so every emitted update is marked as partial
//! and supersedes the previous one for the pairs it contains.
//!
//! Components produced here are identified as `pricelevelstream:{pamm}`, where `{pamm}` is the
//! configured venue name (e.g. `pricelevelstream:fermiswap`) or, for auto-detected venues, the
//! venue address (e.g. `pricelevelstream:0x5979…`). The prefix keeps these components distinct
//! from those any other integration path may produce for the same venue (e.g. `vm:fermiswap`).
//!
//! Opting in with `via_fallback_router` (requires the `evm` feature) emits whitelisted venues
//! under `propammfallback:{pamm}` instead: tycho-execution routes their swaps through Titan's
//! PropAMMRouter, which falls back to a single-hop Uniswap V3 pool when the venue reverts. The
//! builder reads the router's on-chain whitelist via the node at `RPC_URL`; without the
//! variable, or when the read fails, it warns and keeps every venue on the direct path.
//!
//! Distinct identifiers do not imply distinct liquidity, though: a venue served here may also be
//! integrated through another path (FermiSwap, for example, also exists as `vm:fermiswap`), in
//! which case the components of both paths price the same underlying inventory. Consumers
//! subscribing to multiple paths must expect such overlaps and deduplicate by venue — e.g. via
//! the [`PAMM_ADDRESS_ATTRIBUTE`](stream::PAMM_ADDRESS_ATTRIBUTE) — wherever double-counting
//! matters, such as routing over the combined liquidity.
//!
//! Entry point: [`PriceLevelStreamBuilder`](stream::PriceLevelStreamBuilder). Register the pAMMs
//! to serve — the known venues via
//! [`with_known_pamms`](stream::PriceLevelStreamBuilder::with_known_pamms), individual
//! [`PriceLevelStreamConfig`](config::PriceLevelStreamConfig)s via
//! [`add_pamm`](stream::PriceLevelStreamBuilder::add_pamm), or any streamed venue via
//! auto-detection — provide token metadata, and consume the resulting stream of
//! [`Update`](crate::protocol::models::Update)s.

pub mod config;
#[cfg(feature = "evm")]
pub mod fallback_router;
pub mod state;
pub mod stream;
mod titan;
