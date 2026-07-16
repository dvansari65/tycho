use std::str::FromStr;

use num_bigint::BigUint;
use tycho_common::Bytes;

/// Protocol system prefix identifying components sourced from the pAMM price level stream.
///
/// The full protocol system of a component is `pricelevelstream:{pamm}`, where `{pamm}` is the
/// configured venue name (e.g. `pricelevelstream:fermiswap`) or, for auto-detected venues, the
/// venue address (e.g. `pricelevelstream:0x5979…`); see the [module documentation](super) for
/// details.
pub const PRICE_LEVEL_STREAM_PREFIX: &str = "pricelevelstream";

/// Configuration of a single pAMM to be served from the price level stream.
#[derive(Debug, Clone)]
pub struct PriceLevelStreamConfig {
    /// Bare pAMM name (e.g. `fermiswap`); the emitted components carry
    /// `pricelevelstream:{protocol}` as their protocol system.
    pub protocol: String,
    /// The pAMM venue address under which Titan streams its quotes.
    pub address: Bytes,
    /// Constant per-swap gas cost estimate reported for every quote of this pAMM.
    pub gas_cost: BigUint,
}

impl PriceLevelStreamConfig {
    pub fn new(protocol: impl Into<String>, address: Bytes, gas_cost: BigUint) -> Self {
        Self { protocol: protocol.into(), address, gas_cost }
    }

    /// The configuration an auto-detected pAMM (streamed by Titan but not otherwise configured)
    /// is served under: named by its full lowercase hex address, with the default gas cost.
    pub(super) fn auto_detected(address: Bytes) -> Self {
        let protocol = address.to_string();
        Self::new(protocol, address, BigUint::from(DEFAULT_GAS_COST))
    }

    /// The protocol system identifier of components emitted for this pAMM.
    pub fn protocol_system(&self) -> String {
        format!("{PRICE_LEVEL_STREAM_PREFIX}:{}", self.protocol)
    }
}

/// Per-swap gas estimate for auto-detected pAMMs whose venue has not been measured: the midpoint
/// of the known venue profiles (see [`default_pamms`]), which span roughly 165k–341k.
const DEFAULT_GAS_COST: u64 = 260_000;

/// The pAMMs known to be served by the Titan price level stream (as of 2026-07): FermiSwap and
/// Kipseli.
///
/// Registered on a builder via
/// [`with_default_pamms`](super::stream::PriceLevelStreamBuilder::with_default_pamms), so their
/// components carry the venue name instead of the raw address; an
/// [`add_pamm`](super::stream::PriceLevelStreamBuilder::add_pamm) call for one of these
/// addresses overrides the corresponding entry.
///
/// Only the venues' router addresses are registered — the keys the price level stream has been
/// observed to use — because the streamed key doubles as the execution target
/// ([`PAMM_ADDRESS_ATTRIBUTE`](super::stream::PAMM_ADDRESS_ATTRIBUTE)): unlike the state-override
/// stream, which also publishes frames under non-executable oracle aliases, an entry here must
/// be an address a swap can be sent to.
pub fn default_pamms() -> Vec<PriceLevelStreamConfig> {
    // The venues' `IPropAMM::swap` gas, calibrated from real fills (2026-07-15) by replaying
    // them on the live venues at their fill blocks via `debug_traceCall`: ~162k-167k (FermiSwap)
    // and ~339k-343k (Kipseli), plus a small headroom. Deliberately excludes router-level
    // overhead (user/input/fee transfers): tycho-execution's gas estimator accounts for those on
    // top of this per-swap value.
    let pamms = [
        // The FermiSwapper router.
        ("fermiswap", "0x5979458912f80b96d30d4220af8e2e4925a33320", 170_000u64),
        // The KipseliPropAMMWrapper router.
        ("kipseli", "0x71e790dd841c8a9061487cb3e78c288e75ce0b3d", 350_000u64),
    ];
    pamms
        .into_iter()
        .map(|(protocol, address, gas_cost)| {
            PriceLevelStreamConfig::new(
                protocol,
                Bytes::from_str(address).expect("hardcoded pAMM address must parse"),
                BigUint::from(gas_cost),
            )
        })
        .collect()
}
