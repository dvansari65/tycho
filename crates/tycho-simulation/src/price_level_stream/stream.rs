use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};

use chrono::Utc;
use tokio_stream::{Stream, StreamExt};
use tycho_common::{
    models::{token::Token, Chain},
    simulation::protocol_sim::ProtocolSim,
    Bytes,
};

use super::{
    config::{default_pamms, PriceLevelStreamConfig},
    state::{PriceLevelStreamQuote, PriceLevelStreamState},
    titan::{
        self, ConnectionSettings, TitanPairLevels, TitanPammLevels, TitanPriceLevel,
        TitanPriceLevelMessage, TITAN_PRICE_LEVEL_URL,
    },
};
use crate::protocol::models::{ProtocolComponent, Update};

/// Static attribute under which each emitted component carries its pAMM venue address.
pub const PAMM_ADDRESS_ATTRIBUTE: &str = "pamm_address";

/// Builds a stream of [`Update`]s from the Titan pAMM price level WebSocket.
///
/// A new builder serves no pAMMs: register the known venues via
/// [`with_default_pamms`](Self::with_default_pamms), individual ones via
/// [`add_pamm`](Self::add_pamm), or opt into serving unknown streamed venues via
/// [`auto_detect`](Self::auto_detect); [`with_tokens`](Self::with_tokens) provides the token
/// metadata pairs are interpreted with.
///
/// One component is emitted per (pAMM, token pair), identified by the concatenation
/// `pamm ++ token0 ++ token1` (tokens sorted ascending), under the protocol system
/// `pricelevelstream:{pamm}`. The venue address is exposed through the
/// [`PAMM_ADDRESS_ATTRIBUTE`] static attribute for downstream encoding.
#[derive(Default)]
pub struct PriceLevelStreamBuilder {
    registry: HashMap<Bytes, PriceLevelStreamConfig>,
    tokens: HashMap<Bytes, Token>,
    url: Option<String>,
    auto_detect: bool,
    connection: ConnectionSettings,
}

impl PriceLevelStreamBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables serving pAMMs that are not registered via
    /// [`with_default_pamms`](Self::with_default_pamms) or [`add_pamm`](Self::add_pamm)
    /// (disabled by default).
    ///
    /// When enabled, any unknown streamed venue is served under its full lowercase hex address
    /// as the name, with the default gas cost. A venue's protocol system therefore changes from
    /// the address form (`pricelevelstream:{0xaddress}`) to a name (`pricelevelstream:{name}`)
    /// once it gets registered — via [`add_pamm`](Self::add_pamm) or a release's
    /// [`default_pamms`] recognizing it; the name-independent identifiers — the component id and
    /// the [`PAMM_ADDRESS_ATTRIBUTE`] — stay stable across such renames.
    pub fn auto_detect(mut self, enabled: bool) -> Self {
        self.auto_detect = enabled;
        self
    }

    /// Overrides the stream endpoint, e.g. to connect to a closer Titan region than the default
    /// (see <https://docs.titanbuilder.xyz/propamms/takers>).
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Overrides how long a single connection attempt may take before it is aborted and retried
    /// (default: 10s), so a hung TCP/TLS handshake cannot block the stream forever.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connection.connect_timeout = timeout;
        self
    }

    /// Overrides the longest gap between Titan messages tolerated before the connection is
    /// treated as dead and re-established (default: 30s). Titan pushes several updates per
    /// second, so a multi-second silence means a stalled or half-open connection.
    pub fn read_idle_timeout(mut self, timeout: Duration) -> Self {
        self.connection.read_idle_timeout = timeout;
        self
    }

    /// Overrides the cap on the exponential reconnect backoff of `2^attempt` seconds
    /// (default: 32s).
    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.connection.max_backoff = max_backoff;
        self
    }

    /// Registers a pAMM to be served under the given configuration, overriding any default or
    /// auto-detected one for the same address.
    pub fn add_pamm(mut self, config: PriceLevelStreamConfig) -> Self {
        self.registry
            .insert(config.address.clone(), config);
        self
    }

    /// Registers the known venues ([`default_pamms`]) to be served. For an address also
    /// registered via [`add_pamm`](Self::add_pamm), that configuration wins regardless of call
    /// order.
    pub fn with_default_pamms(mut self) -> Self {
        for config in default_pamms() {
            self.registry
                .entry(config.address.clone())
                .or_insert(config);
        }
        self
    }

    /// Provides the token metadata used to build components and interpret amounts. Pairs whose
    /// tokens are missing here are skipped.
    pub fn with_tokens(mut self, tokens: HashMap<Bytes, Token>) -> Self {
        self.tokens = tokens;
        self
    }

    /// Consumes the builder and opens the stream.
    ///
    /// The connection is established lazily on first poll and maintained (with reconnects) for as
    /// long as the stream is polled; it never terminates on its own, and dropping the stream
    /// closes the connection. Frames that contain no served pAMM produce no update.
    ///
    /// Each streamed frame is a complete snapshot per pAMM, so every update carries the full set
    /// of that frame's pair states, with `new_pairs` / `removed_pairs` derived by diffing against
    /// the previously emitted snapshot of the same pAMM. Pairs whose tokens are missing from the
    /// provided token metadata are skipped.
    pub fn build(self) -> impl Stream<Item = Update> + Send {
        let url = self
            .url
            .clone()
            .unwrap_or_else(|| TITAN_PRICE_LEVEL_URL.to_string());
        let mut tracker = SnapshotTracker::new(self.registry, self.tokens, self.auto_detect);

        titan::messages(url, self.connection).filter_map(move |message| tracker.process(message))
    }
}

/// Turns Titan frames into [`Update`]s, tracking each pAMM's previously emitted components so
/// pair additions and removals can be diffed against the last snapshot.
struct SnapshotTracker {
    registry: HashMap<Bytes, PriceLevelStreamConfig>,
    tokens: HashMap<Bytes, Token>,
    /// Whether frames from pAMMs absent from the registry get an address-named configuration
    /// synthesized (and cached in the registry) instead of being skipped.
    auto_detect: bool,
    /// Components of the last emitted snapshot, per pAMM address. Kept per pAMM because a frame
    /// only has snapshot semantics for the pAMMs it contains.
    components: HashMap<Bytes, HashMap<String, ProtocolComponent>>,
}

impl SnapshotTracker {
    fn new(
        registry: HashMap<Bytes, PriceLevelStreamConfig>,
        tokens: HashMap<Bytes, Token>,
        auto_detect: bool,
    ) -> Self {
        Self { registry, tokens, auto_detect, components: HashMap::new() }
    }

    /// Processes one frame into an [`Update`], or `None` if the frame contains nothing relevant
    /// (no registered pAMM with at least one known pair or a pair removal).
    fn process(&mut self, message: TitanPriceLevelMessage) -> Option<Update> {
        let mut states: HashMap<String, Box<dyn ProtocolSim>> = HashMap::new();
        let mut new_pairs = HashMap::new();
        let mut removed_pairs = HashMap::new();

        for TitanPammLevels { pamm, pairs } in message.pamms {
            let config = match self.registry.entry(pamm.clone()) {
                Entry::Occupied(entry) => &*entry.into_mut(),
                Entry::Vacant(entry) => {
                    if !self.auto_detect {
                        tracing::debug!(%pamm, "Skipping unregistered pAMM");
                        continue;
                    }
                    tracing::info!(%pamm, "Serving auto-detected pAMM");
                    &*entry.insert(PriceLevelStreamConfig::auto_detected(pamm.clone()))
                }
            };

            // Merge the frame's per-direction ladders into one entry per unordered token pair.
            let mut merged_pairs: HashMap<(Bytes, Bytes), (Vec<_>, Vec<_>)> = HashMap::new();
            for TitanPairLevels { token_in, token_out, order_book } in pairs {
                if !self.tokens.contains_key(&token_in) || !self.tokens.contains_key(&token_out) {
                    tracing::debug!(%token_in, %token_out, "Skipping pair with unknown token");
                    continue;
                }
                let sells_token0 = token_in < token_out;
                let key = if sells_token0 {
                    (token_in.clone(), token_out.clone())
                } else {
                    (token_out.clone(), token_in.clone())
                };
                let quotes = order_book
                    .into_iter()
                    .map(|TitanPriceLevel { amount_in, amount_out }| {
                        PriceLevelStreamQuote::new(amount_in, amount_out)
                    })
                    .collect();
                let entry = merged_pairs.entry(key).or_default();
                if sells_token0 {
                    entry.0 = quotes;
                } else {
                    entry.1 = quotes;
                }
            }

            let mut previous = self
                .components
                .remove(&pamm)
                .unwrap_or_default();
            let mut current = HashMap::with_capacity(merged_pairs.len());
            for ((token0, token1), (quotes_0_to_1, quotes_1_to_0)) in merged_pairs {
                let id = component_id(&config.address, &token0, &token1);
                let id_string = id.to_string();
                let component = previous
                    .remove(&id_string)
                    .unwrap_or_else(|| {
                        let component = build_component(&self.tokens, config, id, &token0, &token1);
                        new_pairs.insert(id_string.clone(), component.clone());
                        component
                    });

                let state = PriceLevelStreamState::new(
                    token0,
                    token1,
                    quotes_0_to_1,
                    quotes_1_to_0,
                    config.gas_cost.clone(),
                );

                states.insert(id_string.clone(), Box::new(state));
                current.insert(id_string, component);
            }

            // This frame is the pAMM's complete snapshot, and every re-emitted pair was moved
            // into `current` above — whatever remains is gone.
            removed_pairs.extend(previous);
            self.components.insert(pamm, current);
        }

        if states.is_empty() && new_pairs.is_empty() && removed_pairs.is_empty() {
            return None;
        }

        Some(
            // Quotes target the block currently being built, hence partial. Sync states stay
            // empty (like the RFQ path) because no full block header is available.
            Update::new(message.block_number, states, new_pairs)
                .set_is_partial(true)
                .set_removed_pairs(removed_pairs),
        )
    }
}

fn build_component(
    tokens: &HashMap<Bytes, Token>,
    config: &PriceLevelStreamConfig,
    id: Bytes,
    token0: &Bytes,
    token1: &Bytes,
) -> ProtocolComponent {
    let protocol_system = config.protocol_system();
    ProtocolComponent::new(
        id,
        protocol_system.clone(),
        protocol_system,
        // Titan builds Ethereum L1 blocks; the stream carries no other chains.
        Chain::Ethereum,
        vec![tokens[token0].clone(), tokens[token1].clone()],
        vec![config.address.clone()],
        HashMap::from([(PAMM_ADDRESS_ATTRIBUTE.to_string(), config.address.clone())]),
        Bytes::default(),
        Utc::now().naive_utc(),
    )
}

/// The component identity of a (pAMM, pair) combination: `pamm ++ token0 ++ token1`.
fn component_id(pamm: &Bytes, token0: &Bytes, token1: &Bytes) -> Bytes {
    Bytes::from([pamm.as_ref(), token0.as_ref(), token1.as_ref()].concat())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use num_bigint::BigUint;

    use super::*;

    const PAMM: &str = "0x5979458912f80b96d30d4220af8e2e4925a33320";
    const WBTC: &str = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599";
    const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

    fn token(address: &str, symbol: &str, decimals: u32) -> Token {
        Token::new(
            &Bytes::from_str(address).unwrap(),
            symbol,
            decimals,
            0,
            &[Some(10_000)],
            Chain::Ethereum,
            100,
        )
    }

    fn tokens() -> HashMap<Bytes, Token> {
        [token(WBTC, "WBTC", 8), token(USDC, "USDC", 6), token(WETH, "WETH", 18)]
            .into_iter()
            .map(|token| (token.address.clone(), token))
            .collect()
    }

    fn tracker() -> SnapshotTracker {
        let config = PriceLevelStreamConfig::new(
            "fermiswap",
            Bytes::from_str(PAMM).unwrap(),
            BigUint::from(120_000u64),
        );
        SnapshotTracker::new(HashMap::from([(config.address.clone(), config)]), tokens(), false)
    }

    fn level(amount_in: u64, amount_out: u64) -> TitanPriceLevel {
        TitanPriceLevel {
            amount_in: BigUint::from(amount_in),
            amount_out: BigUint::from(amount_out),
        }
    }

    fn pair_levels(
        token_in: &str,
        token_out: &str,
        order_book: Vec<TitanPriceLevel>,
    ) -> TitanPairLevels {
        TitanPairLevels {
            token_in: Bytes::from_str(token_in).unwrap(),
            token_out: Bytes::from_str(token_out).unwrap(),
            order_book,
        }
    }

    fn message(block_number: u64, pairs: Vec<TitanPairLevels>) -> TitanPriceLevelMessage {
        TitanPriceLevelMessage {
            block_number,
            pamms: vec![TitanPammLevels { pamm: Bytes::from_str(PAMM).unwrap(), pairs }],
        }
    }

    fn wbtc_usdc_pairs() -> Vec<TitanPairLevels> {
        vec![
            pair_levels(WBTC, USDC, vec![level(100_000_000, 100_000_000_000)]),
            pair_levels(USDC, WBTC, vec![level(100_000_000_000, 99_000_000)]),
        ]
    }

    fn expected_id() -> String {
        // pamm ++ token0 ++ token1 with WBTC < USDC.
        format!("{PAMM}{}{}", &WBTC[2..], &USDC[2..])
    }

    #[test]
    fn first_snapshot_emits_new_pair_with_both_directions() {
        let mut tracker = tracker();
        let Update {
            block_number_or_timestamp,
            is_partial,
            sync_states,
            states,
            new_pairs,
            removed_pairs,
        } = tracker
            .process(message(100, wbtc_usdc_pairs()))
            .expect("update expected");

        assert_eq!(block_number_or_timestamp, 100);
        assert!(is_partial);
        assert!(sync_states.is_empty());
        assert!(removed_pairs.is_empty());

        let id = expected_id();
        let component = &new_pairs[&id];
        assert_eq!(component.protocol_system, "pricelevelstream:fermiswap");
        assert_eq!(
            component.static_attributes[PAMM_ADDRESS_ATTRIBUTE],
            Bytes::from_str(PAMM).unwrap()
        );

        let PriceLevelStreamState { token0, token1, quotes_0_to_1, quotes_1_to_0, gas_cost } =
            states[&id]
                .as_any()
                .downcast_ref::<PriceLevelStreamState>()
                .expect("price level state");
        assert_eq!(token0, &Bytes::from_str(WBTC).unwrap());
        assert_eq!(token1, &Bytes::from_str(USDC).unwrap());
        assert_eq!(quotes_0_to_1.len(), 1);
        assert_eq!(quotes_1_to_0.len(), 1);
        assert_eq!(quotes_0_to_1[0].amount_in, BigUint::from(100_000_000u64));
        assert_eq!(gas_cost, &BigUint::from(120_000u64));
    }

    #[test]
    fn repeated_snapshot_is_not_a_new_pair() {
        let mut tracker = tracker();
        tracker
            .process(message(100, wbtc_usdc_pairs()))
            .expect("update expected");
        let update = tracker
            .process(message(101, wbtc_usdc_pairs()))
            .expect("update expected");

        assert!(update.new_pairs.is_empty());
        assert!(update.removed_pairs.is_empty());
        assert!(update
            .states
            .contains_key(&expected_id()));
    }

    #[test]
    fn dropped_pair_is_removed() {
        let mut tracker = tracker();
        tracker
            .process(message(100, wbtc_usdc_pairs()))
            .expect("update expected");
        let weth_usdc =
            vec![pair_levels(WETH, USDC, vec![level(1_000_000_000_000_000_000, 3_000_000_000)])];
        let update = tracker
            .process(message(101, weth_usdc))
            .expect("update expected");

        assert_eq!(update.removed_pairs.len(), 1);
        assert!(update
            .removed_pairs
            .contains_key(&expected_id()));
        assert_eq!(update.new_pairs.len(), 1);
        assert_eq!(update.states.len(), 1);
    }

    #[test]
    fn unregistered_pamm_produces_no_update_without_auto_detection() {
        let mut tracker = SnapshotTracker::new(HashMap::new(), tokens(), false);
        assert!(tracker
            .process(message(100, wbtc_usdc_pairs()))
            .is_none());
    }

    #[test]
    fn auto_detected_pamm_is_served_under_its_address() {
        let mut tracker = SnapshotTracker::new(HashMap::new(), tokens(), true);
        let update = tracker
            .process(message(100, wbtc_usdc_pairs()))
            .expect("update expected");

        let component = &update.new_pairs[&expected_id()];
        assert_eq!(component.protocol_system, format!("pricelevelstream:{PAMM}"));
        let state = update.states[&expected_id()]
            .as_any()
            .downcast_ref::<PriceLevelStreamState>()
            .expect("price level state");
        assert_eq!(
            state.gas_cost,
            PriceLevelStreamConfig::auto_detected(Bytes::default()).gas_cost
        );

        // The synthesized config is cached: the next snapshot is not a new pair again.
        let update = tracker
            .process(message(101, wbtc_usdc_pairs()))
            .expect("update expected");
        assert!(update.new_pairs.is_empty());
    }

    #[test]
    fn with_default_pamms_registers_known_venues() {
        // PAMM is the FermiSwap router, one of the default venues.
        let fermiswap_router = Bytes::from_str(PAMM).unwrap();

        let builder = PriceLevelStreamBuilder::new();
        assert!(builder.registry.is_empty());

        let builder = builder.with_default_pamms();
        assert_eq!(builder.registry[&fermiswap_router].protocol, "fermiswap");

        // An `add_pamm` entry wins over the default for the same address, in either call order.
        let custom =
            || PriceLevelStreamConfig::new("custom", fermiswap_router.clone(), BigUint::from(1u64));
        for builder in [
            PriceLevelStreamBuilder::new()
                .add_pamm(custom())
                .with_default_pamms(),
            PriceLevelStreamBuilder::new()
                .with_default_pamms()
                .add_pamm(custom()),
        ] {
            assert_eq!(builder.registry[&fermiswap_router].protocol, "custom");
            assert_eq!(builder.registry[&fermiswap_router].gas_cost, BigUint::from(1u64));
        }
    }

    #[test]
    fn unknown_tokens_are_skipped() {
        let mut tracker = tracker();
        let unknown = vec![pair_levels(
            "0x1111111111111111111111111111111111111111",
            USDC,
            vec![level(1, 1)],
        )];
        assert!(tracker
            .process(message(100, unknown))
            .is_none());
    }
}
