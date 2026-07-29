use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    time::SystemTime,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::{interval, Duration};
use tracing::{error, info};
use tycho_common::{
    models::{protocol::GetAmountOutParams, Chain},
    simulation::indicatively_priced::SignedQuote,
    Bytes,
};

use super::models::{NativeOrderbookEntry, NativeOrderbookSide, NativePriceData};
use crate::{
    rfq::{client::RFQClient, errors::RFQError, models::TimestampHeader},
    tycho_client::feed::synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
    tycho_common::models::protocol::{ProtocolComponent, ProtocolComponentState},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeClient {
    pub chain: Chain,
    pub orderbook_endpoint: String,
    #[serde(skip_serializing, default)]
    pub api_key: String,
    pub tokens: HashSet<Bytes>,
    pub tvl: f64,
    pub quote_tokens: HashSet<Bytes>,
    pub poll_time: Duration,
    quote_timeout: Duration,
}

impl NativeClient {
    pub const PROTOCOL_SYSTEM: &'static str = "rfq:native";
    pub const DEFAULT_ORDERBOOK_ENDPOINT: &'static str =
        "https://v2.api.native.org/swap-api-v2/v1/orderbook";

    pub fn new(
        chain: Chain,
        api_key: String,
        tokens: HashSet<Bytes>,
        tvl: f64,
        quote_tokens: HashSet<Bytes>,
        poll_time: Duration,
        quote_timeout: Duration,
    ) -> Result<Self, RFQError> {
        Ok(Self {
            chain,
            orderbook_endpoint: Self::DEFAULT_ORDERBOOK_ENDPOINT.to_string(),
            api_key,
            tokens,
            tvl,
            quote_tokens,
            poll_time,
            quote_timeout,
        })
    }

    fn create_component_with_state(
        &self,
        component_id: String,
        tokens: Vec<Bytes>,
        book: NativePriceData,
        tvl: f64,
    ) -> ComponentWithState {
        let protocol_component = ProtocolComponent {
            id: component_id.clone(),
            protocol_system: Self::PROTOCOL_SYSTEM.to_string(),
            protocol_type_name: "native_relay_pool".to_string(),
            chain: self.chain,
            tokens,
            contract_addresses: vec![],
            static_attributes: Default::default(),
            change: Default::default(),
            creation_tx: Default::default(),
            created_at: Default::default(),
        };

        let mut attributes = HashMap::new();

        let book_json = serde_json::to_string(&book).unwrap_or_default();
        attributes.insert("book".to_string(), book_json.as_bytes().to_vec().into());

        ComponentWithState {
            state: ProtocolComponentState::new(&component_id, attributes, HashMap::new()),
            component: protocol_component,
            component_tvl: Some(tvl),
            entrypoints: vec![],
        }
    }

    async fn fetch_orderbook(
        &self,
        http_client: &Client,
    ) -> Result<Vec<NativeOrderbookEntry>, RFQError> {
        let response = http_client
            .get(&self.orderbook_endpoint)
            .query(&[("chain", self.chain.to_string())])
            .header("accept", "application/json")
            .header("apikey", &self.api_key)
            .send()
            .await
            .map_err(|e| RFQError::FatalError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(RFQError::ConnectionError(format!(
                "Native Relay orderbook HTTP error {}: {}",
                response.status(),
                response
                    .text()
                    .await
                    .unwrap_or_default()
            )));
        }

        response.json().await.map_err(|e| {
            RFQError::ParsingError(format!("Failed to parse Native Relay orderbook: {e}"))
        })
    }

    fn group_orderbook(
        &self,
        entries: Vec<NativeOrderbookEntry>,
    ) -> HashMap<String, NativePriceData> {
        let mut books = HashMap::new();

        for entry in entries {
            if !self
                .tokens
                .contains(&entry.base_address) ||
                !self
                    .tokens
                    .contains(&entry.quote_address)
            {
                continue;
            }

            let component_id =
                format!("native_relay_{}_{}", entry.base_address, entry.quote_address);
            match books.entry(component_id) {
                Entry::Occupied(mut book) => {
                    let book: &mut NativePriceData = book.get_mut();
                    match entry.side {
                        NativeOrderbookSide::Bid => book.bids.extend(entry.levels),
                        NativeOrderbookSide::Ask => book.asks.extend(entry.levels),
                    }
                }
                Entry::Vacant(book) => {
                    let mut price_data = NativePriceData {
                        base_symbol: entry.base_symbol,
                        quote_symbol: entry.quote_symbol,
                        base_address: entry.base_address,
                        quote_address: entry.quote_address,
                        minimum_in_base: entry.minimum_in_base,
                        bids: Vec::new(),
                        asks: Vec::new(),
                    };
                    match entry.side {
                        NativeOrderbookSide::Bid => price_data.bids = entry.levels,
                        NativeOrderbookSide::Ask => price_data.asks = entry.levels,
                    }
                    book.insert(price_data);
                }
            }
        }

        books
    }
}

#[async_trait]
impl RFQClient for NativeClient {
    fn stream(
        &self,
    ) -> BoxStream<'static, Result<(String, StateSyncMessage<TimestampHeader>), RFQError>> {
        let client = self.clone();
        let http_client = Client::new();

        Box::pin(async_stream::stream! {
            let mut current_components: HashMap<String, ComponentWithState> = HashMap::new();
            let mut ticker = interval(client.poll_time);

            loop {
                ticker.tick().await;

                // Native Relay publishes a complete RFQ orderbook once per request. Polling the
                // full book keeps component creation/removal deterministic and avoids per-pair REST
                // fan-out.
                let books = match client.fetch_orderbook(&http_client).await {
                    Ok(entries) => client.group_orderbook(entries),
                    Err(e) => {
                        error!("Failed to fetch Native Relay orderbook: {}", e);
                        continue;
                    }
                };

                let mut new_components = HashMap::new();

                for (component_id, book) in &books {
                    let quote_price_data = if client.quote_tokens.contains(&book.quote_address) {
                        None
                    } else {
                        // TVL thresholds are applied in approved quote-token units. If Native
                        // quotes this market against another token, normalize through any
                        // available approved quote-token market before filtering.
                        client
                            .quote_tokens
                            .iter()
                            .find_map(|approved_quote_token| {
                                books
                                    .values()
                                    .find(|candidate| {
                                        (candidate.base_address == book.quote_address &&
                                            &candidate.quote_address == approved_quote_token) ||
                                            (candidate.quote_address == book.quote_address &&
                                                &candidate.base_address == approved_quote_token)
                                    })
                            })
                    };

                    if !client.quote_tokens.contains(&book.quote_address) &&
                        quote_price_data.is_none()
                    {
                        continue;
                    }

                    let incoming_tvl = book.calculate_tvl(quote_price_data);

                    if incoming_tvl < client.tvl {
                        info!("Filtering out Native Relay market {} due to low TVL: {:.2} < {:.2}", component_id, incoming_tvl, client.tvl);
                        continue;
                    }

                    let tokens = vec![book.base_address.clone(), book.quote_address.clone()];
                    let component_with_state = client.create_component_with_state(
                        component_id.clone(),
                        tokens,
                        book.clone(),
                        incoming_tvl,
                    );
                    new_components.insert(component_id.clone(), component_with_state);
                }

                // Emit removals for markets that disappeared from the Relay orderbook or no longer
                // pass token/TVL filtering.
                let removed_components: HashMap<String, ProtocolComponent> = current_components
                    .iter()
                    .filter(|&(id, _)| !new_components.contains_key(id))
                    .map(|(k, v)| (k.clone(), v.component.clone()))
                    .collect();

                current_components = new_components.clone();

                let snapshot = Snapshot {
                    states: new_components,
                    vm_storage: HashMap::new(),
                };

                // Native is off-chain and timestamped, not block-based. Downstream decoders use
                // this wall-clock header to build a normal Tycho state update.
                let timestamp = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let msg = StateSyncMessage::<TimestampHeader> {
                    header: TimestampHeader { timestamp },
                    snapshots: snapshot,
                    deltas: None,
                    removed_components,
                };

                yield Ok(("native".to_string(), msg));
            }
        })
    }

    async fn request_binding_quote(
        &self,
        _params: &GetAmountOutParams,
    ) -> Result<SignedQuote, RFQError> {
        Err(RFQError::FatalError(
            "Native Relay binding quote endpoint is not implemented yet".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use tycho_common::models::Chain;

    use super::*;
    use crate::rfq::protocols::native::models::NativePriceLevel;

    #[test]
    fn test_native_client_serialization() {
        let mut tokens = HashSet::new();
        tokens.insert(Bytes::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap());
        tokens.insert(Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap());

        let client = NativeClient::new(
            Chain::Ethereum,
            "test-api-key".to_string(),
            tokens,
            10.0,
            HashSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .unwrap();

        let serialized = serde_json::to_string(&client).unwrap();
        let deserialized: NativeClient = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.chain, client.chain);
        assert_eq!(deserialized.orderbook_endpoint, client.orderbook_endpoint);
        assert_eq!(deserialized.tokens, client.tokens);
        assert_eq!(deserialized.tvl, client.tvl);
        assert!(deserialized.api_key.is_empty());
    }

    #[test]
    fn creates_indexer_compatible_component_from_relay_orderbook() {
        let weth = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let usdt = Bytes::from_str("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap();
        let client = NativeClient::new(
            Chain::Ethereum,
            "test-api-key".to_string(),
            HashSet::from([weth.clone(), usdt.clone()]),
            0.0,
            HashSet::from([usdt.clone()]),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .unwrap();

        let books = client.group_orderbook(vec![
            NativeOrderbookEntry {
                base_symbol: "WETH".to_string(),
                quote_symbol: "USDT".to_string(),
                base_address: weth.clone(),
                quote_address: usdt.clone(),
                minimum_in_base: 0.0,
                side: NativeOrderbookSide::Bid,
                levels: vec![NativePriceLevel { quantity: 0.0001, price: 3213.12345 }],
            },
            NativeOrderbookEntry {
                base_symbol: "WETH".to_string(),
                quote_symbol: "USDT".to_string(),
                base_address: weth.clone(),
                quote_address: usdt.clone(),
                minimum_in_base: 0.0,
                side: NativeOrderbookSide::Ask,
                levels: vec![NativePriceLevel { quantity: 2.0, price: 3214.0 }],
            },
        ]);

        let (component_id, book) = books
            .into_iter()
            .next()
            .expect("one grouped book");
        let component = client.create_component_with_state(
            component_id.clone(),
            vec![book.base_address.clone(), book.quote_address.clone()],
            book.clone(),
            book.calculate_tvl(None),
        );

        assert_eq!(component.component.id, component_id);
        assert_eq!(component.component.protocol_system, NativeClient::PROTOCOL_SYSTEM);
        assert_eq!(component.component.protocol_type_name, "native_relay_pool");
        assert_eq!(component.component.tokens, vec![weth, usdt]);
        assert_eq!(component.state.component_id, component_id);

        let encoded_book = component
            .state
            .attributes
            .get("book")
            .expect("book attribute");
        let decoded_book: NativePriceData = serde_json::from_slice(encoded_book).unwrap();
        assert_eq!(decoded_book.bids.len(), 1);
        assert_eq!(decoded_book.asks.len(), 1);
        assert_eq!(decoded_book.bids[0].quantity, 0.0001);
        assert_eq!(decoded_book.bids[0].price, 3213.12345);
    }
}
