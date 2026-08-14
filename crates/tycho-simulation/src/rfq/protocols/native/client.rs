use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::SystemTime,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use num_bigint::BigUint;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::{interval, timeout, Duration};
use tracing::{error, info, warn};
use tycho_common::{
    models::{protocol::GetAmountOutParams, Chain},
    simulation::indicatively_priced::SignedQuote,
    Bytes,
};

use super::models::{NativeOrderbookEntry, NativeOrderbookSide, NativePriceData};
use crate::{
    rfq::{
        client::RFQClient,
        errors::RFQError,
        models::TimestampHeader,
        protocols::{
            native::models::{
                FirmQuoteRequest, FirmQuoteResponse, NativeApiErrorResponse, NativeSupportedChain,
            },
            utils::bytes_to_address,
        },
    },
    tycho_client::feed::synchronizer::{ComponentWithState, Snapshot, StateSyncMessage},
    tycho_common::models::protocol::{ProtocolComponent, ProtocolComponentState},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeClient {
    pub chain: Chain,
    pub endpoint: String,
    #[serde(skip_serializing, default)]
    pub api_key: String,
    pub tokens: HashSet<Bytes>,
    pub tvl: f64,
    pub quote_tokens: HashSet<Bytes>,
    pub poll_time: Duration,
    pub quote_timeout: Duration,
}

impl NativeClient {
    pub const PROTOCOL_SYSTEM: &'static str = "rfq:native";
    pub const DEFAULT_ENDPOINT: &'static str = "https://v2.api.native.org/swap-api-v2/v1";

    fn classify_api_error(error: &NativeApiErrorResponse) -> (bool, RFQError) {
        let message = format!("Native API error {}: {}", error.code, error.message);
        match error.code {
            // Native documents these as temporary risk/rate-limit failures.
            301016 | 405030 => (true, RFQError::QuoteNotFound(message)),
            201005 => (true, RFQError::ConnectionError(message)),
            // The requested quote is unavailable for the current orderbook/liquidity.
            101010 | 171037 | 171011 | 171015 | 101007 => (false, RFQError::QuoteNotFound(message)),
            // The request itself must be corrected before another attempt can succeed.
            131003 | 131004 | 131011 | 171018 | 171053 | 131005 => {
                (false, RFQError::InvalidInput(message))
            }
            201001 => (false, RFQError::FatalError(message)),
            _ => (
                false,
                RFQError::FatalError(format!(
                    "Unknown Native API error {}: {}",
                    error.code, error.message
                )),
            ),
        }
    }

    async fn wait_before_retry(
        start_time: &std::time::Instant,
        quote_timeout: Duration,
        retry_delay: Duration,
    ) -> bool {
        let Some(remaining_time) = quote_timeout.checked_sub(start_time.elapsed()) else {
            return false;
        };
        if remaining_time.is_zero() {
            return false;
        }

        tokio::time::sleep(retry_delay.min(remaining_time)).await;
        start_time.elapsed() < quote_timeout
    }

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
            endpoint: Self::DEFAULT_ENDPOINT.to_string(),
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
            .get(format!("{}/orderbook", self.endpoint))
            .query(&[("chain", self.chain.to_string())])
            .header("accept", "application/json")
            .header("apikey", &self.api_key)
            .send()
            .await
            .map_err(|e| RFQError::ConnectionError(e.to_string()))?;

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
        let mut entries_by_pair: HashMap<(Bytes, Bytes), Vec<NativeOrderbookEntry>> =
            HashMap::new();

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

            let pair = if entry.base_address.as_ref() <= entry.quote_address.as_ref() {
                (entry.base_address.clone(), entry.quote_address.clone())
            } else {
                (entry.quote_address.clone(), entry.base_address.clone())
            };
            entries_by_pair
                .entry(pair)
                .or_default()
                .push(entry);
        }

        let mut books = HashMap::new();
        for entries in entries_by_pair.into_values() {
            let Some(first) = entries.first() else {
                continue;
            };
            let has_reverse = entries.iter().any(|entry| {
                entry.base_address == first.quote_address &&
                    entry.quote_address == first.base_address
            });
            let canonical = if has_reverse {
                entries
                    .iter()
                    .find(|entry: &&NativeOrderbookEntry| {
                        self.quote_tokens
                            .contains(&entry.quote_address) &&
                            !self
                                .quote_tokens
                                .contains(&entry.base_address)
                    })
                    .or_else(|| {
                        entries.iter().min_by(|a, b| {
                            a.base_address
                                .as_ref()
                                .cmp(b.base_address.as_ref())
                        })
                    })
                    .unwrap_or(first)
            } else {
                first
            };

            let base_symbol = canonical.base_symbol.clone();
            let quote_symbol = canonical.quote_symbol.clone();
            let base_address = canonical.base_address.clone();
            let quote_address = canonical.quote_address.clone();
            let mut direct_bids = Vec::new();
            let mut direct_asks = Vec::new();
            let mut mirrored_bids = Vec::new();
            let mut mirrored_asks = Vec::new();
            let mut minimum_in_base: f64 = 0.0;
            let mut minimum_in_quote: f64 = 0.0;

            for entry in entries {
                let is_direct =
                    entry.base_address == base_address && entry.quote_address == quote_address;
                if is_direct {
                    minimum_in_base = minimum_in_base.max(entry.minimum_in_base);
                    match entry.side {
                        NativeOrderbookSide::Bid => direct_bids.extend(entry.levels),
                        NativeOrderbookSide::Ask => direct_asks.extend(entry.levels),
                    }
                } else {
                    minimum_in_quote = minimum_in_quote.max(entry.minimum_in_base);
                    let levels = NativePriceData::invert_price_levels(&entry.levels);
                    match entry.side {
                        NativeOrderbookSide::Bid => mirrored_asks.extend(levels),
                        NativeOrderbookSide::Ask => mirrored_bids.extend(levels),
                    }
                }
            }

            let bids = if direct_bids.is_empty() { mirrored_bids } else { direct_bids };
            let asks = if direct_asks.is_empty() { mirrored_asks } else { direct_asks };
            let component_id = format!("native_relay_{}_{}", base_address, quote_address);
            books.insert(
                component_id,
                NativePriceData {
                    base_symbol,
                    quote_symbol,
                    base_address,
                    quote_address,
                    minimum_in_base,
                    minimum_in_quote,
                    bids,
                    asks,
                },
            );
        }

        books
    }
    fn process_quote_response(
        quote_response: FirmQuoteResponse,
        params: &GetAmountOutParams,
    ) -> Result<SignedQuote, RFQError> {
        // 1. Check API-level success
        if !quote_response.success {
            return Err(RFQError::QuoteNotFound(format!(
                "Native Relay quote request failed: {}",
                quote_response.error_message
            )));
        }

        // Ensure we actually got an order
        let order = quote_response
            .orders
            .first()
            .ok_or_else(|| {
                RFQError::QuoteNotFound(format!(
                    "No Native Relay orders for {} {} -> {}",
                    params.amount_in, params.token_in, params.token_out,
                ))
            })?
            .clone();

        // Prevents silently accepting a mismatched/malicious quote.
        let seller_token = bytes_to_address(&params.token_in)?;
        let buyer_token = bytes_to_address(&params.token_out)?;
        if !order
            .seller_token
            .eq_ignore_ascii_case(&seller_token.to_string()) ||
            !order
                .buyer_token
                .eq_ignore_ascii_case(&buyer_token.to_string())
        {
            return Err(RFQError::ParsingError(format!(
                "Native Relay quote token mismatch: expected {}/{}, got {}/{}",
                seller_token, buyer_token, order.seller_token, order.buyer_token
            )));
        }

        // Security: reject already-expired quotes before we bother building a SignedQuote
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| RFQError::ParsingError("SystemTime before UNIX EPOCH!".to_string()))?
            .as_secs();

        if order.deadline_timestamp <= now {
            return Err(RFQError::QuoteNotFound(format!(
                "Native Relay quote already expired: deadline {} <= now {}",
                order.deadline_timestamp, now
            )));
        }

        let quoted_amount_in = BigUint::from_str(&quote_response.amount_in).map_err(|_| {
            RFQError::ParsingError(format!(
                "Failed to parse amount_in: {}",
                quote_response.amount_in
            ))
        })?;
        if quoted_amount_in != params.amount_in {
            return Err(RFQError::ParsingError(format!(
                "Native Relay quote input amount mismatch: expected {}, got {}",
                params.amount_in, quoted_amount_in
            )));
        }

        if quote_response
            .tx_request
            .calldata
            .is_empty()
        {
            return Err(RFQError::ConnectionError("Calldata not found!".to_string()));
        }
        // Decode calldata (pre-built by Native Relay, ready to submit as-is)
        let calldata = hex::decode(
            quote_response
                .tx_request
                .calldata
                .trim_start_matches("0x"),
        )
        .map_err(|e| RFQError::ParsingError(format!("Failed to decode calldata: {e}")))?;

        let target = Bytes::from_str(&quote_response.tx_request.target).map_err(|_| {
            RFQError::ParsingError(format!(
                "Failed to parse router target address: {}",
                quote_response.tx_request.target
            ))
        })?;

        let mut quote_attributes: HashMap<String, Bytes> = HashMap::new();
        // Native returns tx_request_value in string
        quote_attributes.insert(
            "value".to_string(),
            Bytes::from(
                quote_response
                    .tx_request
                    .value
                    .as_bytes()
                    .to_vec(),
            ),
        );
        quote_attributes.insert("target".to_string(), target);
        quote_attributes.insert("calldata".to_string(), Bytes::from(calldata));
        quote_attributes.insert(
            "deadline_timestamp".to_string(),
            Bytes::from(
                order
                    .deadline_timestamp
                    .to_be_bytes()
                    .to_vec(),
            ),
        );
        quote_attributes
            .insert("quote_id".to_string(), Bytes::from(order.quote_id.as_bytes().to_vec()));

        // Signature may be empty for Relay-signed pool orders; store only if present.
        if !order.signature.is_empty() {
            quote_attributes.insert(
                "signature".to_string(),
                Bytes::from(hex::decode(order.signature.trim_start_matches("0x")).map_err(
                    |e| RFQError::ParsingError(format!("Failed to decode signature: {e}")),
                )?),
            );
        }

        Ok(SignedQuote {
            base_token: params.token_in.clone(),
            quote_token: params.token_out.clone(),
            amount_in: quoted_amount_in,
            amount_out: BigUint::from_str(&quote_response.amount_out).map_err(|_| {
                RFQError::ParsingError(format!(
                    "Failed to parse amount_out: {}",
                    quote_response.amount_out
                ))
            })?,
            quote_attributes,
        })
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
        params: &GetAmountOutParams,
    ) -> Result<SignedQuote, RFQError> {
        let receiver = bytes_to_address(&params.receiver)?;
        let token_in = bytes_to_address(&params.token_in)?;
        let token_out = bytes_to_address(&params.token_out)?;
        let http_client = Client::new();

        let chain = NativeSupportedChain::try_from(self.chain).map_err(RFQError::FatalError)?;

        let request_data = FirmQuoteRequest {
            src_chain: chain,
            dst_chain: chain,
            from_address: receiver.to_string(),
            amount_wei: params.amount_in.to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            version: 4,
            allow_multihop: false, // Multihop not implemented yet
        };
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        let start_time = std::time::Instant::now();

        for attempt in 0..MAX_RETRIES {
            let elapse = start_time.elapsed();
            if elapse >= self.quote_timeout {
                return Err(last_error.unwrap_or_else(|| {
                    RFQError::ConnectionError(format!(
                        "Native quote request timed out after {:?} secs",
                        self.quote_timeout.as_secs()
                    ))
                }));
            }
            let remaining_time = self.quote_timeout - elapse;

            let req = http_client
                .get(format!("{}/firm-quote", self.endpoint))
                .query(&request_data)
                .header("apikey", &self.api_key);

            let response = match timeout(remaining_time, req.send()).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => {
                    warn!("Quote request failed: {}/{} : {}", attempt + 1, MAX_RETRIES - 1, e);

                    last_error = Some(RFQError::ConnectionError(format!(
                        "Failed to make RFQ quote request:{e}"
                    )));
                    if attempt < MAX_RETRIES - 1 &&
                        Self::wait_before_retry(
                            &start_time,
                            self.quote_timeout,
                            Duration::from_millis(100),
                        )
                        .await
                    {
                        continue;
                    }
                    return Err(last_error.unwrap());
                }
                Err(_) => {
                    return Err(RFQError::ConnectionError(format!(
                        "Native quote request timed out after {} seconds",
                        self.quote_timeout.as_secs()
                    )))
                }
            };

            let status = response.status();
            let elapsed = start_time.elapsed();
            if elapsed >= self.quote_timeout {
                return Err(RFQError::ConnectionError(format!(
                    "Native quote request timed out after {} seconds",
                    self.quote_timeout.as_secs()
                )));
            }
            let response_body = match timeout(self.quote_timeout - elapsed, response.bytes()).await
            {
                Ok(Ok(body)) => body,
                Ok(Err(e)) => {
                    last_error = Some(RFQError::ConnectionError(format!(
                        "Failed to read Native quote response: {e}"
                    )));
                    if attempt < MAX_RETRIES - 1 {
                        warn!(
                            "Failed to read Native quote response (attempt {}/{}): {}",
                            attempt + 1,
                            MAX_RETRIES,
                            e
                        );
                        if Self::wait_before_retry(
                            &start_time,
                            self.quote_timeout,
                            Duration::from_millis(100),
                        )
                        .await
                        {
                            continue;
                        }
                    }
                    return Err(last_error.unwrap());
                }
                Err(_) => {
                    return Err(RFQError::ConnectionError(format!(
                        "Native quote request timed out after {} seconds",
                        self.quote_timeout.as_secs()
                    )))
                }
            };
            let response_text = String::from_utf8_lossy(&response_body);

            // Native returns documented API error codes in the body, including with HTTP 200.
            if let Ok(api_error) = serde_json::from_slice::<NativeApiErrorResponse>(&response_body)
            {
                let (should_retry, error) = Self::classify_api_error(&api_error);
                if should_retry {
                    last_error = Some(error);
                    if attempt < MAX_RETRIES - 1 {
                        warn!(
                            "Native returned retryable API error {} (attempt {}/{}): {}",
                            api_error.code,
                            attempt + 1,
                            MAX_RETRIES,
                            api_error.message
                        );
                        if Self::wait_before_retry(
                            &start_time,
                            self.quote_timeout,
                            Duration::from_secs(1),
                        )
                        .await
                        {
                            continue;
                        }
                    }
                    return Err(last_error.unwrap());
                }
                return Err(error);
            }

            if !status.is_success() {
                return Err(RFQError::ConnectionError(format!(
                    "Unexpected Native quote HTTP response ({status}): {response_text}"
                )));
            }

            let quote_response = match serde_json::from_slice::<FirmQuoteResponse>(&response_body) {
                Ok(res) => res,
                Err(e) => {
                    last_error = Some(RFQError::ParsingError(format!(
                        "Failed to parse Native quote response: {e}"
                    )));
                    if attempt < MAX_RETRIES - 1 {
                        warn!(
                            "Failed to parse Native quote response (attempt {}/{}): {}",
                            attempt + 1,
                            MAX_RETRIES,
                            e
                        );
                        if Self::wait_before_retry(
                            &start_time,
                            self.quote_timeout,
                            Duration::from_millis(100),
                        )
                        .await
                        {
                            continue;
                        }
                    }
                    return Err(last_error.unwrap());
                }
            };
            if quote_response.success {
                return Self::process_quote_response(quote_response, params);
            } else {
                return Err(RFQError::FatalError(format!(
                    "Native API returned success=false without a documented error code: {}",
                    quote_response.error_message
                )));
            }
        }

        Err(last_error.unwrap_or_else(|| {
            RFQError::ConnectionError("Native quote request failed after retries".to_string())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        str::FromStr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tycho_common::models::Chain;

    use super::*;
    use crate::rfq::protocols::native::models::NativePriceLevel;

    fn successful_quote_response(amount_in: &str) -> FirmQuoteResponse {
        serde_json::from_value(serde_json::json!({
            "success": true,
            "orders": [{
                "pool": "0x1111111111111111111111111111111111111111",
                "signer": "0x2222222222222222222222222222222222222222",
                "recipient": "0x4444444444444444444444444444444444444444",
                "sellerToken": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "buyerToken": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "effectiveSellerTokenAmount": amount_in,
                "sellerTokenAmount": amount_in,
                "buyerTokenAmount": "2",
                "deadlineTimestamp": u64::MAX,
                "nonce": 1,
                "quoteId": "test-quote",
                "multiHop": false,
                "signature": "",
                "externalSwapCalldata": "",
                "amountOutMinimum": "2",
                "widgetFee": {
                    "signer": "0x0000000000000000000000000000000000000000",
                    "feeRecipient": "0x0000000000000000000000000000000000000000",
                    "feeRate": 0.0
                },
                "widgetFeeSignature": ""
            }],
            "widgetFee": {
                "signer": "0x0000000000000000000000000000000000000000",
                "feeRecipient": "0x0000000000000000000000000000000000000000",
                "feeRate": 0.0
            },
            "widgetFeeSignature": "",
            "recipient": "0x4444444444444444444444444444444444444444",
            "amountIn": amount_in,
            "amountOut": "2",
            "amountOutBeforeFee": "2",
            "fallbackSwapDataArray": null,
            "tokenTransferFeeOnPercent": 0.0,
            "txRequest": {
                "target": "0x8a2ddc0461Fcf96F81a05529Bed540d4f1eb2a00",
                "calldata": "0x0947c2d9",
                "value": "0"
            },
            "source": [6],
            "errorMessage": "",
            "router_version": "4",
            "toWrap": false,
            "toUnwrap": false,
            "amountInOffset": 0,
            "amountOutMinimumOffset": 0
        }))
        .unwrap()
    }

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
        assert_eq!(deserialized.endpoint, client.endpoint);
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
            NativeOrderbookEntry {
                base_symbol: "USDT".to_string(),
                quote_symbol: "WETH".to_string(),
                base_address: usdt.clone(),
                quote_address: weth.clone(),
                minimum_in_base: 100.0,
                side: NativeOrderbookSide::Bid,
                levels: vec![NativePriceLevel { quantity: 6428.0, price: 1.0 / 3214.0 }],
            },
            NativeOrderbookEntry {
                base_symbol: "USDT".to_string(),
                quote_symbol: "WETH".to_string(),
                base_address: usdt.clone(),
                quote_address: weth.clone(),
                minimum_in_base: 100.0,
                side: NativeOrderbookSide::Ask,
                levels: vec![NativePriceLevel { quantity: 0.321312345, price: 1.0 / 3213.12345 }],
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

    #[test]
    fn merges_mirrored_bid_books_into_one_bid_ask_book() {
        let weth = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let usdc = Bytes::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap();
        let client = NativeClient::new(
            Chain::Ethereum,
            "test-api-key".to_string(),
            HashSet::from([weth.clone(), usdc.clone()]),
            0.0,
            HashSet::from([usdc.clone()]),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .unwrap();
        let entries = vec![
            NativeOrderbookEntry {
                base_symbol: "WETH".to_string(),
                quote_symbol: "USDC".to_string(),
                base_address: weth.clone(),
                quote_address: usdc.clone(),
                minimum_in_base: 100_000_000_000.0,
                side: NativeOrderbookSide::Bid,
                levels: vec![NativePriceLevel { quantity: 1.0, price: 2_000.0 }],
            },
            NativeOrderbookEntry {
                base_symbol: "USDC".to_string(),
                quote_symbol: "WETH".to_string(),
                base_address: usdc.clone(),
                quote_address: weth.clone(),
                minimum_in_base: 100.0,
                side: NativeOrderbookSide::Bid,
                levels: vec![NativePriceLevel { quantity: 2_000.0, price: 0.0005 }],
            },
        ];

        let books = client.group_orderbook(entries.clone());
        let reversed_books = client.group_orderbook(entries.into_iter().rev().collect());

        assert_eq!(books, reversed_books);
        assert_eq!(books.len(), 1);
        let component_id = format!("native_relay_{}_{}", weth, usdc);
        let book = books.get(&component_id).unwrap();
        assert_eq!(book.base_address, weth);
        assert_eq!(book.quote_address, usdc);
        assert_eq!(book.minimum_in_base, 100_000_000_000.0);
        assert_eq!(book.minimum_in_quote, 100.0);
        assert_eq!(book.bids, vec![NativePriceLevel { quantity: 1.0, price: 2_000.0 }]);
        assert_eq!(book.asks, vec![NativePriceLevel { quantity: 1.0, price: 2_000.0 }]);
    }

    fn create_test_quote_params() -> GetAmountOutParams {
        GetAmountOutParams {
            amount_in: BigUint::from(1u64),
            token_in: Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
            token_out: Bytes::from_str("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap(),
            sender: Bytes::from_str("0x3333333333333333333333333333333333333333").unwrap(),
            receiver: Bytes::from_str("0x4444444444444444444444444444444444444444").unwrap(),
        }
    }

    #[test]
    fn accepts_quote_with_requested_input_amount() {
        let params = create_test_quote_params();
        let response = successful_quote_response(&params.amount_in.to_string());

        let quote = NativeClient::process_quote_response(response, &params).unwrap();

        assert_eq!(quote.amount_in, params.amount_in);
    }

    #[test]
    fn rejects_quote_with_mismatched_input_amount() {
        let params = create_test_quote_params();
        let response = successful_quote_response("2");

        let result = NativeClient::process_quote_response(response, &params);

        assert!(matches!(
            result,
            Err(RFQError::ParsingError(message)) if message.contains("input amount mismatch")
        ));
    }

    #[test]
    fn accepts_native_eth_response_using_tycho_zero_address() {
        let mut params = create_test_quote_params();
        params.token_in = Bytes::zero(20);
        let mut response = successful_quote_response(&params.amount_in.to_string());
        response.orders[0].seller_token = "0x0000000000000000000000000000000000000000".to_string();
        response.tx_request.value = params.amount_in.to_string();

        let quote = NativeClient::process_quote_response(response, &params).unwrap();

        assert_eq!(quote.amount_in, params.amount_in);
    }

    #[test]
    fn accepts_native_eth_orderbook_using_tycho_zero_address() {
        let tycho_native_eth = Bytes::zero(20);
        let usdc = Bytes::from_str("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap();
        let client = NativeClient::new(
            Chain::Ethereum,
            "test-api-key".to_string(),
            HashSet::from([tycho_native_eth.clone(), usdc.clone()]),
            0.0,
            HashSet::from([usdc.clone()]),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .unwrap();

        let books = client.group_orderbook(vec![NativeOrderbookEntry {
            base_symbol: "ETH".to_string(),
            quote_symbol: "USDC".to_string(),
            base_address: tycho_native_eth.clone(),
            quote_address: usdc,
            minimum_in_base: 1.0,
            side: NativeOrderbookSide::Bid,
            levels: vec![NativePriceLevel { quantity: 1.0, price: 3_000.0 }],
        }]);

        let book = books.values().next().unwrap();
        assert_eq!(book.base_address, tycho_native_eth);
    }

    fn create_test_client(endpoint: String) -> NativeClient {
        let mut client = NativeClient::new(
            Chain::Ethereum,
            "test-api-key".to_string(),
            HashSet::new(),
            0.0,
            HashSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .unwrap();
        client.endpoint = endpoint;
        client
    }

    async fn create_quote_server(
        final_status: &'static str,
        final_body: &'static str,
    ) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_request_count = request_count.clone();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                server_request_count.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 {final_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{final_body}",
                    final_body.len()
                );
                let _ = stream
                    .write_all(response.as_bytes())
                    .await;
                let _ = stream.shutdown().await;
            }
        });

        (address, request_count)
    }

    #[tokio::test]
    async fn handles_documented_quote_error_without_retrying() {
        let (address, request_count) = create_quote_server(
            "200 OK",
            r#"{"code":171015,"message":"quoted token not available"}"#,
        )
        .await;
        let client = create_test_client(format!("http://{address}"));

        let result = client
            .request_binding_quote(&create_test_quote_params())
            .await;

        match result {
            Err(RFQError::QuoteNotFound(message)) => {
                assert!(message.contains("171015"));
                assert!(message.contains("quoted token not available"));
            }
            other => panic!("Expected Native API error, got {other:?}"),
        }
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_documented_temporary_api_error() {
        let (address, request_count) = create_quote_server(
            "200 OK",
            r#"{"code":301016,"message":"quote invalid, risk management checks failed"}"#,
        )
        .await;
        let client = create_test_client(format!("http://{address}"));

        let result = client
            .request_binding_quote(&create_test_quote_params())
            .await;

        assert!(matches!(result, Err(RFQError::QuoteNotFound(_))));
        assert_eq!(request_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retries_malformed_success_response() {
        let (address, request_count) =
            create_quote_server("200 OK", r#"{"unexpected":true}"#).await;
        let client = create_test_client(format!("http://{address}"));

        let result = client
            .request_binding_quote(&create_test_quote_params())
            .await;

        assert!(matches!(result, Err(RFQError::ParsingError(_))));
        assert_eq!(request_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_documented_authentication_error() {
        let (address, request_count) = create_quote_server(
            "200 OK",
            r#"{"code":201001,"message":"auth get api key is invalid"}"#,
        )
        .await;
        let client = create_test_client(format!("http://{address}"));

        let result = client
            .request_binding_quote(&create_test_quote_params())
            .await;

        assert!(matches!(result, Err(RFQError::FatalError(_))));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn classifies_only_documented_native_api_codes() {
        for code in [301016, 405030] {
            let (retry, error) = NativeClient::classify_api_error(&NativeApiErrorResponse {
                code,
                message: "temporary risk failure".to_string(),
            });
            assert!(retry);
            assert!(matches!(error, RFQError::QuoteNotFound(_)));
        }

        let (retry, error) = NativeClient::classify_api_error(&NativeApiErrorResponse {
            code: 201005,
            message: "rate reach limit".to_string(),
        });
        assert!(retry);
        assert!(matches!(error, RFQError::ConnectionError(_)));

        for code in [101010, 171037, 171011, 171015, 101007] {
            let (retry, error) = NativeClient::classify_api_error(&NativeApiErrorResponse {
                code,
                message: "quote unavailable".to_string(),
            });
            assert!(!retry);
            assert!(matches!(error, RFQError::QuoteNotFound(_)));
        }

        for code in [131003, 131004, 131011, 171018, 171053, 131005] {
            let (retry, error) = NativeClient::classify_api_error(&NativeApiErrorResponse {
                code,
                message: "invalid request".to_string(),
            });
            assert!(!retry);
            assert!(matches!(error, RFQError::InvalidInput(_)));
        }

        let (retry, error) = NativeClient::classify_api_error(&NativeApiErrorResponse {
            code: 999999,
            message: "unknown".to_string(),
        });
        assert!(!retry);
        assert!(matches!(error, RFQError::FatalError(_)));
    }
}
