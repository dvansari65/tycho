use std::str::FromStr;

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};
use tycho_common::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NativeOrderbookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativePriceLevel {
    pub quantity: f64,
    pub price: f64,
}

fn deserialize_level<'de, D>(deserializer: D) -> Result<NativePriceLevel, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = <[f64; 2]>::deserialize(deserializer)?;
    Ok(NativePriceLevel { quantity: values[0], price: values[1] })
}

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<NativePriceLevel>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Level(#[serde(deserialize_with = "deserialize_level")] NativePriceLevel);

    Vec::<Level>::deserialize(deserializer).map(|levels| {
        levels
            .into_iter()
            .map(|level| level.0)
            .collect()
    })
}

fn deserialize_address<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let address = String::deserialize(deserializer)?;
    let address = Address::from_str(&address).map_err(serde::de::Error::custom)?;
    Bytes::from_str(&address.to_checksum(None)).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct NativeOrderbookEntry {
    pub base_symbol: String,
    pub quote_symbol: String,
    #[serde(deserialize_with = "deserialize_address")]
    pub base_address: Bytes,
    #[serde(deserialize_with = "deserialize_address")]
    pub quote_address: Bytes,
    pub minimum_in_base: f64,
    pub side: NativeOrderbookSide,
    #[serde(deserialize_with = "deserialize_levels")]
    pub levels: Vec<NativePriceLevel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativePriceData {
    pub base_symbol: String,
    pub quote_symbol: String,
    pub base_address: Bytes,
    pub quote_address: Bytes,
    pub minimum_in_base: f64,
    pub bids: Vec<NativePriceLevel>,
    pub asks: Vec<NativePriceLevel>,
}

impl NativePriceData {
    pub fn calculate_tvl(&self, quote_price_data: Option<&NativePriceData>) -> f64 {
        let bid_tvl: f64 = self
            .bids
            .iter()
            .map(|level: &NativePriceLevel| level.quantity * level.price)
            .sum();
        let ask_tvl: f64 = self
            .asks
            .iter()
            .map(|level: &NativePriceLevel| level.quantity * level.price)
            .sum();

        let mut total_tvl = (bid_tvl + ask_tvl) / 2.0;
        if let Some(quote_data) = quote_price_data {
            if let Some(price_of_quote_token) =
                quote_data.get_mid_price(total_tvl, &self.quote_address)
            {
                total_tvl *= price_of_quote_token;
            } else {
                return 0.0;
            }
        }

        total_tvl
    }

    pub fn get_mid_price(&self, amount: f64, sell_token: &Bytes) -> Option<f64> {
        if sell_token != &self.base_address && sell_token != &self.quote_address {
            return None;
        }

        let inverse = sell_token == &self.quote_address;
        let asks_price = Self::get_price_for_levels(amount, &self.asks, inverse)?;
        let bids_price = Self::get_price_for_levels(amount, &self.bids, inverse)?;
        Some((asks_price + bids_price) / 2.0)
    }

    fn get_price_for_levels(
        amount_in: f64,
        price_levels: &[NativePriceLevel],
        invert: bool,
    ) -> Option<f64> {
        if price_levels.is_empty() || amount_in <= 0.0 {
            return None;
        }

        let levels =
            if invert { Self::invert_price_levels(price_levels) } else { price_levels.to_vec() };

        let (amount_out, remaining_in) = Self::get_amount_out_from_levels(amount_in, &levels);
        let consumed_amount_in = amount_in - remaining_in;
        if consumed_amount_in <= 0.0 {
            return None;
        }

        Some(amount_out / consumed_amount_in)
    }

    pub fn get_amount_out_from_levels(
        amount_in: f64,
        price_levels: &[NativePriceLevel],
    ) -> (f64, f64) {
        let mut remaining_amount_in = amount_in;
        let mut amount_out = 0.0;

        for level in price_levels {
            if remaining_amount_in <= 0.0 {
                break;
            }

            let amount_in_available_to_trade = remaining_amount_in.min(level.quantity);
            amount_out += amount_in_available_to_trade * level.price;
            remaining_amount_in -= amount_in_available_to_trade;
        }

        (amount_out, remaining_amount_in)
    }

    pub fn invert_price_levels(price_levels: &[NativePriceLevel]) -> Vec<NativePriceLevel> {
        price_levels
            .iter()
            .filter(|level| level.price > 0.0)
            .map(|level| NativePriceLevel {
                quantity: level.quantity * level.price,
                price: 1.0 / level.price,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FirmQuoteRequest {
    pub from_address: String,
    pub src_chain: NativeSupportedChain,
    pub dst_chain: NativeSupportedChain,
    pub token_in: String,
    pub token_out: String,
    pub amount_wei: String,
    pub version: u32,
    pub allow_multihop: bool,
}

// --- Response ---

#[derive(Debug, Clone, Deserialize)]
pub struct WidgetFee {
    pub signer: String,
    #[serde(rename = "feeRecipient")]
    pub fee_recipient: String,
    #[serde(rename = "feeRate")]
    pub fee_rate: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TxRequest {
    pub target: String,
    pub calldata: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FirmQuoteOrder {
    pub pool: String,
    pub signer: String,
    pub recipient: String,
    #[serde(rename = "sellerToken")]
    pub seller_token: String,
    #[serde(rename = "buyerToken")]
    pub buyer_token: String,
    #[serde(rename = "effectiveSellerTokenAmount")]
    pub effective_seller_token_amount: String,
    #[serde(rename = "sellerTokenAmount")]
    pub seller_token_amount: String,
    #[serde(rename = "buyerTokenAmount")]
    pub buyer_token_amount: String,
    #[serde(rename = "deadlineTimestamp")]
    pub deadline_timestamp: u64,
    pub nonce: u64,
    #[serde(rename = "quoteId")]
    pub quote_id: String,
    #[serde(rename = "multiHop")]
    pub multi_hop: bool,
    pub signature: String,
    #[serde(rename = "externalSwapCalldata")]
    pub external_swap_calldata: String,
    #[serde(rename = "amountOutMinimum")]
    pub amount_out_minimum: String,
    #[serde(rename = "widgetFee")]
    pub widget_fee: WidgetFee,
    #[serde(rename = "widgetFeeSignature")]
    pub widget_fee_signature: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FirmQuoteResponse {
    pub success: bool,
    pub orders: Vec<FirmQuoteOrder>,
    #[serde(rename = "widgetFee")]
    pub widget_fee: WidgetFee,
    #[serde(rename = "widgetFeeSignature")]
    pub widget_fee_signature: String,
    pub recipient: String,
    #[serde(rename = "amountIn")]
    pub amount_in: String,
    #[serde(rename = "amountOut")]
    pub amount_out: String,
    #[serde(rename = "amountOutBeforeFee")]
    pub amount_out_before_fee: String,
    #[serde(rename = "fallbackSwapDataArray")]
    pub fallback_swap_data_array: Option<serde_json::Value>,
    #[serde(rename = "tokenTransferFeeOnPercent")]
    pub token_transfer_fee_on_percent: f64,
    #[serde(rename = "txRequest")]
    pub tx_request: TxRequest,
    pub source: Vec<u32>,
    #[serde(rename = "errorMessage")]
    pub error_message: String,
    #[serde(rename = "router_version")]
    pub router_version: String,
    #[serde(rename = "toWrap")]
    pub to_wrap: bool,
    #[serde(rename = "toUnwrap")]
    pub to_unwrap: bool,
    #[serde(rename = "amountInOffset")]
    pub amount_in_offset: u32,
    #[serde(rename = "amountOutMinimumOffset")]
    pub amount_out_minimum_offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NativeSupportedChain {
    Ethereum,
    Bsc,
    Arbitrum,
    Base,
    Monad,
    Xlayer,
    Robinhood,
    Morph,
}

impl TryFrom<tycho_common::models::Chain> for NativeSupportedChain {
    type Error = String;

    fn try_from(chain: tycho_common::models::Chain) -> Result<Self, Self::Error> {
        match chain.id() {
            1 => Ok(NativeSupportedChain::Ethereum),
            56 => Ok(NativeSupportedChain::Bsc),
            42161 => Ok(NativeSupportedChain::Arbitrum),
            8453 => Ok(NativeSupportedChain::Base),
            id => Err(format!("Chain ID {} not supported by Native API", id)),
        }
    }
}

// TODO: take intersection of the tycho supported chains and native supported chain
impl NativeSupportedChain {
    pub fn as_str(&self) -> &'static str {
        match self {
            NativeSupportedChain::Ethereum => "ethereum",
            NativeSupportedChain::Bsc => "bsc",
            NativeSupportedChain::Arbitrum => "arbitrum",
            NativeSupportedChain::Base => "base",
            NativeSupportedChain::Monad => "monad",
            NativeSupportedChain::Xlayer => "xlayer",
            NativeSupportedChain::Robinhood => "robinhood",
            NativeSupportedChain::Morph => "morph",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(address: &str) -> Bytes {
        Bytes::from_str(address).unwrap()
    }

    #[test]
    fn deserializes_native_relay_orderbook_entry() {
        let json = r#"{
            "base_symbol": "WETH",
            "quote_symbol": "USDT",
            "base_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "quote_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "minimum_in_base": 0,
            "side": "bid",
            "levels": [[0.0001, 3213.12345], [12.75786733219471, 3210.15]]
        }"#;

        let entry: NativeOrderbookEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.base_symbol, "WETH");
        assert_eq!(entry.side, NativeOrderbookSide::Bid);
        assert_eq!(entry.levels[0].quantity, 0.0001);
        assert_eq!(entry.levels[0].price, 3213.12345);
    }

    #[test]
    fn calculates_tvl_as_average_bid_ask_quote_value() {
        let price_data = NativePriceData {
            base_symbol: "WETH".to_string(),
            quote_symbol: "USDC".to_string(),
            base_address: addr("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            quote_address: addr("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            minimum_in_base: 0.0,
            bids: vec![
                NativePriceLevel { quantity: 1.0, price: 2000.0 },
                NativePriceLevel { quantity: 2.0, price: 1999.0 },
            ],
            asks: vec![
                NativePriceLevel { quantity: 1.5, price: 2001.0 },
                NativePriceLevel { quantity: 1.0, price: 2002.0 },
            ],
        };

        assert!((price_data.calculate_tvl(None) - 5500.75).abs() < 0.01);
    }

    #[test]
    fn normalizes_tvl_through_quote_token_market() {
        let tamara = addr("0x1234567890123456789012345678901234567890");
        let usdc = addr("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let price_data_eth_tamara = NativePriceData {
            base_symbol: "WETH".to_string(),
            quote_symbol: "TAMARA".to_string(),
            base_address: addr("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            quote_address: tamara.clone(),
            minimum_in_base: 0.0,
            bids: vec![NativePriceLevel { quantity: 3.0, price: 100.0 }],
            asks: vec![NativePriceLevel { quantity: 3.0, price: 100.0 }],
        };
        let price_data_tamara_usdc = NativePriceData {
            base_symbol: "TAMARA".to_string(),
            quote_symbol: "USDC".to_string(),
            base_address: tamara,
            quote_address: usdc,
            minimum_in_base: 0.0,
            bids: vec![NativePriceLevel { quantity: 300.0, price: 9.0 }],
            asks: vec![NativePriceLevel { quantity: 300.0, price: 11.0 }],
        };

        assert_eq!(price_data_eth_tamara.calculate_tvl(Some(&price_data_tamara_usdc)), 3000.0);
    }
}
