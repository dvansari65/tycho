use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use alloy::{
    primitives::{Address, U256},
    sol_types::SolValue,
};
use tokio::runtime::Handle;
use tycho_common::{
    models::{protocol::GetAmountOutParams, Chain},
    Bytes,
};

use crate::encoding::{
    errors::EncodingError,
    evm::utils::{bytes_to_address, create_encoding_runtime, on_blocking_thread, SafeRuntime},
    models::{EncodingContext, Swap},
    swap_encoder::SwapEncoder,
};

fn parse_quote_value(value: &Bytes) -> Result<U256, EncodingError> {
    let value = std::str::from_utf8(value.as_ref()).map_err(|e| {
        EncodingError::InvalidInput(format!("Native quote value must be UTF-8 decimal bytes: {e}"))
    })?;

    U256::from_str_radix(value, 10)
        .map_err(|e| EncodingError::InvalidInput(format!("Invalid Native quote value: {e}")))
}

#[derive(Clone)]
pub struct NativeSwapEncoder {
    executor_address: Bytes,
    allowed_targets: HashSet<Address>,
    runtime_handle: Handle,
    #[allow(dead_code)]
    runtime: SafeRuntime,
}

impl SwapEncoder for NativeSwapEncoder {
    fn new(
        executor_address: Bytes,
        _chain: Chain,
        config: Option<HashMap<String, String>>,
    ) -> Result<Self, EncodingError> {
        let config = config
            .ok_or_else(|| EncodingError::FatalError("Native config is empty".to_string()))?;
        let mut allowed_targets = HashSet::new();
        for key in ["router_v4", "router_v3", "credit_vault"] {
            let value = config.get(key).ok_or_else(|| {
                EncodingError::FatalError(format!("Missing {key} in Native config"))
            })?;
            let address = Address::from_str(value).map_err(|e| {
                EncodingError::FatalError(format!("Invalid {key} in Native config: {e}"))
            })?;

            if address == Address::ZERO {
                if key == "router_v3" {
                    continue;
                }
                return Err(EncodingError::FatalError(format!(
                    "Native {key} cannot be the zero address"
                )));
            }
            allowed_targets.insert(address);
        }

        let (runtime_handle, runtime) = create_encoding_runtime()?;
        Ok(Self { executor_address, allowed_targets, runtime_handle, runtime })
    }

    fn encode_swap(
        &self,
        swap: &Swap,
        encoding_context: &EncodingContext,
    ) -> Result<Vec<u8>, EncodingError> {
        let protocol_state = swap
            .protocol_state()
            .as_ref()
            .ok_or_else(|| {
                EncodingError::FatalError("protocol_state is required for Native".to_string())
            })?;

        let amount_in = swap
            .estimated_amount_in()
            .as_ref()
            .ok_or(EncodingError::FatalError(
                "Estimated amount in is mandatory for a Native swap".to_string(),
            ))?
            .clone();

        let sender = encoding_context
            .router_address
            .clone()
            .ok_or(EncodingError::FatalError(
                "The router address is needed to perform a Native swap".to_string(),
            ))?;

        let signed_quote = on_blocking_thread(|| {
            self.runtime_handle.block_on(async {
                protocol_state
                    .as_indicatively_priced()?
                    .request_signed_quote(GetAmountOutParams {
                        amount_in,
                        token_in: swap.token_in().address.clone(),
                        token_out: swap.token_out().address.clone(),
                        sender: sender.clone(),
                        receiver: sender,
                    })
                    .await
            })
        })??;
        let value_bytes = signed_quote
            .quote_attributes
            .get("value")
            .ok_or(EncodingError::FatalError(
                "Native quote must have a value attribute".to_string(),
            ))?;

        // Native returns `value` as a decimal string. Reject malformed values rather than
        // silently encoding zero and dropping required msg.value for native-input swaps.
        let value = parse_quote_value(value_bytes)?;

        let target_bytes = signed_quote
            .quote_attributes
            .get("target")
            .ok_or(EncodingError::FatalError(
                "Native quote must have a target attribute".to_string(),
            ))?;

        let target = bytes_to_address(target_bytes)?;
        if !self.allowed_targets.contains(&target) {
            return Err(EncodingError::InvalidInput(format!(
                "Native quote target {target} is not configured for this chain"
            )));
        }

        let calldata = signed_quote
            .quote_attributes
            .get("calldata")
            .ok_or(EncodingError::FatalError(
                "Native quote must have a calldata attribute".to_string(),
            ))?;

        // We must translate Tycho's internal `address(0)` representation to the standard
        // EVM `0xEeeee...` address used by TychoRouter so the Executor correctly processes Native
        // ETH.
        let token_in = crate::encoding::evm::utils::convert_to_router_token(bytes_to_address(
            &swap.token_in().address,
        )?);
        let token_out = crate::encoding::evm::utils::convert_to_router_token(bytes_to_address(
            &swap.token_out().address,
        )?);

        // Encode packed data for the executor
        // Format: tokenIn | tokenOut | target | value | native_calldata[..]
        // 20 bytes + 20 bytes + 20 bytes + 32 bytes + dynamic length
        // We pack tokenIn and tokenOut at the very beginning so the Solidity NativeExecutor
        // can easily slice them out in `getTransferData` without parsing the opaque
        // `native_calldata`.
        let args = (token_in, token_out, target, value, &calldata[..]);
        Ok(args.abi_encode_packed())
    }

    fn executor_address(&self) -> &Bytes {
        &self.executor_address
    }

    fn clone_box(&self) -> Box<dyn SwapEncoder> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, sync::Arc};

    use alloy::hex::encode;
    use num_bigint::BigUint;
    use tycho_common::models::protocol::ProtocolComponent;

    use super::*;
    use crate::encoding::{
        evm::{swap_encoder::native::NativeSwapEncoder, testing_utils::MockRFQState},
        models::{default_token, Swap},
    };

    fn native_config() -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            ("router_v4".to_string(), "0x8a2ddc0461Fcf96F81a05529Bed540d4f1eb2a00".to_string()),
            ("router_v3".to_string(), "0xa540ec8C73322200d68E1B86c471A5C850854f22".to_string()),
            ("credit_vault".to_string(), "0xe3D41d19564922C9952f692C5Dd0563030f5f2EF".to_string()),
        ]))
    }

    #[test]
    fn test_encode_native_single_fails_without_protocol_data() {
        let native_component = ProtocolComponent {
            id: String::from("native-rfq"),
            protocol_system: String::from("rfq:native"),
            ..Default::default()
        };

        let token_in = Bytes::from("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_out = Bytes::from("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");

        let swap = Swap::new(
            native_component,
            default_token(token_in.clone()),
            default_token(token_out.clone()),
            BigUint::ZERO,
        )
        .with_estimated_amount_in(BigUint::from_str("3000000000").unwrap());

        let encoding_context = EncodingContext {
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in.clone(),
            group_token_out: token_out.clone(),
        };

        let encoder = NativeSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            native_config(),
        )
        .unwrap();
        encoder
            .encode_swap(&swap, &encoding_context)
            .expect_err("Should returned an error if the swap has no protocol state");
    }

    #[test]
    fn test_encode_native_single_with_protocol_state() {
        let quote_amount_out = BigUint::from_str("1000000000000000000").unwrap();

        let native_component = ProtocolComponent {
            id: String::from("native-rfq"),
            protocol_system: String::from("rfq:native"),
            ..Default::default()
        };

        let target_address = "0x8a2ddc0461Fcf96F81a05529Bed540d4f1eb2a00";
        let target_bytes = Bytes::from_str(target_address).unwrap();
        let calldata_hex = "0947c2d900000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000000";
        let calldata_bytes = Bytes::from(hex::decode(calldata_hex).unwrap());
        let value_bytes = Bytes::from(b"0".to_vec());

        let native_quote_data = vec![
            ("target".to_string(), target_bytes.clone()),
            ("calldata".to_string(), calldata_bytes.clone()),
            ("value".to_string(), value_bytes),
        ];

        let native_state =
            MockRFQState { quote_amount_out, quote_data: native_quote_data.into_iter().collect() };

        let token_in = Bytes::from("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_out = Bytes::from("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");

        let swap = Swap::new(
            native_component,
            default_token(token_in.clone()),
            default_token(token_out.clone()),
            BigUint::ZERO,
        )
        .with_estimated_amount_in(BigUint::from_str("3000000000").unwrap())
        .with_protocol_state(Arc::new(native_state));

        let encoding_context = EncodingContext {
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in.clone(),
            group_token_out: token_out.clone(),
        };

        let encoder = NativeSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            native_config(),
        )
        .unwrap();

        let encoded_swap = encoder
            .encode_swap(&swap, &encoding_context)
            .unwrap();
        let hex_swap = encode(&encoded_swap);

        let expected_swap = format!(
            "{}{}{}{}{}",
            "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // token_in (20 bytes, lowercase, no 0x)
            "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // token_out (20 bytes)
            target_address
                .to_lowercase()
                .trim_start_matches("0x"), // target (20 bytes)
            "0000000000000000000000000000000000000000000000000000000000000000", /* value as 32
                                                         * bytes — but
                                                         * check length
                                                         * below */
            calldata_hex
        );
        assert_eq!(hex_swap, expected_swap);
    }

    #[test]
    fn test_encode_native_rejects_unconfigured_quote_target() {
        let target_bytes = Bytes::from_str("0xb2d1F342D2049684Fb2f8c4eF320633415598333").unwrap();
        let native_state = MockRFQState {
            quote_amount_out: BigUint::from(1_000_000u64),
            quote_data: HashMap::from([
                ("target".to_string(), target_bytes),
                ("calldata".to_string(), Bytes::from(vec![0x09, 0x47, 0xc2, 0xd9])),
                ("value".to_string(), Bytes::from(b"0".to_vec())),
            ]),
        };
        let token_in = Bytes::from("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_out = Bytes::from("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");
        let swap = Swap::new(
            ProtocolComponent { protocol_system: "rfq:native".to_string(), ..Default::default() },
            default_token(token_in.clone()),
            default_token(token_out.clone()),
            BigUint::ZERO,
        )
        .with_estimated_amount_in(BigUint::from(3_000_000_000u64))
        .with_protocol_state(Arc::new(native_state));
        let context = EncodingContext {
            router_address: Some(Bytes::zero(20)),
            group_token_in: token_in,
            group_token_out: token_out,
        };
        let encoder = NativeSwapEncoder::new(
            Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4"),
            Chain::Ethereum,
            native_config(),
        )
        .unwrap();

        let error = encoder
            .encode_swap(&swap, &context)
            .unwrap_err();

        assert!(matches!(error, EncodingError::InvalidInput(message) if message.contains(
            "is not configured for this chain"
        )));
    }

    #[test]
    fn test_native_config_requires_v4_and_vault_but_allows_zero_v3() {
        let executor = Bytes::from("0x543778987b293C7E8Cf0722BB2e935ba6f4068D4");

        assert!(matches!(
            NativeSwapEncoder::new(executor.clone(), Chain::Ethereum, None),
            Err(EncodingError::FatalError(message)) if message.contains("config is empty")
        ));

        let mut config = native_config().unwrap();
        config.remove("credit_vault");
        assert!(matches!(
            NativeSwapEncoder::new(executor.clone(), Chain::Ethereum, Some(config)),
            Err(EncodingError::FatalError(message)) if message.contains("Missing credit_vault")
        ));

        let mut config = native_config().unwrap();
        config.insert(
            "router_v3".to_string(),
            "0x0000000000000000000000000000000000000000".to_string(),
        );
        let encoder = NativeSwapEncoder::new(executor, Chain::Ethereum, Some(config)).unwrap();
        assert_eq!(encoder.allowed_targets.len(), 2);
    }

    #[test]
    fn test_parse_quote_value_accepts_decimal_utf8() {
        let value = Bytes::from(b"1000000000000000000".to_vec());

        assert_eq!(parse_quote_value(&value).unwrap(), U256::from(1_000_000_000_000_000_000u64));
    }

    #[test]
    fn test_parse_quote_value_rejects_binary_value() {
        let error = parse_quote_value(&Bytes::from(vec![0])).unwrap_err();

        assert!(matches!(error, EncodingError::InvalidInput(_)));
    }

    #[test]
    fn test_parse_quote_value_rejects_invalid_utf8() {
        let error = parse_quote_value(&Bytes::from(vec![0xff])).unwrap_err();

        assert!(matches!(error, EncodingError::InvalidInput(_)));
    }

    #[test]
    fn test_parse_quote_value_rejects_invalid_decimal() {
        let error = parse_quote_value(&Bytes::from(b"not-a-number".to_vec())).unwrap_err();

        assert!(matches!(error, EncodingError::InvalidInput(_)));
    }

    #[test]
    fn test_parse_quote_value_rejects_u256_overflow() {
        let overflow = Bytes::from(
            b"115792089237316195423570985008687907853269984665640564039457584007913129639936"
                .to_vec(),
        );
        let error = parse_quote_value(&overflow).unwrap_err();

        assert!(matches!(error, EncodingError::InvalidInput(_)));
    }
}
