use std::collections::HashMap;

use alloy::{primitives::U256, sol_types::SolValue};
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

#[derive(Clone)]
pub struct NativeSwapEncoder {
    executor_address: Bytes,
    runtime_handle: Handle,
    #[allow(dead_code)]
    runtime: SafeRuntime,
}

impl SwapEncoder for NativeSwapEncoder {
    fn new(
        executor_address: Bytes,
        _chain: Chain,
        _config: Option<HashMap<String, String>>,
    ) -> Result<Self, EncodingError> {
        let (runtime_handle, runtime) = create_encoding_runtime()?;
        Ok(Self { executor_address, runtime_handle, runtime })
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

        // The Native API returns `value` as a string (e.g. "0").
        // We parse it as UTF-8 string bytes into a U256 so it can be cleanly packed for the
        // executor.
        let value_str = String::from_utf8_lossy(value_bytes.as_ref());
        let value = U256::from_str_radix(&value_str, 10).unwrap_or_default();

        let target_bytes = signed_quote
            .quote_attributes
            .get("target")
            .ok_or(EncodingError::FatalError(
                "Native quote must have a target attribute".to_string(),
            ))?;

        let target = bytes_to_address(target_bytes)?;

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
        Some(HashMap::from([(
            "native_router_address".to_string(),
            "0x55084eE0fEf03f14a305cd24286359A35D735151".to_string(),
        )]))
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

        let target_address = "0xb2d1F342D2049684Fb2f8c4eF320633415598333";
        let target_bytes = Bytes::from_str(target_address).unwrap();
        let calldata_hex = "af70653900000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000000";
        let calldata_bytes = Bytes::from(hex::decode(calldata_hex).unwrap());
        let value_bytes = Bytes::from("00");

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
}
