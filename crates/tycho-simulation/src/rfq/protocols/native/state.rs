use std::{any::Any, collections::HashMap, fmt};

use async_trait::async_trait;
use num_bigint::BigUint;
use num_traits::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use tycho_common::{
    dto::ProtocolStateDelta,
    models::{protocol::GetAmountOutParams, token::Token},
    simulation::{
        errors::{SimulationError, TransitionError},
        indicatively_priced::{IndicativelyPriced, SignedQuote},
        protocol_sim::{Balances, GetAmountOutResult, ProtocolSim},
    },
    Bytes,
};

use crate::rfq::{
    client::RFQClient,
    protocols::native::{client::NativeClient, models::NativePriceData},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct NativeState {
    pub base_token: Token,
    pub quote_token: Token,
    pub book: NativePriceData,
    pub client: NativeClient,
}

impl fmt::Debug for NativeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeState")
            .field("base_token", &self.base_token)
            .field("quote_token", &self.quote_token)
            .finish_non_exhaustive()
    }
}

impl NativeState {
    pub fn new(
        base_token: Token,
        quote_token: Token,
        book: NativePriceData,
        client: NativeClient,
    ) -> Self {
        NativeState { base_token, quote_token, book, client }
    }
}

#[typetag::serde]
impl ProtocolSim for NativeState {
    fn fee(&self) -> f64 {
        0.0
    }

    fn spot_price(&self, base: &Token, quote: &Token) -> Result<f64, SimulationError> {
        let best_bid = self
            .book
            .bids
            .first()
            .map(|lvl| lvl.price);
        let best_ask = self
            .book
            .asks
            .first()
            .map(|lvl| lvl.price);

        let average_price = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => (bid + ask) / 2.0,
            (Some(bid), None) => bid,
            (None, Some(ask)) => ask,
            (None, None) => {
                return Err(SimulationError::RecoverableError("No liquidity".to_string()))
            }
        };

        if base.address == self.quote_token.address && quote.address == self.base_token.address {
            Ok(1.0 / average_price)
        } else if quote.address == self.quote_token.address &&
            base.address == self.base_token.address
        {
            Ok(average_price)
        } else {
            Err(SimulationError::RecoverableError(format!(
                "Invalid token addresses: {}, {}",
                base.address, quote.address
            )))
        }
    }

    fn get_amount_out(
        &self,
        amount_in: BigUint,
        token_in: &Token,
        token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        let is_sell_base = token_in.address == self.base_token.address &&
            token_out.address == self.quote_token.address;
        let is_sell_quote = token_in.address == self.quote_token.address &&
            token_out.address == self.base_token.address;

        if !is_sell_base && !is_sell_quote {
            return Err(SimulationError::InvalidInput(
                format!(
                    "Invalid token addresses. Got in={}, out={}",
                    token_in.address, token_out.address
                ),
                None,
            ));
        }

        let amount_in_f64 = amount_in.to_f64().ok_or_else(|| {
            SimulationError::RecoverableError("Can't convert amount in to f64".into())
        })? / 10f64.powi(token_in.decimals as i32);

        let levels = if is_sell_base {
            self.book.bids.clone()
        } else {
            NativePriceData::invert_price_levels(&self.book.asks)
        };

        if levels.is_empty() {
            return Err(SimulationError::RecoverableError("No liquidity".into()));
        }

        let (amount_out_f64, remaining) =
            NativePriceData::get_amount_out_from_levels(amount_in_f64, &levels);

        if remaining > 0.0 {
            return Err(SimulationError::InvalidInput(
                format!("Pool has not enough liquidity to support complete swap. Input amount: {}, consumed: {}", amount_in_f64, amount_in_f64 - remaining),
                None,
            ));
        }

        let amount_base = if is_sell_base { amount_in_f64 } else { amount_out_f64 };
        if self.book.minimum_in_base > 0.0 && amount_base < self.book.minimum_in_base {
            return Err(SimulationError::RecoverableError(format!(
                "Amount below minimum. Base amount: {}, min amount: {}",
                amount_base, self.book.minimum_in_base
            )));
        }

        let res = GetAmountOutResult {
            amount: BigUint::from_f64(amount_out_f64 * 10f64.powi(token_out.decimals as i32))
                .ok_or_else(|| {
                    SimulationError::RecoverableError("Can't convert amount out to BigUInt".into())
                })?,
            gas: BigUint::from(134_000u64), // Approximate standard gas for Native swap
            new_state: self.clone_box(),
        };

        Ok(res)
    }

    fn get_limits(
        &self,
        sell_token: Bytes,
        buy_token: Bytes,
    ) -> Result<(BigUint, BigUint), SimulationError> {
        let is_sell_base =
            sell_token == self.base_token.address && buy_token == self.quote_token.address;
        let is_sell_quote =
            sell_token == self.quote_token.address && buy_token == self.base_token.address;

        if !is_sell_base && !is_sell_quote {
            return Err(SimulationError::InvalidInput(
                format!("Invalid token addresses. Got sell={}, buy={}", sell_token, buy_token),
                None,
            ));
        }

        let levels = if is_sell_base {
            self.book.bids.clone()
        } else {
            NativePriceData::invert_price_levels(&self.book.asks)
        };

        if levels.is_empty() {
            return Err(SimulationError::RecoverableError("No liquidity".into()));
        }

        let (total_sell_amount, total_buy_amount) =
            levels
                .iter()
                .fold((0.0, 0.0), |(sell_sum, buy_sum), level| {
                    (sell_sum + level.quantity, buy_sum + level.quantity * level.price)
                });

        let sell_decimals =
            if is_sell_base { self.base_token.decimals } else { self.quote_token.decimals };
        let buy_decimals =
            if is_sell_base { self.quote_token.decimals } else { self.base_token.decimals };

        let sell_limit = BigUint::from_f64(total_sell_amount * 10f64.powi(sell_decimals as i32))
            .ok_or_else(|| {
                SimulationError::RecoverableError("Can't convert limit to BigUInt".into())
            })?;
        let buy_limit = BigUint::from_f64(total_buy_amount * 10f64.powi(buy_decimals as i32))
            .ok_or_else(|| {
                SimulationError::RecoverableError("Can't convert limit to BigUInt".into())
            })?;

        Ok((sell_limit, buy_limit))
    }

    fn delta_transition(
        &mut self,
        _delta: ProtocolStateDelta,
        _tokens: &HashMap<Bytes, Token>,
        _balances: &Balances,
    ) -> Result<(), TransitionError> {
        Err(TransitionError::DecodeError("Not implemented".into()))
    }

    fn clone_box(&self) -> Box<dyn ProtocolSim> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn eq(&self, other: &dyn ProtocolSim) -> bool {
        if let Some(other_state) = other
            .as_any()
            .downcast_ref::<NativeState>()
        {
            self.base_token == other_state.base_token &&
                self.quote_token == other_state.quote_token &&
                self.book == other_state.book
        } else {
            false
        }
    }

    fn as_indicatively_priced(&self) -> Result<&dyn IndicativelyPriced, SimulationError> {
        Ok(self)
    }
}

#[async_trait]
impl IndicativelyPriced for NativeState {
    async fn request_signed_quote(
        &self,
        params: GetAmountOutParams,
    ) -> Result<SignedQuote, SimulationError> {
        Ok(self
            .client
            .request_binding_quote(&params)
            .await?)
    }
}
