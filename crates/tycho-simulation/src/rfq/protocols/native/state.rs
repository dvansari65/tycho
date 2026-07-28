use std::{any::Any, collections::HashMap, fmt};

use async_trait::async_trait;
use num_bigint::BigUint;
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
        _amount_in: BigUint,
        _token_in: &Token,
        _token_out: &Token,
    ) -> Result<GetAmountOutResult, SimulationError> {
        // Basic stub since amount out calculation requires proper decimal shifting and level
        // walking
        Err(SimulationError::RecoverableError("Not implemented".into()))
    }

    fn get_limits(
        &self,
        _sell_token: Bytes,
        _buy_token: Bytes,
    ) -> Result<(BigUint, BigUint), SimulationError> {
        // Basic stub
        Err(SimulationError::RecoverableError("Not implemented".into()))
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
