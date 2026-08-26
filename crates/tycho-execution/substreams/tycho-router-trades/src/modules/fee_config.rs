//! Fee configuration tracking: FeeCalculator admin events and router fee-calculator rotations.
use anyhow::Result;
use substreams::{
    prelude::*,
    store::{StoreDelete, StoreSetString},
};
use substreams_ethereum::{
    pb::eth::v2::{Block, Log},
    Event,
};

use super::{block_timestamp, events, hex_addr, keys};
use crate::{
    abi::{
        fee_calculator::events as fc, fee_calculator_v3_0::events as fc_v3_0,
        tycho_router_v3_0::events as router_v3_0, tycho_router_v3_1::events as router_v3_1,
    },
    params::Params,
    pb::tycho::router::v1::{FeeConfigEvent, FeeConfigEvents},
};

/// Emits every FeeCalculator admin event and every fee-calculator rotation on a known router.
///
/// FeeCalculator events are matched by topic from any emitter (the FeeCalculator constructor
/// emits nothing, so the set of calculators is only known through router events and params);
/// the store keys them by emitter so unrelated emitters never influence a trade.
#[substreams::handlers::map]
pub fn map_fee_config_events(params: String, block: Block) -> Result<FeeConfigEvents> {
    let params = Params::parse(&params)?;
    let timestamp = block_timestamp(&block);
    let mut events = Vec::new();
    for tx in block.transactions() {
        for log in tx
            .receipt
            .as_ref()
            .map(|r| r.logs.as_slice())
            .unwrap_or_default()
        {
            let is_router = params.router(&log.address).is_some();
            let decoded =
                if is_router { decode_router_event(log) } else { decode_fee_calculator_event(log) };
            let Some((event, client, old_value, new_value)) = decoded else {
                continue;
            };
            events.push(FeeConfigEvent {
                chain: params.chain.clone(),
                block_number: block.number,
                block_timestamp: timestamp,
                tx_hash: tx.hash.clone(),
                log_index: log.index,
                ordinal: log.ordinal,
                emitter: log.address.clone(),
                event: event.to_string(),
                client,
                old_value,
                new_value,
            });
        }
    }
    Ok(FeeConfigEvents { events })
}

type Decoded = (&'static str, Vec<u8>, String, String);

fn decode_router_event(log: &Log) -> Option<Decoded> {
    if let Some(ev) = router_v3_1::FeeCalculatorActivated::match_and_decode(log) {
        return Some((
            events::FEE_CALCULATOR_ACTIVATED,
            Vec::new(),
            hex_addr(&ev.old_calculator),
            hex_addr(&ev.new_calculator),
        ));
    }
    if let Some(ev) = router_v3_1::FeeCalculatorSet::match_and_decode(log) {
        return Some((
            events::FEE_CALCULATOR_SET,
            Vec::new(),
            ev.timelock_expires_at.to_string(),
            hex_addr(&ev.fee_calculator),
        ));
    }
    if let Some(ev) = router_v3_0::FeeCalculatorUpdated::match_and_decode(log) {
        return Some((
            events::FEE_CALCULATOR_UPDATED,
            Vec::new(),
            hex_addr(&ev.old_calculator),
            hex_addr(&ev.new_calculator),
        ));
    }
    None
}

fn decode_fee_calculator_event(log: &Log) -> Option<Decoded> {
    macro_rules! bps_event {
        ($ty:ty, $name:expr) => {
            if let Some(ev) = <$ty>::match_and_decode(log) {
                return Some((
                    $name,
                    Vec::new(),
                    ev.old_fee_bps.to_string(),
                    ev.new_fee_bps.to_string(),
                ));
            }
        };
    }
    macro_rules! custom_bps_event {
        ($ty:ty, $name:expr) => {
            if let Some(ev) = <$ty>::match_and_decode(log) {
                return Some((
                    $name,
                    ev.client,
                    ev.old_fee_bps.to_string(),
                    ev.new_fee_bps.to_string(),
                ));
            }
        };
    }
    macro_rules! removed_event {
        ($ty:ty, $name:expr) => {
            if let Some(ev) = <$ty>::match_and_decode(log) {
                return Some(($name, ev.client, String::new(), String::new()));
            }
        };
    }
    macro_rules! receiver_event {
        ($ty:ty) => {
            if let Some(ev) = <$ty>::match_and_decode(log) {
                return Some((
                    events::ROUTER_FEE_RECEIVER_UPDATED,
                    Vec::new(),
                    hex_addr(&ev.old_receiver),
                    hex_addr(&ev.new_receiver),
                ));
            }
        };
    }
    bps_event!(fc::RouterFeeOnOutputUpdated, events::ROUTER_FEE_ON_OUTPUT_UPDATED);
    bps_event!(fc_v3_0::RouterFeeOnOutputUpdated, events::ROUTER_FEE_ON_OUTPUT_UPDATED);
    bps_event!(fc::RouterFeeOnClientFeeUpdated, events::ROUTER_FEE_ON_CLIENT_FEE_UPDATED);
    bps_event!(fc_v3_0::RouterFeeOnClientFeeUpdated, events::ROUTER_FEE_ON_CLIENT_FEE_UPDATED);
    custom_bps_event!(fc::CustomRouterFeeOnOutputUpdated, events::CUSTOM_FEE_ON_OUTPUT_UPDATED);
    custom_bps_event!(
        fc_v3_0::CustomRouterFeeOnOutputUpdated,
        events::CUSTOM_FEE_ON_OUTPUT_UPDATED
    );
    custom_bps_event!(
        fc::CustomRouterFeeOnClientFeeUpdated,
        events::CUSTOM_FEE_ON_CLIENT_FEE_UPDATED
    );
    custom_bps_event!(
        fc_v3_0::CustomRouterFeeOnClientFeeUpdated,
        events::CUSTOM_FEE_ON_CLIENT_FEE_UPDATED
    );
    removed_event!(fc::CustomRouterFeeOnOutputRemoved, events::CUSTOM_FEE_ON_OUTPUT_REMOVED);
    removed_event!(fc_v3_0::CustomRouterFeeOnOutputRemoved, events::CUSTOM_FEE_ON_OUTPUT_REMOVED);
    removed_event!(fc::CustomRouterFeeOnClientFeeRemoved, events::CUSTOM_FEE_ON_CLIENT_FEE_REMOVED);
    removed_event!(
        fc_v3_0::CustomRouterFeeOnClientFeeRemoved,
        events::CUSTOM_FEE_ON_CLIENT_FEE_REMOVED
    );
    receiver_event!(fc::RouterFeeReceiverUpdated);
    receiver_event!(fc_v3_0::RouterFeeReceiverUpdated);
    if let Some(ev) = fc::PositiveSlippageToggled::match_and_decode(log) {
        return Some((
            events::POSITIVE_SLIPPAGE_TOGGLED,
            Vec::new(),
            String::new(),
            if ev.enabled { "1".to_string() } else { "0".to_string() },
        ));
    }
    None
}

/// Materialises the fee configuration so `map_trades` can resolve the bps in effect per trade.
#[substreams::handlers::store]
pub fn store_fee_config(events: FeeConfigEvents, store: StoreSetString) {
    for ev in &events.events {
        let fc = ev.emitter.as_slice();
        match ev.event.as_str() {
            events::ROUTER_FEE_ON_OUTPUT_UPDATED => {
                store.set(ev.ordinal, keys::fee_on_output(fc), &ev.new_value)
            }
            events::ROUTER_FEE_ON_CLIENT_FEE_UPDATED => {
                store.set(ev.ordinal, keys::fee_on_client_fee(fc), &ev.new_value)
            }
            events::CUSTOM_FEE_ON_OUTPUT_UPDATED => {
                store.set(ev.ordinal, keys::custom_fee_on_output(fc, &ev.client), &ev.new_value)
            }
            events::CUSTOM_FEE_ON_CLIENT_FEE_UPDATED => {
                store.set(ev.ordinal, keys::custom_fee_on_client_fee(fc, &ev.client), &ev.new_value)
            }
            events::CUSTOM_FEE_ON_OUTPUT_REMOVED => {
                store.delete_prefix(ev.ordinal as i64, &keys::custom_fee_on_output(fc, &ev.client))
            }
            events::CUSTOM_FEE_ON_CLIENT_FEE_REMOVED => store
                .delete_prefix(ev.ordinal as i64, &keys::custom_fee_on_client_fee(fc, &ev.client)),
            events::POSITIVE_SLIPPAGE_TOGGLED => {
                store.set(ev.ordinal, keys::positive_slippage(fc), &ev.new_value)
            }
            events::ROUTER_FEE_RECEIVER_UPDATED => {
                store.set(ev.ordinal, keys::fee_receiver(fc), &ev.new_value)
            }
            events::FEE_CALCULATOR_ACTIVATED | events::FEE_CALCULATOR_UPDATED => {
                store.set(ev.ordinal, keys::router_fee_calculator(fc), &ev.new_value)
            }
            events::FEE_CALCULATOR_SET => {}
            other => substreams::log::info!("ignoring unknown fee config event {}", other),
        }
    }
}
