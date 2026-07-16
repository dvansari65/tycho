use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::StreamExt;
use miette::miette;
use rand::prelude::IteratorRandom;
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{info, warn};
use tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};
use tycho_simulation::{
    price_level_stream::stream::PriceLevelStreamBuilder,
    protocol::models::{ProtocolComponent, Update},
};

use crate::{
    metrics,
    stream_processor::{StreamUpdate, UpdateType},
};

/// Streams Titan pAMM price level updates and forwards throttled, sampled [`StreamUpdate`]s.
pub struct PriceLevelStreamProcessor {
    chain: Chain,
    sample_size: usize,
    /// One sampled update is emitted per this many blocks. Titan pushes several snapshots per
    /// second, far more than is useful for validation.
    block_interval: u64,
    /// Mark the served venues stale when no Titan message arrives within this window (zero
    /// disables the watchdog). Lives here rather than on the emitted updates because throttling
    /// hides most messages from the consumer.
    stale_threshold: Duration,
}

impl PriceLevelStreamProcessor {
    /// Creates the processor, or `None` on chains the Titan price level stream does not serve
    /// (it only serves Ethereum).
    pub fn new(
        chain: Chain,
        sample_size: usize,
        block_interval: u64,
        stale_threshold: Duration,
    ) -> Option<Self> {
        if chain != Chain::Ethereum {
            return None;
        }
        Some(Self { chain, sample_size, block_interval, stale_threshold })
    }

    pub async fn run_stream(
        &self,
        all_tokens: &HashMap<Bytes, Token>,
        stream_tx: Sender<miette::Result<StreamUpdate>>,
    ) -> miette::Result<JoinHandle<()>> {
        info!("Starting price level stream processor for chain {:?}", self.chain);
        // The default venues are served under their names, auto-detection additionally serves
        // any newly streamed pAMM under its address.
        let stream = PriceLevelStreamBuilder::new()
            .with_default_pamms()
            .auto_detect(true)
            .with_tokens(all_tokens.clone())
            .build();

        let sample_size = self.sample_size;
        let block_interval = self.block_interval;
        let stale_threshold = self.stale_threshold;
        let handle = tokio::spawn(async move {
            info!("Price level stream processor started");
            tokio::pin!(stream);
            // Components seen so far. Updates announce a pair under `new_pairs` only once, so
            // sampled states of later updates must be re-joined with their components from here.
            let mut components: HashMap<String, ProtocolComponent> = HashMap::new();
            // Pairs removed since the last emission and not re-added since. Removals announced
            // by skipped messages must survive until the next emitted update.
            let mut removed: HashMap<String, ProtocolComponent> = HashMap::new();
            // Venues currently exported to the pair-count gauge, so a venue whose last pair
            // disappears drops to zero instead of freezing at its final count.
            let mut gauged_protocols: HashSet<String> = HashSet::new();
            let mut is_first_update = true;
            // The snapshot chosen for emission, held back until the stream moves past its block:
            // Titan streams many snapshots per built block, and the block's last one has the
            // least drift to what the builder finalizes, so the downstream wait for the target
            // block to land on-chain stays minimal.
            let mut chosen: Option<Update> = None;
            let mut next_emission_block = 0u64;
            loop {
                let next = if stale_threshold.is_zero() {
                    stream.next().await
                } else {
                    match tokio::time::timeout(stale_threshold, stream.next()).await {
                        Ok(next) => next,
                        Err(_) => {
                            // Titan pushes several snapshots per second, so a silent window this
                            // long means the connection is down. A dead connection emits nothing,
                            // so the watchdog must live here, where every message is seen before
                            // throttling — not with the consumer of the emitted updates.
                            for protocol in &gauged_protocols {
                                metrics::mark_protocol_stale(protocol);
                            }
                            continue;
                        }
                    }
                };
                let Some(mut update) = next else { break };
                // Receipt is the liveness signal: flip the served venues (back) to Ready.
                for protocol in &gauged_protocols {
                    metrics::mark_protocol_ready(protocol);
                }

                // Emit before folding the current message into the caches, so the emitted update
                // is a consistent view as of the chosen block's last snapshot.
                let block = update.block_number_or_timestamp;
                if let Some(mut emitted) =
                    chosen.take_if(|snapshot| block > snapshot.block_number_or_timestamp)
                {
                    // Sample random pair states and attach their components under `new_pairs`,
                    // which is where the update processor looks up components of off-chain
                    // streams.
                    emitted.states = emitted
                        .states
                        .into_iter()
                        .choose_multiple(&mut rand::rng(), sample_size)
                        .into_iter()
                        .collect();
                    emitted.new_pairs = emitted
                        .states
                        .keys()
                        .filter_map(|id| {
                            components
                                .get(id)
                                .map(|component| (id.clone(), component.clone()))
                        })
                        .collect();
                    emitted.removed_pairs = std::mem::take(&mut removed);

                    let result =
                        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                            Ok(received_at) => {
                                let stream_update = StreamUpdate {
                                    update_type: UpdateType::PriceLevelStream,
                                    update: emitted,
                                    is_first_update,
                                    received_at,
                                };
                                is_first_update = false;
                                Ok(stream_update)
                            }
                            Err(e) => Err(miette!(e).wrap_err("Error getting current timestamp")),
                        };
                    if stream_tx.send(result).await.is_err() {
                        warn!("Receiver dropped, stopping stream processor");
                        break;
                    }
                }

                // Keep the caches in sync with every message, including skipped ones, so later
                // samples always resolve. A re-added pair is no removal.
                let pairs_changed =
                    !update.new_pairs.is_empty() || !update.removed_pairs.is_empty();
                for id in update.new_pairs.keys() {
                    removed.remove(id);
                }
                components.extend(std::mem::take(&mut update.new_pairs));
                for (id, component) in std::mem::take(&mut update.removed_pairs) {
                    components.remove(&id);
                    removed.insert(id, component);
                }

                if pairs_changed {
                    let mut counts: HashMap<&String, usize> = HashMap::new();
                    for component in components.values() {
                        *counts
                            .entry(&component.protocol_system)
                            .or_default() += 1;
                    }
                    for protocol in &gauged_protocols {
                        if !counts.contains_key(protocol) {
                            metrics::record_protocol_pool_count(protocol, 0);
                        }
                    }
                    for (protocol, count) in &counts {
                        metrics::record_protocol_pool_count(protocol, *count);
                    }
                    gauged_protocols = counts.into_keys().cloned().collect();
                }

                if let Some(snapshot) = &mut chosen {
                    // A fresher snapshot of the chosen block supersedes the held one.
                    if snapshot.block_number_or_timestamp == block {
                        *snapshot = update;
                    }
                } else if block >= next_emission_block {
                    next_emission_block = block + block_interval;
                    chosen = Some(update);
                }
            }
        });
        Ok(handle)
    }
}
