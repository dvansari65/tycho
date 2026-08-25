//! Finds the registry slots to override for a pAMM quote, per block.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use alloy::primitives::{
    map::{AddressHashMap, B256HashMap},
    Address, B256, U256,
};
use tokio::sync::watch;
use tracing::{debug, info, warn};
use tycho_simulation::evm::override_stream::{titan::default_providers, OverrideSnapshot};

/// The pAMM protocol systems collected from the stream.
const OVERRIDE_STREAM_PROTOCOLS: [&str; 1] = ["vm:fermiswap"];

/// How many quoted blocks are kept.
const RETAINED_BLOCKS: usize = 8;

/// Storage overrides keyed by contract address, then by storage slot.
type Storage = HashMap<Address, HashMap<U256, U256>>;

/// The overrides of the most recent quoted blocks, newest at the back.
type Blocks = Arc<RwLock<VecDeque<(u64, Storage)>>>;

/// pAMM oracle overrides per quoted block.
pub struct OracleOverrides {
    blocks: Blocks,
}

impl OracleOverrides {
    /// Collects Titan's state overrides in the background. `None` when the stream serves no pAMM
    /// channel.
    pub fn spawn() -> Option<Self> {
        let providers = default_providers(
            OVERRIDE_STREAM_PROTOCOLS
                .iter()
                .map(|protocol| protocol.to_string()),
        );
        let mut receivers = Vec::new();
        for protocol in OVERRIDE_STREAM_PROTOCOLS {
            match providers
                .get(protocol)
                .and_then(|provider| provider.subscribe(protocol))
            {
                Some(receiver) => receivers.push((protocol, receiver)),
                None => warn!(protocol, "Titan's state override stream serves no channel"),
            }
        }
        if receivers.is_empty() {
            return None;
        }

        let blocks: Blocks = Arc::new(RwLock::new(VecDeque::new()));
        for (protocol, receiver) in receivers {
            tokio::spawn(collect(protocol, receiver, blocks.clone()));
        }
        info!("Collecting pAMM oracle overrides from Titan's state override stream");
        Some(Self { blocks })
    }

    /// The overrides Titan published for `block`, or `None` when none were collected for it.
    pub fn for_block(&self, block: u64) -> Option<AddressHashMap<B256HashMap<B256>>> {
        let blocks = match self.blocks.read() {
            Ok(blocks) => blocks,
            Err(e) => {
                warn!("Failed to acquire read lock on pAMM oracle overrides: {e}");
                return None;
            }
        };
        blocks
            .iter()
            .find(|(number, _)| *number == block)
            .map(|(_, storage)| slot_overrides(storage))
    }
}

/// Records every snapshot of one protocol's channel.
async fn collect(
    protocol: &'static str,
    mut receiver: watch::Receiver<OverrideSnapshot>,
    blocks: Blocks,
) {
    loop {
        if receiver.changed().await.is_err() {
            warn!(protocol, "Titan override channel closed; stopping oracle override collection");
            return;
        }
        let (block_number, storage) = {
            let snapshot = receiver.borrow_and_update();
            (snapshot.block_number, snapshot.storage.clone())
        };
        let Some(block_number) = block_number else {
            debug!(protocol, "Titan override snapshot carries no block number; skipping");
            continue;
        };
        if storage.is_empty() {
            continue;
        }
        record(&blocks, block_number, &storage);
    }
}

/// Merges one snapshot into `block`'s entry, evicting the oldest block past [`RETAINED_BLOCKS`].
fn record(blocks: &Blocks, block: u64, storage: &Storage) {
    let mut blocks = match blocks.write() {
        Ok(blocks) => blocks,
        Err(e) => {
            warn!("Failed to acquire write lock on pAMM oracle overrides: {e}");
            return;
        }
    };
    let entry = match blocks
        .iter_mut()
        .find(|(number, _)| *number == block)
    {
        Some((_, entry)) => entry,
        None => {
            blocks.push_back((block, Storage::new()));
            while blocks.len() > RETAINED_BLOCKS {
                blocks.pop_front();
            }
            &mut blocks
                .back_mut()
                .expect("the entry just pushed is present")
                .1
        }
    };
    for (account, slots) in storage.iter() {
        entry
            .entry(*account)
            .or_default()
            .extend(
                slots
                    .iter()
                    .map(|(slot, value)| (*slot, *value)),
            );
    }
}

/// Converts collected storage into the `B256` slots the execution simulation takes.
fn slot_overrides(storage: &Storage) -> AddressHashMap<B256HashMap<B256>> {
    let mut overrides = AddressHashMap::default();
    for (account, slots) in storage {
        let slots: B256HashMap<B256> = slots
            .iter()
            .map(|(slot, value)| {
                (B256::from(slot.to_be_bytes::<32>()), B256::from(value.to_be_bytes::<32>()))
            })
            .collect();
        overrides.insert(*account, slots);
    }
    overrides
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: Address =
        alloy::primitives::address!("da7afeed01fe625cf15d187a19f94b45f00b8c5f");
    const VENUE: Address = alloy::primitives::address!("160141a205f5ddcf096ba3f48b7ed21eb52c62ea");

    fn storage(account: Address, slot: u64, value: u64) -> Storage {
        HashMap::from([(account, HashMap::from([(U256::from(slot), U256::from(value))]))])
    }

    fn overrides() -> OracleOverrides {
        OracleOverrides { blocks: Arc::new(RwLock::new(VecDeque::new())) }
    }

    fn slot_value(
        overrides: &AddressHashMap<B256HashMap<B256>>,
        account: Address,
        slot: u64,
    ) -> Option<B256> {
        overrides
            .get(&account)?
            .get(&B256::from(U256::from(slot).to_be_bytes::<32>()))
            .copied()
    }

    #[test]
    fn several_venues_write_one_block() {
        let overrides = overrides();
        record(&overrides.blocks, 100, &storage(REGISTRY, 1, 11));
        record(&overrides.blocks, 100, &storage(REGISTRY, 2, 22));
        record(&overrides.blocks, 100, &storage(VENUE, 1, 33));

        let block = overrides
            .for_block(100)
            .expect("block 100 was recorded");
        assert_eq!(slot_value(&block, REGISTRY, 1), Some(B256::from(U256::from(11))));
        assert_eq!(slot_value(&block, REGISTRY, 2), Some(B256::from(U256::from(22))));
        assert_eq!(slot_value(&block, VENUE, 1), Some(B256::from(U256::from(33))));
    }

    #[test]
    fn a_later_frame_rewrites_a_slot() {
        let overrides = overrides();
        record(&overrides.blocks, 100, &storage(REGISTRY, 1, 11));
        record(&overrides.blocks, 100, &storage(REGISTRY, 1, 99));

        let block = overrides
            .for_block(100)
            .expect("block 100 was recorded");
        assert_eq!(slot_value(&block, REGISTRY, 1), Some(B256::from(U256::from(99))));
    }

    #[test]
    fn two_blocks_write_the_same_slot() {
        let overrides = overrides();
        record(&overrides.blocks, 100, &storage(REGISTRY, 1, 11));
        record(&overrides.blocks, 101, &storage(REGISTRY, 1, 22));

        assert_eq!(
            slot_value(
                &overrides
                    .for_block(100)
                    .expect("recorded"),
                REGISTRY,
                1
            ),
            Some(B256::from(U256::from(11)))
        );
        assert_eq!(
            slot_value(
                &overrides
                    .for_block(101)
                    .expect("recorded"),
                REGISTRY,
                1
            ),
            Some(B256::from(U256::from(22)))
        );
    }

    #[test]
    fn a_block_that_was_never_recorded() {
        let overrides = overrides();
        record(&overrides.blocks, 100, &storage(REGISTRY, 1, 11));

        assert!(overrides.for_block(102).is_none());
    }

    #[test]
    fn more_blocks_than_the_cache_holds() {
        let overrides = overrides();
        for block in 0..=RETAINED_BLOCKS as u64 {
            record(&overrides.blocks, block, &storage(REGISTRY, 1, block));
        }

        assert!(overrides.for_block(0).is_none());
        assert!(overrides
            .for_block(RETAINED_BLOCKS as u64)
            .is_some());
        assert_eq!(
            overrides
                .blocks
                .read()
                .expect("lock")
                .len(),
            RETAINED_BLOCKS
        );
    }
}
