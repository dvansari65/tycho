//! Retries enrichment after pending token identities have reached finalized storage.
//! No RPC work or recovery write runs on the extractor's block-processing future.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures03::{stream, StreamExt};
use metrics::{counter, gauge};
use tokio::time::{sleep, timeout, Instant};
use tracing::warn;
use tycho_common::{
    models::{
        blockchain::BlockTag,
        token::{Token, TokenOwnerStore},
        Chain,
    },
    storage::StorageError,
    traits::TokenOwnerFinding,
    Bytes,
};
use tycho_ethereum::services::token_pre_processor::EthereumTokenPreProcessor;
use tycho_storage::postgres::cache::CachedGateway;

use super::{
    protocol_cache::{ProtocolDataCache, ProtocolMemoryCache},
    token_analysis_cron::find_token_owners,
    ExtractionError,
};

/// The durable queue of token identities whose metadata is still pending, plus the owner
/// lookup that recovery needs. Every method is a single storage round trip.
#[async_trait]
pub trait PendingTokenStore: Send + Sync {
    /// Number of pending rows for the chain and the insertion time of the oldest one.
    async fn pending_token_metadata_stats(
        &self,
        chain: Chain,
    ) -> Result<(i64, Option<NaiveDateTime>), StorageError>;

    /// Up to `limit` pending rows with an id greater than `after_id`, in ascending id order.
    async fn pending_token_metadata(
        &self,
        chain: Chain,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, Token, NaiveDateTime)>, StorageError>;

    /// The subset of `addresses` whose metadata is already complete in storage.
    async fn ready_token_metadata(
        &self,
        chain: Chain,
        addresses: &[Bytes],
    ) -> Result<Vec<Token>, StorageError>;

    /// Persists recovered metadata and returns the tokens whose row this call completed.
    /// Tokens missing from the result were completed by another writer first.
    async fn complete_token_metadata(&self, tokens: &[Token]) -> Result<Vec<Token>, StorageError>;

    /// Funded balance owners for `addresses`, used to probe transfer behaviour.
    async fn token_owners(
        &self,
        chain: Chain,
        addresses: &[Bytes],
    ) -> anyhow::Result<TokenOwnerStore>;
}

#[async_trait]
impl PendingTokenStore for CachedGateway {
    async fn pending_token_metadata_stats(
        &self,
        chain: Chain,
    ) -> Result<(i64, Option<NaiveDateTime>), StorageError> {
        CachedGateway::pending_token_metadata_stats(self, chain).await
    }

    async fn pending_token_metadata(
        &self,
        chain: Chain,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, Token, NaiveDateTime)>, StorageError> {
        CachedGateway::pending_token_metadata(self, chain, after_id, limit).await
    }

    async fn ready_token_metadata(
        &self,
        chain: Chain,
        addresses: &[Bytes],
    ) -> Result<Vec<Token>, StorageError> {
        CachedGateway::ready_token_metadata(self, chain, addresses).await
    }

    async fn complete_token_metadata(&self, tokens: &[Token]) -> Result<Vec<Token>, StorageError> {
        CachedGateway::complete_token_metadata(self, tokens).await
    }

    async fn token_owners(
        &self,
        chain: Chain,
        addresses: &[Bytes],
    ) -> anyhow::Result<TokenOwnerStore> {
        find_token_owners(chain, addresses, self).await
    }
}

/// One recovery attempt for a single token. The caller owns the timeout for the attempt.
#[async_trait]
pub trait TokenRecovery: Send + Sync {
    /// Fetches the token's metadata at `block`. `Err` means the token stays pending.
    async fn recover_token(
        &self,
        address: Bytes,
        finder: Arc<dyn TokenOwnerFinding>,
        block: BlockTag,
    ) -> Result<Token, String>;
}

#[async_trait]
impl TokenRecovery for EthereumTokenPreProcessor {
    async fn recover_token(
        &self,
        address: Bytes,
        finder: Arc<dyn TokenOwnerFinding>,
        block: BlockTag,
    ) -> Result<Token, String> {
        EthereumTokenPreProcessor::recover_token(self, address, finder, block).await
    }
}

const PAGE_SIZE: i64 = 32;
const CONCURRENCY: usize = 4;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// With nothing pending the sweep is a single aggregate query; there is no reason to run it at
/// the page cadence.
const IDLE_INTERVAL: Duration = Duration::from_secs(60);
/// Longest wait between attempts for one token. Bounds the RPC cost of a token that keeps
/// failing without giving up on it: a stalled provider or an unfunded pool can recover later.
const MAX_BACKOFF: Duration = Duration::from_secs(60 * 60);

/// Per-token exponential backoff, kept in memory only. A restart resets it, which is
/// acceptable: the durable queue is the source of truth and one extra attempt is cheap.
#[derive(Default)]
struct RetryBackoff {
    attempts: HashMap<Bytes, (u32, Instant)>,
    /// Pending addresses read since the current sweep started.
    seen: HashSet<Bytes>,
}

impl RetryBackoff {
    fn is_due(&self, address: &Bytes, now: Instant) -> bool {
        self.attempts
            .get(address)
            .is_none_or(|(_, due)| *due <= now)
    }

    /// The wait starts at `failed_at`, the moment the failure was observed. An attempt can take
    /// up to `ATTEMPT_TIMEOUT`, and so can every queued attempt ahead of it on the page, so
    /// counting from the page start would leave the short waits already expired.
    fn record_failure(&mut self, address: Bytes, failed_at: Instant) {
        let attempts = self
            .attempts
            .get(&address)
            .map_or(0, |(attempts, _)| *attempts);
        let delay = RETRY_INTERVAL
            .saturating_mul(1u32 << attempts.min(16))
            .min(MAX_BACKOFF);
        self.attempts
            .insert(address, (attempts.saturating_add(1), failed_at + delay));
    }

    fn record_success(&mut self, address: &Bytes) {
        self.attempts.remove(address);
    }

    /// Notes that `address` is still in the durable queue during the current sweep.
    fn observe(&mut self, address: &Bytes) {
        self.seen.insert(address.clone());
    }

    /// Ends a sweep that read the queue to its end, and drops bookkeeping for tokens it did not
    /// see: another writer completed them. Only a finished sweep may prune. One cut short, for
    /// example by a failed page read, has not seen every pending token and would erase the
    /// backoff of the ones it missed.
    fn finish_sweep(&mut self) {
        let seen = std::mem::take(&mut self.seen);
        self.attempts
            .retain(|address, _| seen.contains(address));
    }
}

/// Runs until cancelled by its owner. Errors leave rows pending for a later sweep.
///
/// Recovery applies the same rules as the hot path: metadata that reverts gets the legacy
/// fallbacks, a token without a funded owner gets a Bad verdict, and only an RPC that does not
/// answer keeps the token pending. Otherwise a token that was merely deferred by a slow RPC
/// would end up in a different state than the same token fetched on a fast one.
pub async fn run<S: PendingTokenStore, P: TokenRecovery>(
    chain: Chain,
    store: S,
    cache: ProtocolMemoryCache,
    processor: P,
) -> Result<(), ExtractionError> {
    let mut after_id = 0;
    let mut backoff = RetryBackoff::default();
    loop {
        if after_id == 0 {
            // A successful repair in another process (or an ambiguous commit response) can
            // remove a row from the durable queue before this cache has observed it, so this
            // runs before the queue is consulted: an empty queue is exactly the case where the
            // cache may still hold entries nobody else will repair. Addresses without a
            // database row (reverted blocks) simply never come back ready.
            for addresses in cache
                .pending_token_addresses()
                .await
                .chunks(PAGE_SIZE as usize)
            {
                match timeout(ATTEMPT_TIMEOUT, store.ready_token_metadata(chain, addresses)).await {
                    Ok(Ok(ready)) => cache.add_tokens(ready).await?,
                    result => warn!(?result, "Could not reconcile pending token cache"),
                }
            }
            match timeout(ATTEMPT_TIMEOUT, store.pending_token_metadata_stats(chain)).await {
                Ok(Ok((count, oldest))) => {
                    gauge!("token_metadata_pending_count", "chain" => chain.to_string())
                        .set(count as f64);
                    let age = oldest
                        .map(|ts| {
                            (chrono::Utc::now().naive_utc() - ts)
                                .num_seconds()
                                .max(0)
                        })
                        .unwrap_or(0);
                    gauge!("token_metadata_pending_age_seconds", "chain" => chain.to_string())
                        .set(age as f64);
                    if count == 0 {
                        // An empty queue is a finished sweep: no token needs a backoff.
                        backoff.finish_sweep();
                        sleep(IDLE_INTERVAL).await;
                        continue;
                    }
                }
                result => warn!(?result, "Could not measure pending token metadata"),
            }
        }
        // The cursor advances even on failed attempts. A poison token must not keep later
        // pages from being visited, and a shrinking pending set must not shift an OFFSET.
        let page = match timeout(
            ATTEMPT_TIMEOUT,
            store.pending_token_metadata(chain, after_id, PAGE_SIZE),
        )
        .await
        {
            Ok(Ok(page)) => page,
            result => {
                warn!(?result, "Could not read pending token metadata");
                sleep(RETRY_INTERVAL).await;
                continue;
            }
        };
        if page.is_empty() {
            after_id = 0;
            backoff.finish_sweep();
            sleep(RETRY_INTERVAL).await;
            continue;
        }
        after_id = page.last().expect("nonempty page").0;
        let now = Instant::now();
        let mut addresses = Vec::new();
        for (_, token, _) in &page {
            backoff.observe(&token.address);
            if backoff.is_due(&token.address, now) {
                addresses.push(token.address.clone());
            }
        }
        if addresses.is_empty() {
            continue;
        }
        let owners = match timeout(ATTEMPT_TIMEOUT, store.token_owners(chain, &addresses)).await {
            Ok(Ok(owners)) => Arc::new(owners),
            result => {
                warn!(?result, "Could not resolve pending token owners");
                sleep(RETRY_INTERVAL).await;
                continue;
            }
        };
        let mut work = stream::iter(addresses)
            .map(|address| {
                let owners = owners.clone();
                let processor = &processor;
                async move {
                    let attempt =
                        processor.recover_token(address.clone(), owners, BlockTag::Latest);
                    (address, timeout(ATTEMPT_TIMEOUT, attempt).await)
                }
            })
            .buffer_unordered(CONCURRENCY);
        let mut ready = Vec::new();
        while let Some((address, result)) = work.next().await {
            match result {
                Ok(Ok(token)) => ready.push(token),
                result => {
                    backoff.record_failure(address, Instant::now());
                    counter!("token_metadata_recovery_attempts", "chain" => chain.to_string(), "outcome" => "deferred").increment(1);
                    warn!(?result, "Token metadata recovery deferred");
                }
            }
        }
        // Do not time out the commit future: cancelling after PostgreSQL commits would make
        // its acknowledgement ambiguous. A DB failure leaves the cache untouched.
        if !ready.is_empty() {
            match store
                .complete_token_metadata(&ready)
                .await
            {
                Ok(completed) => {
                    let already_complete = ready.len() - completed.len();
                    counter!("token_metadata_recovery_attempts", "chain" => chain.to_string(), "outcome" => "completed").increment(completed.len() as u64);
                    counter!("token_metadata_recovery_attempts", "chain" => chain.to_string(), "outcome" => "already_complete").increment(already_complete as u64);
                    for token in &ready {
                        backoff.record_success(&token.address);
                    }
                    cache.add_tokens(completed).await?;
                }
                Err(error) => {
                    counter!("token_metadata_recovery_attempts", "chain" => chain.to_string(), "outcome" => "commit_failed").increment(ready.len() as u64);
                    warn!(%error, "Could not persist recovered token metadata");
                }
            }
        }
        sleep(RETRY_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(id: u8) -> Bytes {
        Bytes::from(vec![id])
    }

    #[test]
    fn unknown_tokens_are_due_immediately() {
        let backoff = RetryBackoff::default();
        assert!(backoff.is_due(&address(1), Instant::now()));
    }

    #[test]
    fn failures_double_the_wait_up_to_the_cap() {
        let mut backoff = RetryBackoff::default();
        let now = Instant::now();
        let token = address(1);
        let mut expected = RETRY_INTERVAL;
        for _ in 0..20 {
            backoff.record_failure(token.clone(), now);
            let (_, due) = backoff.attempts[&token];
            assert_eq!(due - now, expected);
            assert!(!backoff.is_due(&token, now + expected - Duration::from_millis(1)));
            assert!(backoff.is_due(&token, now + expected));
            expected = (expected * 2).min(MAX_BACKOFF);
        }
        assert_eq!(backoff.attempts[&token].1 - now, MAX_BACKOFF);
    }

    #[test]
    fn success_resets_and_stale_entries_are_pruned() {
        let mut backoff = RetryBackoff::default();
        let now = Instant::now();
        backoff.record_failure(address(1), now);
        backoff.record_failure(address(2), now);
        backoff.record_success(&address(1));
        assert!(backoff.is_due(&address(1), now));
        assert!(!backoff.is_due(&address(2), now));

        backoff.finish_sweep();
        assert!(backoff.is_due(&address(2), now));
    }
}

#[cfg(test)]
mod loop_tests {
    use std::sync::Mutex;

    use tokio::{task::yield_now, time::advance};
    use tycho_common::models::token::TokenMetadataStatus;

    use super::*;
    use crate::testing::MockGateway;

    const CHAIN: Chain = Chain::Ethereum;

    #[derive(Debug, Clone, PartialEq)]
    enum Call {
        Stats,
        Page { after_id: i64 },
        Ready(Vec<Bytes>),
        Owners(Vec<Bytes>),
        Complete(Vec<Bytes>),
    }

    enum Commit {
        Persist,
        Fail,
        /// Rows for these addresses were completed by another writer before the commit.
        AlreadyComplete(HashSet<Bytes>),
    }

    struct StoreState {
        /// Pending rows in ascending id order.
        queue: Vec<(i64, Token)>,
        /// Rows repaired by another writer, served by `ready_token_metadata`.
        repaired: HashMap<Bytes, Token>,
        commit: Commit,
        owners_available: bool,
        /// The next this many `pending_token_metadata` calls fail.
        failing_page_reads: usize,
        calls: Vec<Call>,
    }

    #[derive(Clone)]
    struct MockStore(Arc<Mutex<StoreState>>);

    impl MockStore {
        fn with_queue(ids: impl IntoIterator<Item = u8>) -> Self {
            let queue = ids
                .into_iter()
                .map(|id| (i64::from(id), Token::pending(&address(id), CHAIN)))
                .collect();
            Self(Arc::new(Mutex::new(StoreState {
                queue,
                repaired: HashMap::new(),
                commit: Commit::Persist,
                owners_available: true,
                failing_page_reads: 0,
                calls: Vec::new(),
            })))
        }

        fn state(&self) -> std::sync::MutexGuard<'_, StoreState> {
            self.0.lock().expect("store mutex")
        }

        fn calls(&self) -> Vec<Call> {
            self.state().calls.clone()
        }

        fn page_calls(&self) -> Vec<i64> {
            self.calls()
                .into_iter()
                .filter_map(|call| match call {
                    Call::Page { after_id } => Some(after_id),
                    Call::Stats | Call::Ready(_) | Call::Owners(_) | Call::Complete(_) => None,
                })
                .collect()
        }

        fn completed(&self) -> Vec<Vec<Bytes>> {
            self.calls()
                .into_iter()
                .filter_map(|call| match call {
                    Call::Complete(addresses) => Some(addresses),
                    Call::Stats | Call::Ready(_) | Call::Owners(_) | Call::Page { .. } => None,
                })
                .collect()
        }

        fn queued_ids(&self) -> Vec<i64> {
            self.state()
                .queue
                .iter()
                .map(|(id, _)| *id)
                .collect()
        }
    }

    #[async_trait]
    impl PendingTokenStore for MockStore {
        async fn pending_token_metadata_stats(
            &self,
            _chain: Chain,
        ) -> Result<(i64, Option<NaiveDateTime>), StorageError> {
            let mut state = self.state();
            state.calls.push(Call::Stats);
            let count = state.queue.len() as i64;
            let oldest = (count > 0).then(|| chrono::Utc::now().naive_utc());
            Ok((count, oldest))
        }

        async fn pending_token_metadata(
            &self,
            _chain: Chain,
            after_id: i64,
            limit: i64,
        ) -> Result<Vec<(i64, Token, NaiveDateTime)>, StorageError> {
            let mut state = self.state();
            state
                .calls
                .push(Call::Page { after_id });
            if state.failing_page_reads > 0 {
                state.failing_page_reads -= 1;
                return Err(StorageError::Unexpected("connection reset".into()));
            }
            let inserted = chrono::Utc::now().naive_utc();
            Ok(state
                .queue
                .iter()
                .filter(|(id, _)| *id > after_id)
                .take(limit as usize)
                .map(|(id, token)| (*id, token.clone(), inserted))
                .collect())
        }

        async fn ready_token_metadata(
            &self,
            _chain: Chain,
            addresses: &[Bytes],
        ) -> Result<Vec<Token>, StorageError> {
            let mut state = self.state();
            state
                .calls
                .push(Call::Ready(addresses.to_vec()));
            Ok(addresses
                .iter()
                .filter_map(|address| state.repaired.get(address).cloned())
                .collect())
        }

        async fn complete_token_metadata(
            &self,
            tokens: &[Token],
        ) -> Result<Vec<Token>, StorageError> {
            let mut state = self.state();
            let addresses: Vec<_> = tokens
                .iter()
                .map(|token| token.address.clone())
                .collect();
            state
                .calls
                .push(Call::Complete(addresses.clone()));
            let skipped = match &state.commit {
                Commit::Fail => {
                    return Err(StorageError::Unexpected("connection reset".into()));
                }
                Commit::Persist => HashSet::new(),
                Commit::AlreadyComplete(skipped) => skipped.clone(),
            };
            state
                .queue
                .retain(|(_, token)| !addresses.contains(&token.address));
            Ok(tokens
                .iter()
                .filter(|token| !skipped.contains(&token.address))
                .cloned()
                .collect())
        }

        async fn token_owners(
            &self,
            _chain: Chain,
            addresses: &[Bytes],
        ) -> anyhow::Result<TokenOwnerStore> {
            let mut state = self.state();
            state
                .calls
                .push(Call::Owners(addresses.to_vec()));
            if !state.owners_available {
                anyhow::bail!("owner lookup unavailable");
            }
            Ok(TokenOwnerStore::new(HashMap::new()))
        }
    }

    enum Outcome {
        Ready,
        Fail,
        Hang,
    }

    struct MockRecovery {
        outcomes: HashMap<Bytes, Outcome>,
        calls: Arc<Mutex<Vec<Bytes>>>,
    }

    impl MockRecovery {
        fn new(
            outcomes: impl IntoIterator<Item = (u8, Outcome)>,
        ) -> (Self, Arc<Mutex<Vec<Bytes>>>) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let outcomes = outcomes
                .into_iter()
                .map(|(id, outcome)| (address(id), outcome))
                .collect();
            (Self { outcomes, calls: calls.clone() }, calls)
        }

        fn always_ready() -> (Self, Arc<Mutex<Vec<Bytes>>>) {
            Self::new([])
        }
    }

    #[async_trait]
    impl TokenRecovery for MockRecovery {
        async fn recover_token(
            &self,
            address: Bytes,
            _finder: Arc<dyn TokenOwnerFinding>,
            _block: BlockTag,
        ) -> Result<Token, String> {
            self.calls
                .lock()
                .expect("recovery mutex")
                .push(address.clone());
            match self
                .outcomes
                .get(&address)
                .unwrap_or(&Outcome::Ready)
            {
                Outcome::Ready => Ok(ready_token(&address)),
                Outcome::Fail => Err("rpc unavailable".into()),
                Outcome::Hang => std::future::pending().await,
            }
        }
    }

    fn address(id: u8) -> Bytes {
        Bytes::from(vec![id])
    }

    fn ready_token(address: &Bytes) -> Token {
        Token::new(address, "TKN", 18, 0, &[Some(21_000)], CHAIN, 100)
    }

    fn cache() -> ProtocolMemoryCache {
        ProtocolMemoryCache::new(CHAIN, chrono::Duration::seconds(60), Arc::new(MockGateway::new()))
    }

    async fn cache_with_pending(ids: impl IntoIterator<Item = u8>) -> ProtocolMemoryCache {
        let cache = cache();
        let pending: Vec<_> = ids
            .into_iter()
            .map(|id| Token::pending(&address(id), CHAIN))
            .collect();
        cache
            .add_tokens(pending)
            .await
            .expect("in-memory add");
        cache
    }

    fn spawn(
        store: &MockStore,
        cache: ProtocolMemoryCache,
        recovery: MockRecovery,
    ) -> tokio::task::JoinHandle<Result<(), ExtractionError>> {
        tokio::spawn(run(CHAIN, store.clone(), cache, recovery))
    }

    /// Lets the worker run every step that does not wait on the clock. The mocks never park,
    /// so a bounded number of yields drains all of them.
    async fn settle() {
        for _ in 0..100 {
            yield_now().await;
        }
    }

    async fn tick(duration: Duration) {
        advance(duration).await;
        settle().await;
    }

    fn recover_calls(calls: &Mutex<Vec<Bytes>>) -> Vec<Bytes> {
        calls
            .lock()
            .expect("recovery mutex")
            .clone()
    }

    // A hanging token on page 1 must neither starve page 2 within the sweep nor pin the cursor
    // once the queue is exhausted.
    #[tokio::test(start_paused = true)]
    async fn cursor_advances_past_a_hanging_page_and_resets_after_the_empty_page() {
        let store = MockStore::with_queue(1..=33);
        let (recovery, _) = MockRecovery::new([(1, Outcome::Hang)]);
        let worker = spawn(&store, cache(), recovery);

        settle().await;
        tick(ATTEMPT_TIMEOUT).await;
        tick(RETRY_INTERVAL).await;
        tick(RETRY_INTERVAL).await;
        tick(RETRY_INTERVAL).await;

        let calls = store.calls();
        let page_two_completed = calls
            .iter()
            .position(|call| matches!(call, Call::Complete(done) if done == &[address(33)]))
            .expect("page 2 token completed");
        let sweep_restart = calls
            .iter()
            .rposition(|call| *call == Call::Page { after_id: 0 })
            .expect("cursor reset");
        assert!(page_two_completed < sweep_restart, "page 2 completed in the first sweep");
        assert_eq!(store.page_calls(), vec![0, 32, 33, 0]);
        assert_eq!(store.queued_ids(), vec![1]);
        worker.abort();
    }

    // The timeout, not the poison token, decides when a page finishes; free slots keep
    // serving the rest of the page in the meantime.
    #[tokio::test(start_paused = true)]
    async fn hanging_attempt_is_cut_at_the_timeout_without_blocking_other_slots() {
        let store = MockStore::with_queue(1..=6);
        let (recovery, recover) = MockRecovery::new([(1, Outcome::Hang)]);
        let worker = spawn(&store, cache(), recovery);

        settle().await;
        assert_eq!(recover_calls(&recover).len(), 6, "every slot kept working around the hang");
        assert!(store.completed().is_empty());

        tick(ATTEMPT_TIMEOUT - Duration::from_millis(1)).await;
        assert!(store.completed().is_empty(), "page still waits on the hanging attempt");

        tick(Duration::from_millis(1)).await;
        let mut completed = store.completed();
        let mut page = completed
            .pop()
            .expect("page committed at the timeout");
        page.sort();
        assert_eq!(page, (2..=6).map(address).collect::<Vec<_>>());
        assert_eq!(store.queued_ids(), vec![1]);
        worker.abort();
    }

    // A failed commit must not let the cache claim metadata that storage never accepted; the
    // row stays queued and is retried once the cursor wraps around.
    #[tokio::test(start_paused = true)]
    async fn commit_failure_leaves_the_cache_pending_until_the_retry_succeeds() {
        let store = MockStore::with_queue([1]);
        store.state().commit = Commit::Fail;
        let cache = cache_with_pending([1]).await;
        let (recovery, recover) = MockRecovery::always_ready();
        let worker = spawn(&store, cache.clone(), recovery);

        settle().await;
        assert_eq!(store.completed(), vec![vec![address(1)]]);
        assert_eq!(cache.pending_token_addresses().await, vec![address(1)]);
        assert_eq!(store.queued_ids(), vec![1]);

        store.state().commit = Commit::Persist;
        tick(RETRY_INTERVAL).await;
        assert_eq!(recover_calls(&recover).len(), 1, "no retry before the cursor wraps");
        assert_eq!(cache.pending_token_addresses().await, vec![address(1)]);

        tick(RETRY_INTERVAL).await;
        assert_eq!(recover_calls(&recover).len(), 2);
        assert_eq!(store.completed().len(), 2);
        assert!(cache
            .pending_token_addresses()
            .await
            .is_empty());
        assert!(store.queued_ids().is_empty());
        worker.abort();
    }

    // Only rows this commit actually completed may update the cache; the other writer's
    // version is picked up by reconciliation instead.
    #[tokio::test(start_paused = true)]
    async fn partial_commit_updates_only_the_completed_subset() {
        let store = MockStore::with_queue([1, 2]);
        store.state().commit = Commit::AlreadyComplete(HashSet::from([address(2)]));
        let cache = cache_with_pending([1, 2]).await;
        let (recovery, _) = MockRecovery::always_ready();
        let worker = spawn(&store, cache.clone(), recovery);

        settle().await;
        assert_eq!(store.completed().len(), 1);
        assert_eq!(cache.pending_token_addresses().await, vec![address(2)]);
        let ready = cache
            .get_tokens(&[address(1)])
            .await
            .expect("cached")
            .remove(0)
            .expect("token 1 cached");
        assert_eq!(ready.metadata_status, TokenMetadataStatus::Ready);
        worker.abort();
    }

    // Rows repaired elsewhere must reach the cache before the sweep spends RPC budget, in
    // pages the store can serve.
    #[tokio::test(start_paused = true)]
    async fn reconciliation_runs_in_pages_before_the_first_queue_page() {
        let store = MockStore::with_queue([41]);
        let stale: Vec<u8> = (1..=40).collect();
        for id in &stale {
            let token = ready_token(&address(*id));
            store
                .state()
                .repaired
                .insert(token.address.clone(), token);
        }
        let cache = cache_with_pending(stale.iter().copied()).await;
        let (recovery, _) = MockRecovery::always_ready();
        let worker = spawn(&store, cache.clone(), recovery);

        settle().await;
        let calls = store.calls();
        let first_page = calls
            .iter()
            .position(|call| matches!(call, Call::Page { .. }))
            .expect("queue page read");
        let mut reconciled = HashSet::new();
        let mut ready_calls = 0;
        for call in &calls[..first_page] {
            let Call::Ready(addresses) = call else { continue };
            ready_calls += 1;
            assert!(addresses.len() <= PAGE_SIZE as usize, "chunked by PAGE_SIZE");
            reconciled.extend(addresses.iter().cloned());
        }
        assert_eq!(ready_calls, 2);
        assert_eq!(
            reconciled,
            stale
                .iter()
                .map(|id| address(*id))
                .collect()
        );
        assert!(cache
            .pending_token_addresses()
            .await
            .is_empty());
        worker.abort();
    }

    // The empty-queue check is one aggregate query; only after IDLE_INTERVAL does the worker
    // look at the queue again.
    #[tokio::test(start_paused = true)]
    async fn idle_worker_polls_the_queue_only_after_the_idle_interval() {
        let store = MockStore::with_queue([]);
        let (recovery, _) = MockRecovery::always_ready();
        let worker = spawn(&store, cache(), recovery);

        settle().await;
        assert_eq!(store.calls(), vec![Call::Stats]);

        store
            .state()
            .queue
            .push((1, Token::pending(&address(1), CHAIN)));
        tick(IDLE_INTERVAL - Duration::from_millis(1)).await;
        assert_eq!(store.calls(), vec![Call::Stats], "still idle");

        tick(Duration::from_millis(1)).await;
        assert_eq!(store.page_calls(), vec![0]);
        assert_eq!(store.completed(), vec![vec![address(1)]]);
        worker.abort();
    }

    // Exponential backoff must keep a failing token off the RPC until its due time even though
    // every sweep re-reads its row.
    #[tokio::test(start_paused = true)]
    async fn backoff_skips_a_failed_token_until_it_is_due() {
        let store = MockStore::with_queue([1]);
        let (recovery, recover) = MockRecovery::new([(1, Outcome::Fail)]);
        let worker = spawn(&store, cache(), recovery);

        // Sweeps run every 2 * RETRY_INTERVAL, so the first two failures (5s, 10s) are due by
        // the next sweep; the third (20s) is not.
        let expected_attempts = [1, 1, 2, 2, 3, 3, 3, 3, 4];
        settle().await;
        for (step, expected) in expected_attempts
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                recover_calls(&recover).len(),
                expected,
                "attempts at t = {}s",
                step as u64 * RETRY_INTERVAL.as_secs()
            );
            tick(RETRY_INTERVAL).await;
        }
        worker.abort();
    }

    // Regression: a failed read of the first page used to restart the sweep with an empty seen
    // set and erase every token's backoff, so a database blip sent the whole backlog back to the
    // RPC at once.
    #[tokio::test(start_paused = true)]
    async fn failed_first_page_read_keeps_the_backoff() {
        let store = MockStore::with_queue([1]);
        let (recovery, recover) = MockRecovery::new([(1, Outcome::Fail)]);
        let worker = spawn(&store, cache(), recovery);

        // Failures at 0 s, 10 s and 20 s leave the third wait running until 40 s; the empty page
        // at 25 s finishes that sweep.
        settle().await;
        for _ in 0..5 {
            tick(RETRY_INTERVAL).await;
        }
        assert_eq!(recover_calls(&recover).len(), 3);

        // The sweep starting at 30 s fails its first page read and restarts at 35 s.
        store.state().failing_page_reads = 1;
        tick(RETRY_INTERVAL).await;
        tick(RETRY_INTERVAL).await;
        assert_eq!(recover_calls(&recover).len(), 3, "not due before 40 s");

        tick(RETRY_INTERVAL).await;
        assert_eq!(recover_calls(&recover).len(), 4);
        worker.abort();
    }

    // Regression: the wait used to count from the page start, so an attempt that hung until
    // ATTEMPT_TIMEOUT came back with its short waits already expired.
    #[tokio::test(start_paused = true)]
    async fn backoff_counts_from_when_an_attempt_failed() {
        let store = MockStore::with_queue([1]);
        let (recovery, recover) = MockRecovery::new([(1, Outcome::Hang)]);
        let worker = spawn(&store, cache(), recovery);

        // Attempts start at 0 s, 20 s and 40 s and each fails 10 s later. The third failure, at
        // 50 s, waits 20 s, so the sweeps at 60 s and 65 s skip the token and 70 s retries it.
        // Counting from the page start would have retried it at 60 s.
        let expected_attempts = [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4];
        settle().await;
        for (step, expected) in expected_attempts
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                recover_calls(&recover).len(),
                expected,
                "attempts at t = {}s",
                step as u64 * RETRY_INTERVAL.as_secs()
            );
            tick(RETRY_INTERVAL).await;
        }
        worker.abort();
    }

    // Without owners there is nothing to probe against, so the page is skipped as a whole and
    // revisited once the cursor wraps.
    #[tokio::test(start_paused = true)]
    async fn owner_lookup_failure_skips_recovery_and_retries_later() {
        let store = MockStore::with_queue([1]);
        store.state().owners_available = false;
        let (recovery, recover) = MockRecovery::always_ready();
        let worker = spawn(&store, cache(), recovery);

        settle().await;
        assert_eq!(
            store.calls(),
            vec![Call::Stats, Call::Page { after_id: 0 }, Call::Owners(vec![address(1)])]
        );
        assert!(recover_calls(&recover).is_empty());

        store.state().owners_available = true;
        tick(RETRY_INTERVAL).await;
        assert_eq!(store.page_calls(), vec![0, 1], "cursor moved on after the failed page");
        assert!(recover_calls(&recover).is_empty());

        tick(RETRY_INTERVAL).await;
        assert_eq!(recover_calls(&recover), vec![address(1)]);
        assert_eq!(store.completed(), vec![vec![address(1)]]);
        worker.abort();
    }

    // The durable queue alone must be enough to resume: a restarted worker carries no memory of
    // earlier attempts.
    #[tokio::test(start_paused = true)]
    async fn restarted_worker_resumes_from_the_durable_queue() {
        let store = MockStore::with_queue([1]);
        let (recovery, first_recover) = MockRecovery::new([(1, Outcome::Fail)]);
        let first = spawn(&store, cache(), recovery);
        settle().await;
        assert_eq!(recover_calls(&first_recover).len(), 1);
        first.abort();
        assert!(first
            .await
            .expect_err("aborted")
            .is_cancelled());
        assert_eq!(store.queued_ids(), vec![1]);

        let (recovery, second_recover) = MockRecovery::always_ready();
        let second = spawn(&store, cache(), recovery);
        settle().await;
        assert_eq!(recover_calls(&second_recover), vec![address(1)]);
        assert!(store.queued_ids().is_empty());
        second.abort();
    }

    // Shutdown aborts the worker; `service_task_result` in main.rs maps that cancelled join to
    // a clean exit (its own tests cover the mapping).
    #[tokio::test(start_paused = true)]
    async fn abort_resolves_as_a_cancelled_join() {
        let store = MockStore::with_queue([1]);
        let (recovery, _) = MockRecovery::new([(1, Outcome::Hang)]);
        let worker = spawn(&store, cache(), recovery);
        settle().await;

        worker.abort();
        let error = worker.await.expect_err("aborted");
        assert!(error.is_cancelled());
    }

    // Regression: reconciliation once ran only after a non-zero pending count, so a cache whose
    // pending rows were all repaired by another writer (or lost to an ambiguous commit) never
    // learned they were ready.
    #[tokio::test(start_paused = true)]
    async fn reconciliation_runs_even_when_the_queue_is_empty() {
        let store = MockStore::with_queue([]);
        let repaired = ready_token(&address(1));
        store
            .state()
            .repaired
            .insert(repaired.address.clone(), repaired);
        let cache = cache_with_pending([1]).await;
        let (recovery, _) = MockRecovery::always_ready();
        let worker = spawn(&store, cache.clone(), recovery);

        settle().await;
        tick(IDLE_INTERVAL).await;
        assert!(
            store
                .calls()
                .contains(&Call::Ready(vec![address(1)])),
            "pending cache entries are reconciled while the queue is empty"
        );
        assert!(cache
            .pending_token_addresses()
            .await
            .is_empty());
        worker.abort();
    }
}
