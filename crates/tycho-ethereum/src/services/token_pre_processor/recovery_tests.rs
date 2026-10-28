use std::{
    collections::HashMap,
    future::pending,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use alloy::sol_types::SolValue;
use mockito::Server;
use rstest::rstest;
use serde_json::{json, Value};
use tycho_common::models::token::{TokenMetadataStatus, TokenOwnerStore};

use super::*;
use crate::rpc::config::RPCRetryConfig;

fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn rpc_error_mock(code: i64, message: &'static str) -> impl Fn(&mockito::Request) -> Vec<u8> {
    move |request| {
        let calls: Vec<Value> = serde_json::from_slice(request.body().unwrap()).unwrap();
        serde_json::to_vec(
            &calls
                .iter()
                .map(|call| rpc_error(&call["id"], code, message))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }
}

/// Answers every item of a metadata batch by the ERC-20 function it calls, inside the single
/// array response the batch expects.
fn per_field_mock(
    symbol: impl Fn(&Value) -> Value + Send + Sync + 'static,
    decimals: impl Fn(&Value) -> Value + Send + Sync + 'static,
) -> impl Fn(&mockito::Request) -> Vec<u8> {
    let symbol_input = Bytes::from(symbolCall {}.abi_encode()).to_string();
    move |request| {
        let calls: Vec<Value> = serde_json::from_slice(request.body().unwrap()).unwrap();
        serde_json::to_vec(
            &calls
                .iter()
                .map(|call| {
                    if call["params"][0]["input"] == symbol_input {
                        symbol(call)
                    } else {
                        decimals(call)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }
}

/// Finds no owner for any token, promptly for most and never for the `hanging` ones, which
/// hold their concurrency slot until the caller drops the analysis.
#[derive(Debug, Default)]
struct HangingOwnerFinder {
    hanging: HashSet<Bytes>,
    /// Analyses started for a hanging token and not yet dropped.
    active: Arc<AtomicUsize>,
    /// Every `find_owner` call, hanging or not: only tokens that got a slot make one.
    calls: AtomicUsize,
}

struct ActiveAttempt(Arc<AtomicUsize>);

impl Drop for ActiveAttempt {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenOwnerFinding for HangingOwnerFinder {
    async fn find_owner(&self, token: Bytes, _: Bytes) -> Result<Option<(Bytes, Bytes)>, String> {
        self.calls
            .fetch_add(1, Ordering::SeqCst);
        if !self.hanging.contains(&token) {
            return Ok(None);
        }
        self.active
            .fetch_add(1, Ordering::SeqCst);
        let _attempt = ActiveAttempt(self.active.clone());
        pending().await
    }
}

#[derive(Debug, Default)]
struct CountingOwnerFinder {
    calls: AtomicUsize,
}

#[async_trait]
impl TokenOwnerFinding for CountingOwnerFinder {
    async fn find_owner(&self, _: Bytes, _: Bytes) -> Result<Option<(Bytes, Bytes)>, String> {
        self.calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

fn metadata_response(request: &Value) -> Value {
    let result =
        if request["params"][0]["input"] == Bytes::from(symbolCall {}.abi_encode()).to_string() {
            "TEST".abi_encode()
        } else {
            decimalsCall::abi_encode_returns(&6)
        };
    json!({"jsonrpc": "2.0", "id": request["id"], "result": Bytes::from(result)})
}

#[tokio::test]
async fn collection_deadline_preserves_completed_tokens_and_cancels_queued_work() {
    let mut server = Server::new_async().await;
    let _metadata = server
        .mock("POST", "/")
        .with_body_from_request(|request| {
            let calls: Vec<Value> = serde_json::from_slice(request.body().unwrap()).unwrap();
            serde_json::to_vec(
                &calls
                    .iter()
                    .map(metadata_response)
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        })
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO)
        .with_enrichment_budget(Duration::from_secs(1));
    let addresses: Vec<_> = (1..=12)
        .map(|id| Address::repeat_byte(id).to_bytes())
        .collect();
    let mut duplicated = addresses.clone();
    duplicated.push(addresses[0].clone());
    // One token completes; every other token holds its concurrency slot until cancelled.
    let finder = Arc::new(HangingOwnerFinder {
        hanging: addresses[1..].iter().cloned().collect(),
        ..Default::default()
    });
    // A per-token timeout would need three waves of 1 s each, exceeding this 2.5 s bound.
    let tokens = tokio::time::timeout(
        Duration::from_millis(2_500),
        processor.get_tokens(duplicated, finder.clone(), BlockTag::Latest),
    )
    .await
    .expect("the entire collection must share a deadline");
    assert_eq!(
        tokens
            .iter()
            .map(|t| t.address.clone())
            .collect::<Vec<_>>(),
        addresses
    );
    assert_eq!(tokens[0].metadata_status, TokenMetadataStatus::Ready);
    assert_eq!(tokens[0].decimals, 6);
    assert!(tokens[1..]
        .iter()
        .all(|t| t.metadata_status == TokenMetadataStatus::Pending));
    assert_eq!(finder.active.load(Ordering::SeqCst), 0, "unfinished analyses must be dropped");
}

#[tokio::test]
async fn stalled_metadata_leaves_every_token_pending_even_when_analysis_finishes() {
    // Accepting a connection without sending headers stalls every batched metadata call, while
    // analysis finishes at once. More tokens than concurrency slots: the ones still queued when
    // the deadline hits must come back pending too, in input order.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let rpc =
        EthereumRpcClient::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO)
        .with_enrichment_budget(Duration::from_millis(50));
    let addresses: Vec<_> = (1..=(MAX_CONCURRENT_TOKENS as u8 + 2))
        .map(|id| Address::repeat_byte(id).to_bytes())
        .collect();
    let finder = Arc::new(CountingOwnerFinder::default());
    let tokens = tokio::time::timeout(
        Duration::from_millis(500),
        processor.get_tokens(addresses.clone(), finder.clone(), BlockTag::Latest),
    )
    .await
    .unwrap();
    assert_eq!(
        tokens
            .iter()
            .map(|t| t.address.clone())
            .collect::<Vec<_>>(),
        addresses
    );
    assert!(tokens
        .iter()
        .all(|t| t.metadata_status == TokenMetadataStatus::Pending && t.quality == 0));
    assert_eq!(
        finder.calls.load(Ordering::SeqCst),
        MAX_CONCURRENT_TOKENS,
        "only tokens that got a concurrency slot run analysis"
    );
}

#[derive(Debug, Clone, Copy)]
enum NoAnswer {
    ProviderError,
    TransportFailure,
}

#[rstest]
#[case::provider_error(NoAnswer::ProviderError)]
#[case::transport_failure(NoAnswer::TransportFailure)]
#[tokio::test]
async fn strict_recovery_stays_pending_when_the_rpc_does_not_answer(#[case] failure: NoAnswer) {
    // A provider error that is neither transient nor a revert is an answer that carries no
    // data; a transport failure is no answer at all. Neither tells anything about the token, so
    // no fallback may be stored and the token must stay pending.
    let mut server = Server::new_async().await;
    let mock = server.mock("POST", "/").expect(1);
    let mock = match failure {
        NoAnswer::ProviderError => {
            mock.with_body_from_request(rpc_error_mock(-32601, "method not found"))
        }
        NoAnswer::TransportFailure => mock.with_status(503),
    }
    .create_async()
    .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let rpc = match failure {
        NoAnswer::ProviderError => rpc,
        NoAnswer::TransportFailure => rpc.with_retry(RPCRetryConfig::new(0, 1, 1)),
    };
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let result = processor
        .recover_token(
            Address::repeat_byte(1).to_bytes(),
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        )
        .await;
    assert!(result.is_err());
    mock.assert_async().await;
}

#[tokio::test]
async fn strict_recovery_stays_pending_when_analysis_rpc_fails() {
    // Metadata answers, but the analysis simulation gets a provider error: the verdict is
    // unknown, so strict mode must not store a Bad quality it cannot back up.
    let mut server = Server::new_async().await;
    let _rpc = server
        .mock("POST", "/")
        .with_body_from_request(|request| {
            let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            let response = match body.as_array() {
                Some(calls) => Value::Array(
                    calls
                        .iter()
                        .map(metadata_response)
                        .collect(),
                ),
                None => rpc_error(&body["id"], -32601, "method not found"),
            };
            response.to_string().into_bytes()
        })
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let address = Address::repeat_byte(1).to_bytes();
    let owners = TokenOwnerStore::new(
        [(address.clone(), (Address::repeat_byte(9).to_bytes(), Bytes::from("0x0186a0")))].into(),
    );
    let error = processor
        .recover_token(address, Arc::new(owners), BlockTag::Latest)
        .await
        .expect_err("an unanswered analysis must keep the token pending");
    assert!(error.contains("eth_call with state overrides failed"), "{error}");
}

#[tokio::test]
async fn strict_recovery_uses_legacy_fallbacks_when_metadata_reverts() {
    // The contract answered and reverted: retrying later cannot change that, so the token gets
    // the same address symbol and 18 decimals the hot path always stored, and analysis
    // (no funded owner here) still decides the quality.
    let mut server = Server::new_async().await;
    let _revert = server
        .mock("POST", "/")
        .with_body_from_request(rpc_error_mock(3, "execution reverted"))
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let address = Address::repeat_byte(1);
    let token = processor
        .recover_token(
            address.to_bytes(),
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        )
        .await
        .expect("a revert is a definitive answer");
    assert_eq!(token.metadata_status, TokenMetadataStatus::Ready);
    assert_eq!(token.symbol, format!("0x{address:x}"));
    assert_eq!(token.decimals, 18);
    assert_eq!(token.quality, 10);
}

#[tokio::test]
async fn strict_recovery_falls_back_only_for_the_reverting_field() {
    // One batch item reverts while the other answers: only the reverting field takes its
    // legacy fallback; the answered field keeps the on-chain value.
    let mut server = Server::new_async().await;
    let batch = server
        .mock("POST", "/")
        .with_body_from_request(per_field_mock(
            |call| rpc_error(&call["id"], 3, "execution reverted"),
            |call| {
                json!({
                    "jsonrpc": "2.0", "id": call["id"],
                    "result": Bytes::from(decimalsCall::abi_encode_returns(&6))
                })
            },
        ))
        .expect(1)
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let address = Address::repeat_byte(1);
    let token = processor
        .recover_token(
            address.to_bytes(),
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        )
        .await
        .expect("a reverting symbol must not defer a token whose decimals answered");
    assert_eq!(token.metadata_status, TokenMetadataStatus::Ready);
    assert_eq!(token.symbol, format!("0x{address:x}"));
    assert_eq!(token.decimals, 6);
    batch.assert_async().await;
}

#[tokio::test]
async fn strict_recovery_stays_pending_when_one_field_has_no_answer() {
    // One batch item answers while the other gets a provider error: a later retry could still
    // fetch the missing field, so strict mode must keep the token pending.
    let mut server = Server::new_async().await;
    let _batch = server
        .mock("POST", "/")
        .with_body_from_request(per_field_mock(
            |call| {
                json!({
                    "jsonrpc": "2.0", "id": call["id"],
                    "result": Bytes::from("TEST".abi_encode())
                })
            },
            |call| rpc_error(&call["id"], -32601, "method not found"),
        ))
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let result = processor
        .recover_token(
            Address::repeat_byte(1).to_bytes(),
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn strict_recovery_uses_legacy_fallbacks_when_metadata_is_undecodable() {
    // The contract answered with data that is not a valid `symbol`/`decimals` return: like a
    // revert, retrying cannot change that, so the hot-path fallbacks apply and analysis (no
    // funded owner here) decides the quality.
    let mut server = Server::new_async().await;
    let _undecodable = server
        .mock("POST", "/")
        .with_body_from_request(per_field_mock(
            |call| json!({"jsonrpc": "2.0", "id": call["id"], "result": "0x"}),
            |call| json!({"jsonrpc": "2.0", "id": call["id"], "result": "0x"}),
        ))
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO);
    let address = Address::repeat_byte(1);
    let token = processor
        .recover_token(
            address.to_bytes(),
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        )
        .await
        .expect("undecodable return data is a definitive answer");
    assert_eq!(token.metadata_status, TokenMetadataStatus::Ready);
    assert_eq!(token.symbol, format!("0x{address:x}"));
    assert_eq!(token.decimals, 18);
    assert_eq!(token.quality, 10);
}

#[tokio::test]
async fn deadline_covers_retry_backoff() {
    // A retryable transport error with a one-second backoff would exceed the budget several
    // times over. The deadline must cut through the backoff sleep, not wait for it.
    let mut server = Server::new_async().await;
    let unavailable = server
        .mock("POST", "/")
        .with_status(503)
        .expect(1)
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url())
        .unwrap()
        .with_retry(RPCRetryConfig::new(3, 1_000, 1_000));
    let processor = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO)
        .with_enrichment_budget(Duration::from_millis(200));
    let tokens = tokio::time::timeout(
        Duration::from_millis(700),
        processor.get_tokens(
            vec![Address::repeat_byte(1).to_bytes()],
            Arc::new(TokenOwnerStore::new(Default::default())),
            BlockTag::Latest,
        ),
    )
    .await
    .expect("budget must bound retry backoff");
    assert_eq!(tokens[0].metadata_status, TokenMetadataStatus::Pending);
    unavailable.assert_async().await;
}

/// SplitMix64: small enough to inline and deterministic per seed, so an assertion that names
/// its seed reproduces exactly. No external property-testing dependency needed.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`; modulo bias is irrelevant for picking test cases.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

/// What the RPC answers to a token's batched `symbol`/`decimals` calls.
#[derive(Debug, Clone, PartialEq)]
enum MetadataAnswer {
    Answers {
        symbol: String,
        decimals: u8,
    },
    /// Both calls revert: the legacy fallbacks apply.
    Reverts,
    /// Only `symbol` reverts; `decimals` answers.
    SymbolReverts {
        decimals: u8,
    },
    /// `symbol` answers; `decimals` gets a provider error that is not a revert.
    NoDecimals {
        symbol: String,
    },
}

#[derive(Debug, Clone)]
struct TokenScript {
    address: Address,
    metadata: MetadataAnswer,
    analysis_hangs: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RpcBehaviour {
    /// Every metadata call is answered per token script.
    Scripted,
    /// A bound socket nobody accepts: every metadata call stalls until the budget cuts it.
    NeverAnswers,
}

#[derive(Debug)]
struct Scenario {
    seed: u64,
    rpc: RpcBehaviour,
    scripts: Vec<TokenScript>,
    /// Scripts in random order, with random duplicates appended.
    input: Vec<Bytes>,
}

fn scenario(seed: u64, rpc: RpcBehaviour) -> Scenario {
    let mut rng = SplitMix64::new(seed);
    let count = 1 + rng.below(12) as u8;
    let scripts: Vec<_> = (1..=count)
        .map(|index| {
            // The seed is part of the address so concurrently running seeds never share a
            // token on the shared mock server.
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&seed.to_be_bytes());
            bytes[19] = index;
            let symbol = format!("T{index}");
            let decimals = rng.below(31) as u8;
            let metadata = match rng.below(4) {
                0 => MetadataAnswer::Answers { symbol, decimals },
                1 => MetadataAnswer::Reverts,
                2 => MetadataAnswer::SymbolReverts { decimals },
                _ => MetadataAnswer::NoDecimals { symbol },
            };
            TokenScript { address: Address::from(bytes), metadata, analysis_hangs: rng.one_in(4) }
        })
        .collect();
    let mut input: Vec<_> = scripts
        .iter()
        .map(|script| script.address.to_bytes())
        .collect();
    for i in (1..input.len()).rev() {
        input.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for _ in 0..rng.below(4) {
        let duplicate = input[rng.below(count as u64) as usize].clone();
        input.push(duplicate);
    }
    Scenario { seed, rpc, scripts, input }
}

type Scripts = Arc<Mutex<HashMap<Bytes, MetadataAnswer>>>;

fn scripted_answer(scripts: &Scripts, call: &Value) -> Option<MetadataAnswer> {
    let address = Bytes::from_str(call["params"][0]["to"].as_str()?).ok()?;
    scripts
        .lock()
        .unwrap()
        .get(&address)
        .cloned()
}

fn rpc_result(call: &Value, result: Vec<u8>) -> Value {
    json!({"jsonrpc": "2.0", "id": call["id"], "result": Bytes::from(result)})
}

/// Routes every batch item by the token it targets (`params[0].to`) to that token's script.
/// An unscripted token gets a provider error so the failure shows up as an unexpected
/// pending token rather than a hung request.
fn scripted_mock(scripts: Scripts) -> impl Fn(&mockito::Request) -> Vec<u8> {
    let symbol_scripts = scripts.clone();
    per_field_mock(
        move |call| match scripted_answer(&symbol_scripts, call) {
            Some(MetadataAnswer::Answers { symbol, .. }) |
            Some(MetadataAnswer::NoDecimals { symbol }) => {
                rpc_result(call, symbol.as_str().abi_encode())
            }
            Some(MetadataAnswer::Reverts) | Some(MetadataAnswer::SymbolReverts { .. }) => {
                rpc_error(&call["id"], 3, "execution reverted")
            }
            None => rpc_error(&call["id"], -32000, "unscripted token"),
        },
        move |call| match scripted_answer(&scripts, call) {
            Some(MetadataAnswer::Answers { decimals, .. }) |
            Some(MetadataAnswer::SymbolReverts { decimals }) => {
                rpc_result(call, decimalsCall::abi_encode_returns(&decimals))
            }
            Some(MetadataAnswer::Reverts) => rpc_error(&call["id"], 3, "execution reverted"),
            Some(MetadataAnswer::NoDecimals { .. }) => {
                rpc_error(&call["id"], -32601, "method not found")
            }
            None => rpc_error(&call["id"], -32000, "unscripted token"),
        },
    )
}

#[derive(Debug, PartialEq)]
enum Expected {
    Pending,
    Ready { symbol: String, decimals: u32 },
}

/// A token only runs once it gets one of the `MAX_CONCURRENT_TOKENS` slots, which hanging
/// tokens ahead of it in input order never release; a stalled RPC leaves everything pending.
fn expected_outcome(scenario: &Scenario, script: &TokenScript, has_slot: bool) -> Expected {
    if scenario.rpc == RpcBehaviour::NeverAnswers || !has_slot || script.analysis_hangs {
        return Expected::Pending;
    }
    let fallback = format!("0x{:x}", script.address);
    match &script.metadata {
        MetadataAnswer::Answers { symbol, decimals } => {
            Expected::Ready { symbol: symbol.clone(), decimals: u32::from(*decimals) }
        }
        MetadataAnswer::Reverts => Expected::Ready { symbol: fallback, decimals: 18 },
        MetadataAnswer::SymbolReverts { decimals } => {
            Expected::Ready { symbol: fallback, decimals: u32::from(*decimals) }
        }
        MetadataAnswer::NoDecimals { .. } => Expected::Pending,
    }
}

async fn get_tokens_for(
    scenario: &Scenario,
    processor: &EthereumTokenPreProcessor,
    outer_timeout: Duration,
) -> (Vec<Token>, Arc<HangingOwnerFinder>) {
    let finder = Arc::new(HangingOwnerFinder {
        hanging: scenario
            .scripts
            .iter()
            .filter(|script| script.analysis_hangs)
            .map(|script| script.address.to_bytes())
            .collect(),
        ..Default::default()
    });
    let tokens = tokio::time::timeout(
        outer_timeout,
        processor.get_tokens(scenario.input.clone(), finder.clone(), BlockTag::Latest),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("seed {}: get_tokens exceeded {outer_timeout:?}: {scenario:?}", scenario.seed)
    });
    (tokens, finder)
}

fn check_invariants(scenario: &Scenario, tokens: &[Token], finder: &HangingOwnerFinder) {
    let seed = scenario.seed;
    let mut seen = HashSet::new();
    let expected_order: Vec<_> = scenario
        .input
        .iter()
        .filter(|address| seen.insert((*address).clone()))
        .cloned()
        .collect();
    let actual_order: Vec<_> = tokens
        .iter()
        .map(|token| token.address.clone())
        .collect();
    assert_eq!(
        actual_order, expected_order,
        "seed {seed}: one token per unique input, in first-occurrence order: {scenario:?}"
    );
    let by_address: HashMap<_, _> = scenario
        .scripts
        .iter()
        .map(|script| (script.address.to_bytes(), script))
        .collect();
    let mut blocked_ahead = 0;
    let mut slots = 0;
    for token in tokens {
        let script = by_address[&token.address];
        let has_slot = blocked_ahead < MAX_CONCURRENT_TOKENS;
        slots += usize::from(has_slot);
        // A slot is held until both metadata and analysis finish: forever if the RPC never
        // answers, or if this token's analysis hangs.
        let holds_slot_forever =
            scenario.rpc == RpcBehaviour::NeverAnswers || script.analysis_hangs;
        blocked_ahead += usize::from(holds_slot_forever);
        let context =
            format!("seed {seed} ({:?}, slot: {has_slot}): {script:?} -> {token:?}", scenario.rpc);
        match expected_outcome(scenario, script, has_slot) {
            Expected::Pending => {
                let placeholder = Token::pending(&token.address, Chain::Ethereum);
                assert_eq!(
                    serde_json::to_value(token).unwrap(),
                    serde_json::to_value(&placeholder).unwrap(),
                    "{context}: must stay the untouched pending placeholder"
                );
            }
            Expected::Ready { symbol, decimals } => {
                assert_eq!(token.metadata_status, TokenMetadataStatus::Ready, "{context}");
                assert_eq!((&token.symbol, token.decimals), (&symbol, decimals), "{context}");
                assert!(
                    !(token.symbol == token.address.to_string() &&
                        token.decimals == 0 &&
                        token.quality == 0),
                    "{context}: a ready token must never carry the pending placeholder"
                );
            }
        }
    }
    assert_eq!(
        finder.calls.load(Ordering::SeqCst),
        slots,
        "seed {seed}: exactly the tokens that got a concurrency slot start analysis: {scenario:?}"
    );
    assert_eq!(
        finder.active.load(Ordering::SeqCst),
        0,
        "seed {seed}: every unfinished analysis must be dropped on return: {scenario:?}"
    );
}

#[tokio::test]
async fn get_tokens_invariants_hold_under_random_stalls() {
    // Each seed draws 1..=12 tokens with a random metadata answer and a random analysis hang,
    // shuffles them and appends duplicates. Scripted seeds share one mock server that routes
    // by token address; never-answering seeds each bind their own silent socket. Any hanging
    // token costs the full budget, so seeds run a few at a time to keep the test short.
    const SCRIPTED_SEEDS: u64 = 150;
    const NEVER_ANSWERING_SEEDS: u64 = 40;
    const BUDGET: Duration = Duration::from_millis(300);
    const OUTER_TIMEOUT: Duration = Duration::from_millis(1_500);
    const CONCURRENT_SEEDS: usize = 8;

    let scripts: Scripts = Default::default();
    let mut server = Server::new_async().await;
    let _metadata = server
        .mock("POST", "/")
        .with_body_from_request(scripted_mock(scripts.clone()))
        .create_async()
        .await;
    let rpc = EthereumRpcClient::new(&server.url()).unwrap();
    let scripted = EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO)
        .with_enrichment_budget(BUDGET);

    let scenarios = (0..SCRIPTED_SEEDS)
        .map(|seed| scenario(seed, RpcBehaviour::Scripted))
        .chain(
            (SCRIPTED_SEEDS..SCRIPTED_SEEDS + NEVER_ANSWERING_SEEDS)
                .map(|seed| scenario(seed, RpcBehaviour::NeverAnswers)),
        );
    stream::iter(scenarios)
        .for_each_concurrent(CONCURRENT_SEEDS, |scenario| {
            let scripted = &scripted;
            let scripts = &scripts;
            async move {
                let (tokens, finder) = match scenario.rpc {
                    RpcBehaviour::Scripted => {
                        scripts.lock().unwrap().extend(
                            scenario
                                .scripts
                                .iter()
                                .map(|script| (script.address.to_bytes(), script.metadata.clone())),
                        );
                        get_tokens_for(&scenario, scripted, OUTER_TIMEOUT).await
                    }
                    RpcBehaviour::NeverAnswers => {
                        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                            .await
                            .unwrap();
                        let url = format!("http://{}", listener.local_addr().unwrap());
                        let rpc = EthereumRpcClient::new(&url).unwrap();
                        let processor =
                            EthereumTokenPreProcessor::new(&rpc, Chain::Ethereum, Address::ZERO)
                                .with_enrichment_budget(BUDGET);
                        get_tokens_for(&scenario, &processor, OUTER_TIMEOUT).await
                    }
                };
                check_invariants(&scenario, &tokens, &finder);
            }
        })
        .await;
}
