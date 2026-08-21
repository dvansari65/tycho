#[path = "../benches/common/mod.rs"]
mod common;

use num_bigint::BigUint;
use rstest::rstest;

/// Replays each committed fixture through the real decoder and pins the exact simulation
/// outputs. The fixtures capture one deterministic block state, so any drift in `get_amount_out`
/// (amount or gas) or in the lazily computed `spot_price` is a behavior change, not noise.
#[rstest]
#[case::balancer_v2_2token("balancer_v2_2token", "26189156316", 120_192u64, 261_412.494_292_522_72)]
#[case::curve_3token("curve_3token", "1445850111629", 154_780u64, 0.001_427_059_136_976_352_2)]
#[case::curve_4token("curve_4token", "1000710034173560", 151_713u64, 1.000_548_926_351_498)]
fn decode_and_simulate(
    #[case] fixture: &str,
    #[case] expected_amount: &str,
    #[case] expected_gas: u64,
    #[case] expected_spot_price: f64,
) {
    let pools = common::load_pools(fixture);
    assert_eq!(
        pools.len(),
        1,
        "Expected exactly one decoded pool from {fixture} fixture, got {}",
        pools.len()
    );

    let (t_in, t_out) = common::pool_tokens(fixture);

    let state = pools
        .values()
        .next()
        .expect("pool count asserted to be 1");

    let amount_in = BigUint::from(1_000_000_000_000_000u64);
    let result = state
        .get_amount_out(amount_in, &t_in, &t_out)
        .unwrap_or_else(|e| {
            panic!(
                "get_amount_out failed for {fixture}: {} -> {}: {e:?}",
                t_in.symbol, t_out.symbol
            )
        });

    assert_eq!(
        result.amount,
        expected_amount
            .parse::<BigUint>()
            .expect("expected_amount is a decimal integer"),
        "amount_out drifted for {fixture}: {} -> {}",
        t_in.symbol,
        t_out.symbol
    );
    assert_eq!(
        result.gas,
        BigUint::from(expected_gas),
        "gas drifted for {fixture}: {} -> {}",
        t_in.symbol,
        t_out.symbol
    );

    let spot_price = state
        .spot_price(&t_in, &t_out)
        .unwrap_or_else(|e| panic!("spot_price failed for {fixture}: {e:?}"));
    assert_eq!(
        spot_price, expected_spot_price,
        "spot_price drifted for {fixture}: {} -> {}",
        t_in.symbol, t_out.symbol
    );
}
