//! New optional reads are bounded and never replace the independent SOL path.
use anyhow::Result;
use copybot_discovery_v2::build_discovery_v2_status;
use serde_json::{json, Value};
use std::time::Duration;
#[allow(dead_code)]
#[path = "token2022_valuation_pipeline/fixture.rs"]
mod fixture;
#[path = "live_portfolio/rpc.rs"]
mod rpc;
use fixture::{metric, Fixture, PriceCase};

fn start(
    sol: u64,
    rows: Vec<Value>,
    reply: impl Fn(&Value) -> rpc::Reply + Send + 'static,
) -> rpc::RpcStub {
    rpc::RpcStub::start(move |request| match request["method"].as_str().unwrap() {
        "getBalance" => rpc::result(json!(sol)).into(),
        "getTokenAccountsByOwner" if request["params"][1]["programId"] == rpc::CLASSIC => {
            rpc::result(json!([])).into()
        }
        "getTokenAccountsByOwner" => rpc::result(json!(rows)).into(),
        "getMultipleAccounts" => reply(request),
        other => panic!("unexpected loopback method: {other}"),
    })
}

fn assert_unknown(row: &Value, reason: &str, positions: u64) {
    let proof = &row["live_valuation"];
    assert_eq!(proof["decision"]["known_supported_token_value_sol"], 0.0);
    assert_eq!(proof["decision"]["unvalued_positions"], positions);
    for contribution in proof["token_2022_positions"].as_array().unwrap() {
        assert!(contribution["token_2022_profile"].is_null());
        assert_eq!(contribution["token_2022_collection_failure"], reason);
        assert_eq!(
            contribution["initial_unknown_reason"],
            "token2022_profile_missing"
        );
    }
}

#[test]
fn stale_slot_malformed_envelope_and_oversize_body_remain_typed_unknown() -> Result<()> {
    for reason in ["slot_behind", "malformed", "budget", "unavailable"] {
        for sol in [0, 50_000_000] {
            let f = Fixture::new(PriceCase::Fresh)?;
            let rows = vec![rpc::account(
                &rpc::key('A'),
                &rpc::key('E'),
                &rpc::key('K'),
                rpc::TOKEN_2022,
                "500",
                3,
            )];
            let server = start(sol, rows, move |_| match reason {
                "slot_behind" => json!({"jsonrpc":"2.0", "id":1,
                    "result":{"context":{"slot":122},"value":[null,null]}})
                .into(),
                "malformed" => json!({"jsonrpc":"2.0", "id":99,
                    "result":{"context":{"slot":124},"value":[null,null]}})
                .into(),
                "budget" => rpc::Reply {
                    body: " ".repeat(256 * 1024 + 1),
                    delay: Duration::from_millis(1),
                    status: 200,
                },
                _ => rpc::Reply {
                    body: "{}".into(),
                    delay: Duration::ZERO,
                    status: 503,
                },
            });
            let status =
                build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, f.options(&server))?;
            assert_eq!(
                status.production_green,
                sol > 0,
                "{reason}: {:?}",
                status.blockers
            );
            assert_unknown(&metric(&status), reason, 1);
            let calls = server.finish();
            assert_eq!(calls.len(), 4, "optional reads have no retries");
            rpc::assert_requests(&calls, &rpc::key('A'), 3);
        }
    }
    Ok(())
}

fn address(index: usize) -> String {
    const A: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    // Thirty leading zero bytes plus two nonzero bytes: a canonical 32-byte key.
    let n = 256 + index;
    format!(
        "{}{}{}",
        "1".repeat(30),
        A[n / 58] as char,
        A[n % 58] as char
    )
}

#[test]
fn maximum_inventory_reads_256_distinct_keys_in_eight_non_retrying_batches() -> Result<()> {
    let mut f = Fixture::new(PriceCase::Fresh)?;
    f.discovery.live_portfolio_max_token_accounts = 128;
    let rows = (0..128)
        .map(|n| {
            rpc::account(
                &rpc::key('A'),
                &address(n),
                &address(n + 128),
                rpc::TOKEN_2022,
                "1",
                3,
            )
        })
        .collect();
    let server = start(0, rows, |request| {
        let size = request["params"][0].as_array().unwrap().len();
        rpc::result(json!(vec![Value::Null; size])).into()
    });
    let status = build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, f.options(&server))?;
    assert!(!status.production_green);
    assert_unknown(&metric(&status), "missing", 128);
    let calls = server.finish();
    assert_eq!(calls.len(), 11);
    rpc::assert_requests(&calls, &rpc::key('A'), 3);
    assert_eq!(
        calls
            .iter()
            .filter(|r| r["method"] == "getMultipleAccounts")
            .count(),
        8
    );
    assert!(calls
        .iter()
        .filter(|r| r["method"] == "getMultipleAccounts")
        .all(|r| r["params"][0].as_array().unwrap().len() == 32));
    Ok(())
}
