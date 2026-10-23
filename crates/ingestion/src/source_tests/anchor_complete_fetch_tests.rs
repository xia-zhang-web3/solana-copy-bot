//! Current bounded HTTP response selection and actual downstream consumer effect.
use super::anchor_complete_replay::local_server;
use super::*;
use crate::source::http_recovery::{identity, ConfirmedHttpRecovery};
use crate::source::tests::{
    anchor_tail_consumers::{policy, projection},
    anchor_tail_pair::{execution_order, load},
};
use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt};
async fn start_server(body: Vec<u8>, slot: u64) -> Servers {
    // Reused transport's parent resolver is anchor_complete_replay::corpus.
    local_server(BTreeMap::from([(slot, body)]), vec![]).await
}
fn http(server: &Servers) -> ConfirmedHttpRecovery {
    ConfirmedHttpRecovery::new(&server.http, None, 4, 8 << 20, Duration::from_secs(10)).unwrap()
}
#[tokio::test]
#[ignore = "pinned COPY09 pair; real consumers, LOCAL current fetch only"]
async fn copy09_anchor_complete_current_fetch_reaches_direct_http_consumers() {
    let pair = load();
    let server = start_server(pair.raw_http.clone(), pair.grpc.slot).await;
    let client = http(&server);
    let tmp = tempfile::tempdir().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let selected = client
        .current_anchor(
            pair.grpc.slot,
            &pair.grpc,
            Some(tmp.path().to_str().unwrap()),
            "LOCAL-current:0".into(),
        )
        .await
        .unwrap()
        .for_context("LOCAL-current:0")
        .unwrap();
    assert_eq!(selected.block(), &pair.http);
    assert!(!identity::block_equivalent(selected.block(), &pair.grpc));
    let direct = execution_order(&pair.http);
    let raw_live = execution_order(&pair.grpc);
    let mut changes = vec![];
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    let mut projections = vec![];
    for ((selected, direct), live) in execution_order(selected.block())
        .into_iter()
        .zip(direct)
        .zip(raw_live)
    {
        let actual = projection(selected, pair.http.slot);
        assert_eq!(
            actual,
            projection(direct, pair.http.slot),
            "chosen input must equal direct complete input effect"
        );
        let truncated = projection(live, pair.grpc.slot);
        if actual != truncated {
            changes.push(
                json!({"execution_index":direct.index,"truncated":truncated,"complete":actual}),
            );
        }
        for branch in ["facts", "native"] {
            *counts
                .entry(format!(
                    "{branch}:{}",
                    actual[branch]["status"].as_str().unwrap()
                ))
                .or_default() += 1;
        }
        projections.push(actual);
    }
    let manifest: Value =
        serde_json::from_slice(&fs::read(tmp.path().join("pair-01/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["comparison"], "HTTP_SELECTED");
    assert_eq!(manifest["comparison_kind"], "TRUSTED_COMPLETE_HTTP");
    assert_eq!(
        manifest["comparison_scope"],
        "TRUSTED_HTTP_COMPLETE_REPRESENTATION"
    );
    assert_eq!(manifest["selected_input"], "HTTP");
    assert_eq!(manifest["hidden_payload_equality_claimed"], false);
    assert_eq!(manifest["ack_proven"], false);
    assert!(!manifest["first_mismatch"].is_null());
    let calls = server.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["method"], "getBlock");
    assert_eq!(calls[0]["params"][0], pair.grpc.slot);
    assert_eq!(calls[0]["params"][1]["commitment"], "confirmed");
    assert_eq!(calls[0]["params"][1]["transactionDetails"], "full");
    let changed = pair
        .http
        .transactions
        .iter()
        .find(|h| {
            !identity::info_equal(
                h,
                pair.grpc
                    .transactions
                    .iter()
                    .find(|g| g.index == h.index)
                    .unwrap(),
            )
        })
        .unwrap();
    let g = pair
        .grpc
        .transactions
        .iter()
        .find(|g| g.index == changed.index)
        .unwrap();
    let tail = &changed.meta.as_ref().unwrap().log_messages
        [g.meta.as_ref().unwrap().log_messages.len() - 1..];
    let (interest, rays, pumps) = policy();
    let invokes:Vec<_>=tail.iter().filter(|line|line.contains(" invoke [")).map(|line| {
        let key=line.strip_prefix("Program ").unwrap().split_once(' ').unwrap().0;
        json!({"record":line,"actual_source_interest":interest.contains(key),"raydium":rays.contains(key),"pumpswap":pumps.contains(key)})
    }).collect();
    let result = json!({"kind":"LOCAL_CURRENT_TRUSTED_COMPLETE_HTTP_CONSUMER_EFFECT", "slot":pair.http.slot,
        "transactions":projections.len(),"direct_complete_effect":"ALL_SELECTED_ROWS_IDENTICAL",
        "truncated_vs_complete_changes":changes,"decoder_native_counts":counts,"rows":projections,
        "hidden_invocation_records":invokes,"HTTP_selection_manifest":manifest,"request":calls,
        "raw_payloads_preserved":true,"hidden_equality_claimed":false,"historical_freshness_retagged":false,
        "provider_requests":0,"financial_actions":0,"transport":"loopback mock/re-encoded RPC envelope; same original result"});
    if let Ok(path) = std::env::var("COPYBOT_ANCHOR_CONSUMER_OUTPUT") {
        assert!(!std::path::Path::new(&path).exists());
        fs::write(path, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    }
    eprintln!(
        "COPY09_SELECTED_CONSUMERS transactions={} changed_projections={} directHTTP=true",
        result["transactions"],
        result["truncated_vs_complete_changes"]
            .as_array()
            .unwrap()
            .len()
    );
}

#[tokio::test]
#[ignore = "pinned COPY09 pair; missing/stale/malformed current fetch only"]
async fn copy09_anchor_complete_current_fetch_context_and_response_refusals() {
    let pair = load();
    let server = start_server(pair.raw_http.clone(), pair.grpc.slot).await;
    let client = http(&server);
    assert!(client
        .current_anchor(pair.grpc.slot + 1, &pair.grpc, None, "LOCAL:0".into())
        .await
        .is_err());
    assert!(client
        .current_anchor(pair.grpc.slot, &pair.grpc, None, String::new())
        .await
        .is_err());
    assert!(
        server.calls.lock().unwrap().is_empty(),
        "invalid context cannot mint from response"
    );
    let choice = client
        .current_anchor(pair.grpc.slot, &pair.grpc, None, "LOCAL:0".into())
        .await
        .unwrap();
    assert!(choice
        .for_context("LOCAL:1")
        .err()
        .unwrap()
        .to_string()
        .contains("stale_association_context"));
    for mode in [
        "missing",
        "partial",
        "second_truncation",
        "prefix",
        "parent",
    ] {
        let mut body: Value = serde_json::from_slice(&pair.raw_http).unwrap();
        let changed = pair
            .http
            .transactions
            .iter()
            .find(|h| {
                !identity::info_equal(
                    h,
                    pair.grpc
                        .transactions
                        .iter()
                        .find(|g| g.index == h.index)
                        .unwrap(),
                )
            })
            .unwrap()
            .index as usize;
        match mode {
            "missing" => body["result"] = Value::Null,
            "partial" => body["result"]["transactions"][changed]["meta"] = Value::Null,
            "second_truncation" => body["result"]["transactions"][changed]["meta"]["logMessages"]
                .as_array_mut()
                .unwrap()
                .push("Log truncated".into()),
            "prefix" => {
                body["result"]["transactions"][changed]["meta"]["logMessages"][0] =
                    "changed available record".into()
            }
            _ => {
                body["result"]["previousBlockhash"] = bs58::encode([0xab; 32]).into_string().into()
            }
        }
        let bad = start_server(serde_json::to_vec(&body).unwrap(), pair.grpc.slot).await;
        assert!(
            http(&bad)
                .current_anchor(pair.grpc.slot, &pair.grpc, None, "LOCAL:0".into())
                .await
                .is_err(),
            "{mode} cannot authorize selection"
        );
    }
    let dead = ConfirmedHttpRecovery::new(
        "http://127.0.0.1:1",
        None,
        4,
        8 << 20,
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(
        dead.current_anchor(pair.grpc.slot, &pair.grpc, None, "LOCAL:0".into())
            .await
            .is_err(),
        "missing current response refuses"
    );
}
