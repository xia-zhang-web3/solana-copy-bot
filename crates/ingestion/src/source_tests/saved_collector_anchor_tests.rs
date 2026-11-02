//! Actual 221-transaction failure, retained in full; no single-transaction substitute.
use super::anchor_complete_replay::local_server;
use crate::source::http_recovery::{complete_anchor, identity, ConfirmedHttpRecovery};
use crate::source::tests::{
    anchor_tail_consumers::projection,
    anchor_tail_pair::{execution_order, load, Pair},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, time::Duration};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};

fn pair() -> Pair {
    let p = load();
    assert_eq!(
        format!(
            "{:x}",
            Sha256::digest(std::fs::read(p.root.join("manifest.json")).unwrap())
        ),
        "18134d69b6670856b02ea5eb16f838e1a8f92bb8ebdb828977c04c16a98eec6d"
    );
    assert_eq!(p.grpc.slot, 464668239);
    assert_eq!(p.grpc.transactions.len(), 221);
    assert_eq!(
        p.manifest["files"]["grpc_typed.pb"]["sha256"],
        "bbda9a3fefe562a6d94732706de0b1ca375b9a1206b7526956323c69c73fef5a"
    );
    assert_eq!(
        p.manifest["files"]["http_response.json"]["sha256"],
        "8908b35be0be65bc960bfae6b477dea01b16dec5833dd185a3ef63dbec18c109"
    );
    p
}
fn changed(b: &mut SubscribeUpdateBlock) -> &mut SubscribeUpdateTransactionInfo {
    b.transactions.iter_mut().find(|i| i.index == 211).unwrap()
}
fn client(url: &str) -> ConfirmedHttpRecovery {
    ConfirmedHttpRecovery::new(url, None, 4, 8 << 20, Duration::from_secs(10)).unwrap()
}

#[tokio::test]
#[ignore = "pinned real 221-transaction pair; LOCAL fetch only"]
async fn saved_collector_full_pair_current_selection_and_consumers() {
    let p = pair();
    assert!(!identity::block_equivalent(&p.http, &p.grpc));
    assert!(complete_anchor::eligible(&p.http, &p.grpc));
    assert!(!complete_anchor::eligible(&p.grpc, &p.http));
    let mut h = p.http.clone();
    let mut g = p.grpc.clone();
    let hm = changed(&mut h).meta.as_ref().unwrap();
    let gm = changed(&mut g).meta.as_ref().unwrap();
    assert!(hm.err.is_some()); // This transaction cannot emit swap facts.
    assert_eq!(hm.log_messages.len(), 154);
    assert_eq!(gm.log_messages.len(), 147);
    assert!(complete_anchor::collector_logs_agree(
        &hm.log_messages,
        &gm.log_messages
    ));
    let server = local_server(BTreeMap::from([(p.grpc.slot, p.raw_http.clone())]), vec![]).await;
    let tmp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let choice = client(&server.http)
        .current_anchor(
            p.grpc.slot,
            &p.grpc,
            Some(tmp.path().to_str().unwrap()),
            "collector:0".into(),
        )
        .await
        .unwrap()
        .for_context("collector:0")
        .unwrap();
    assert_eq!(choice.block(), &p.http);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(tmp.path().join("pair-01/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["selected_input"], "HTTP");
    assert_eq!(manifest["hidden_payload_equality_claimed"], false);
    let mut rows = vec![];
    let mut differences = vec![];
    for ((selected, direct), live) in execution_order(choice.block())
        .into_iter()
        .zip(execution_order(&p.http))
        .zip(execution_order(&p.grpc))
    {
        let actual = projection(selected, p.http.slot);
        assert_eq!(actual, projection(direct, p.http.slot));
        if actual != projection(live, p.grpc.slot) {
            differences.push(direct.index);
        }
        if direct.index == 211 {
            assert_ne!(actual["facts"]["status"], "checked");
        }
        rows.push(actual);
    }
    let calls = server.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["method"], "getBlock");
    assert_eq!(calls[0]["params"][1]["transactionDetails"], "full");
    let proof = json!({"kind":"FULL_SAVED_COLLECTOR_PAIR_CURRENT_HTTP_SELECTION",
        "slot":p.grpc.slot,"transactions":rows.len(),"all_selected_consumer_projections_equal_direct_HTTP":true,
        "live_projection_difference_indices":differences,"rows":rows,"selection_manifest":manifest,
        "provider_requests":0,"financial_actions":0,"historical_freshness_retagged":false,
        "transport":"loopback; RPC id and JSON encoding rebound; original full result preserved"});
    if let Ok(path) = std::env::var("COPYBOT_COLLECTOR_PROOF") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        serde_json::to_writer_pretty(&mut f, &proof).unwrap();
        f.write_all(b"\n").unwrap();
        f.sync_all().unwrap();
    }
    eprintln!("SAVED_COLLECTOR_FULL_PAIR transactions=221 current_HTTP_selected=true consumers=direct_HTTP live_projection_changes={differences:?}");
}

#[test]
#[ignore = "pinned real full pair; LOCAL causal mutations"]
fn saved_collector_full_pair_identity_and_available_log_refusals() {
    let p = pair();
    assert!(complete_anchor::eligible(&p.http, &p.grpc));
    for mode in [
        "slot",
        "parent",
        "hash",
        "time",
        "count",
        "message",
        "instruction",
        "signature",
        "index",
        "fee",
        "native_balance",
        "token_amount",
        "token_UI_bits",
        "CPI",
        "status",
        "return_data",
        "compute",
        "log_flag",
        "known_log",
        "missing_HTTP_tx",
        "unrelated_fee",
    ] {
        let mut h = p.http.clone();
        let t = changed(&mut h);
        let m = t.meta.as_mut().unwrap();
        match mode {
            "slot" => h.slot += 1,
            "parent" => h.parent_slot -= 1,
            "hash" => h.blockhash.push('x'),
            "time" => h.block_time.as_mut().unwrap().timestamp += 1,
            "count" => h.executed_transaction_count -= 1,
            "message" => {
                t.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .recent_blockhash[0] ^= 1
            }
            "instruction" => t
                .transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions[0]
                .data
                .push(1),
            "signature" => t.signature[0] ^= 1,
            "index" => t.index = 0,
            "fee" => m.fee += 1,
            "native_balance" => m.post_balances[0] += 1,
            "token_amount" => m.post_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .amount
                .push('0'),
            "token_UI_bits" => {
                let a = m.post_token_balances[0].ui_token_amount.as_mut().unwrap();
                a.ui_amount = f64::from_bits(a.ui_amount.to_bits() ^ 1);
            }
            "CPI" => m.inner_instructions[0].index += 1,
            "status" => m.err = None,
            "return_data" => m.return_data_none = !m.return_data_none,
            "compute" => m.compute_units_consumed = Some(0),
            "log_flag" => m.log_messages_none = true,
            "known_log" => m.log_messages[0].push('x'),
            "missing_HTTP_tx" => {
                h.transactions.pop();
            }
            "unrelated_fee" => h.transactions[0].meta.as_mut().unwrap().fee += 1,
            _ => unreachable!(),
        }
        assert!(!complete_anchor::eligible(&h, &p.grpc), "accepted {mode}");
    }
    for mode in [
        "missing",
        "changed",
        "reordered",
        "second_marker",
        "non_bare",
        "extra",
    ] {
        let mut g = p.grpc.clone();
        let logs = &mut changed(&mut g).meta.as_mut().unwrap().log_messages;
        match mode {
            "missing" => {
                logs.remove(142);
            }
            "changed" => logs[142].push('x'),
            "reordered" => logs.swap(142, 143),
            "second_marker" => logs.push("Log truncated".into()),
            "non_bare" => logs[141].push(' '),
            "extra" => logs.push("Program log: extra".into()),
            _ => unreachable!(),
        }
        assert!(!complete_anchor::eligible(&p.http, &g), "available {mode}");
    }
    eprintln!("SAVED_COLLECTOR_FULL_PAIR_REFUSALS identity=21 available_logs=6");
}

#[tokio::test]
#[ignore = "pinned real full pair; missing/stale/partial current-response refusals"]
async fn saved_collector_full_pair_current_response_refusals() {
    let p = pair();
    let good = local_server(BTreeMap::from([(p.grpc.slot, p.raw_http.clone())]), vec![]).await;
    let http = client(&good.http);
    assert!(http
        .current_anchor(p.grpc.slot + 1, &p.grpc, None, "ctx:0".into())
        .await
        .is_err());
    assert!(http
        .current_anchor(p.grpc.slot, &p.grpc, None, String::new())
        .await
        .is_err());
    assert!(good.calls.lock().unwrap().is_empty());
    let selected = http
        .current_anchor(p.grpc.slot, &p.grpc, None, "ctx:0".into())
        .await
        .unwrap();
    assert!(selected.for_context("ctx:1").is_err());
    for mode in [
        "missing",
        "partial",
        "truncated_HTTP",
        "amount",
        "message",
        "parent",
    ] {
        let mut raw: Value = serde_json::from_slice(&p.raw_http).unwrap();
        match mode {
            "missing" => raw["result"] = Value::Null,
            "partial" => raw["result"]["transactions"][211]["meta"] = Value::Null,
            "truncated_HTTP" => raw["result"]["transactions"][211]["meta"]["logMessages"]
                .as_array_mut()
                .unwrap()
                .push("Log truncated".into()),
            "amount" => {
                raw["result"]["transactions"][211]["meta"]["postTokenBalances"][0]
                    ["uiTokenAmount"]["amount"] = "1".into()
            }
            "message" => {
                raw["result"]["transactions"][211]["transaction"]["message"]["recentBlockhash"] =
                    bs58::encode([9; 32]).into_string().into()
            }
            "parent" => raw["result"]["parentSlot"] = (p.grpc.parent_slot - 1).into(),
            _ => unreachable!(),
        }
        let bad = local_server(
            BTreeMap::from([(p.grpc.slot, serde_json::to_vec(&raw).unwrap())]),
            vec![],
        )
        .await;
        assert!(
            client(&bad.http)
                .current_anchor(p.grpc.slot, &p.grpc, None, "ctx:0".into())
                .await
                .is_err(),
            "{mode}"
        );
    }
    assert!(client("http://127.0.0.1:1")
        .current_anchor(p.grpc.slot, &p.grpc, None, "ctx:0".into())
        .await
        .is_err());
}
