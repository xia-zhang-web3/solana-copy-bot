use super::*;
#[path = "http_anchor_difference_tests.rs"]
mod anchor_difference_tests;
#[path = "http_anchor_meta_difference_tests.rs"]
mod anchor_meta_difference_tests;
#[path = "http_delivery_tests.rs"]
mod delivery_tests;
#[path = "http_recovery_error_tests.rs"]
mod diagnostic_tests;
#[path = "http_reward_compatibility_tests.rs"]
mod reward_compatibility_tests;
use copybot_core_types::association_delivery::InfoIdentity;
use prost::Message;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn raw_block() -> Value {
    let key = bs58::encode([1u8; 32]).into_string();
    let program = bs58::encode([2u8; 32]).into_string();
    json!({"parentSlot":9,"blockhash":key,"previousBlockhash":program,"blockTime":1700000000,
        "blockHeight":8,"numRewardPartitions":2,
        "rewards":[{"pubkey":key,"lamports":-7,"postBalance":44,"rewardType":"rent","commission":null,"commissionBps":42}],
        "transactions":[{"version":0,"transaction":{"signatures":[bs58::encode([7u8;64]).into_string()],
            "message":{"accountKeys":[key,program],"header":{"numRequiredSignatures":1,
                "numReadonlySignedAccounts":0,"numReadonlyUnsignedAccounts":1},"recentBlockhash":key,
                "instructions":[{"programIdIndex":1,"accounts":[0],"data":""}],"addressTableLookups":[]}},
            "meta":{"err":null,"status":{"Ok":null},"fee":5,"preBalances":[100,0],"postBalances":[95,0],
                "preTokenBalances":[{"accountIndex":0,"mint":key,"owner":key,"programId":program,
                    "uiTokenAmount":{"amount":"101","decimals":2,"uiAmount":1.01,"uiAmountString":"1.01"}}],
                "postTokenBalances":[],"loadedAddresses":{"writable":[],"readonly":[]},
                "innerInstructions":[{"index":0,"instructions":[{"programIdIndex":1,"accounts":[0],"data":"","stackHeight":2}]}],
                "logMessages":["original log"],"rewards":[{"pubkey":key,"lamports":3,"postBalance":55,
                    "rewardType":"staking","commission":4,"commissionBps":null}],
                "returnData":{"programId":program,"data":["AQID","base64"]},"computeUnitsConsumed":72,"costUnits":73}}]})
}

#[test]
fn complete_common_identity_keeps_money_error_reward_return_data_and_float_bits() {
    let raw = raw_block();
    let parsed = block::parse(10, &raw).unwrap();
    let info = &parsed.transactions[0];
    let meta = info.meta.as_ref().unwrap();
    assert_eq!(meta.rewards[0].lamports, 3);
    assert_eq!(meta.return_data.as_ref().unwrap().data, vec![1, 2, 3]);
    assert!(!meta.return_data_none);
    assert_eq!(
        parsed
            .rewards
            .as_ref()
            .unwrap()
            .num_partitions
            .as_ref()
            .unwrap()
            .num_partitions,
        2
    );
    let retained = InfoIdentity {
        encoded: info.encode_to_vec(),
        float_bits: vec![Some(1.01f64.to_bits())],
    };
    assert!(identity::info_equivalent(&retained, &retained));
    let mut changed = info.clone();
    changed.meta.as_mut().unwrap().rewards[0].lamports += 1;
    assert!(!identity::info_equal(info, &changed));
    changed = info.clone();
    changed
        .meta
        .as_mut()
        .unwrap()
        .return_data
        .as_mut()
        .unwrap()
        .data[0] = 9;
    assert!(!identity::info_equal(info, &changed));
    changed = info.clone();
    changed.meta.as_mut().unwrap().cost_units = Some(74);
    assert!(!identity::info_equal(info, &changed));
    changed = info.clone();
    changed.is_vote = !changed.is_vote;
    assert!(!identity::info_equal(info, &changed));
    changed = info.clone();
    changed.meta.as_mut().unwrap().pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = -0.0;
    let mut positive = changed.clone();
    positive.meta.as_mut().unwrap().pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = 0.0;
    assert!(!identity::info_equal(&positive, &changed));
    let mut failure = raw.clone();
    failure["transactions"][0]["meta"]["err"] = json!({"InstructionError":[1,{"Custom":61}]});
    failure["transactions"][0]["meta"]["status"] =
        json!({"Err":{"InstructionError":[1,{"Custom":61}]}});
    let failure = block::parse(10, &failure).unwrap();
    let err = &failure.transactions[0]
        .meta
        .as_ref()
        .unwrap()
        .err
        .as_ref()
        .unwrap()
        .err;
    assert_eq!(err, &vec![8, 0, 0, 0, 1, 25, 0, 0, 0, 61, 0, 0, 0]);
    let mut native = failure.clone();
    native.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .err
        .as_mut()
        .unwrap()
        .err = vec![8, 0, 0, 0, 1, 25, 0, 0, 0, 61, 0, 0, 0];
    assert!(identity::block_equivalent(&failure, &native));
    native.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .err
        .as_mut()
        .unwrap()
        .err[9] = 62;
    assert!(!identity::block_equivalent(&failure, &native));
    assert!(error::encode(&json!("FutureUnknownError")).is_err());
    assert!(error::encode(&json!({"InstructionError":[0,{"Custom":4294967296u64}]})).is_err());
}

#[test]
fn marker_independent_of_version_and_version_one_do_not_bless_financial_target() {
    let mut raw = raw_block();
    raw["transactions"][0]["version"] = 1.into();
    assert!(block::parse(10, &raw)
        .unwrap_err()
        .to_string()
        .contains("v1_config_missing"));
    raw["transactions"][0]["transaction"]["message"]["transactionConfig"] =
        json!({"priorityFee":0,"computeUnitLimit":100});
    let block = block::parse(10, &raw).unwrap();
    assert!(crate::source::yellowstone_facts::unsupported_message_config(&block.transactions[0]));
    raw["transactions"][0]["version"] = 0.into();
    raw["transactions"][0]["transaction"]["message"]["transactionConfig"] = Value::Null;
    let block = block::parse(10, &raw).unwrap();
    assert!(crate::source::yellowstone_facts::unsupported_message_config(&block.transactions[0]));
    raw["transactions"][0]["transaction"]["message"]["transactionConfig"] =
        json!({"unknownBudget":1});
    assert!(block::parse(10, &raw).is_err());
}

#[test]
fn null_duplicate_and_invalid_complete_evidence_do_not_become_blocks() {
    assert!(block::parse(10, &Value::Null)
        .unwrap_err()
        .to_string()
        .contains("unavailable_or_unconfirmed"));
    let mut raw = raw_block();
    let duplicate = raw["transactions"][0].clone();
    raw["transactions"].as_array_mut().unwrap().push(duplicate);
    assert!(block::parse(10, &raw)
        .unwrap_err()
        .to_string()
        .contains("duplicate_signature"));
    let mut raw = raw_block();
    raw["transactions"][0]["meta"]["postBalances"] = json!([1]);
    assert!(block::parse(10, &raw)
        .unwrap_err()
        .to_string()
        .contains("balance_count"));
    let mut raw = raw_block();
    raw["transactions"][0]["meta"]["returnData"]["data"] = json!(["AQI=", "base58"]);
    assert!(block::parse(10, &raw).is_err());
    assert!(value::base64("AR==").is_err());
}

async fn server(results: Vec<Value>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for result in results {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let end = loop {
                let mut part = [0u8; 4096];
                let n = socket.read(&mut part).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&part[..n]);
                if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() - end < length {
                let mut part = [0u8; 4096];
                let n = socket.read(&mut part).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&part[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            let envelope = if let Some(error) = result.get("error") {
                json!({"jsonrpc":"2.0","id":request["id"],"error":error})
            } else {
                json!({"jsonrpc":"2.0","id":request["id"],"result":result})
            };
            let body = serde_json::to_vec(&envelope).unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            requests.push(request);
        }
        requests
    });
    (url, task)
}

#[tokio::test]
async fn actual_http_adapter_uses_confirmed_broker_and_preserves_response() {
    let (url, task) = server(vec![json!([10, 12]), raw_block()]).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 2_000_000, Duration::from_secs(2)).unwrap();
    assert_eq!(client.slots(10, 12).await.unwrap(), vec![10, 12]);
    let block = client.block(10).await.unwrap();
    assert_eq!(block.block.slot, 10);
    assert_eq!(
        serde_json::from_slice::<Value>(&block.raw_response).unwrap()["result"],
        raw_block()
    );
    let requests = task.await.unwrap();
    assert_eq!(requests[0]["method"], "getBlocks");
    assert_eq!(requests[0]["params"][2]["commitment"], "confirmed");
    assert_eq!(
        requests[1]["params"][1]["maxSupportedTransactionVersion"],
        1
    );
    assert_eq!(requests[1]["params"][1]["rewards"], true);
    assert!(ConfirmedHttpRecovery::new(
        "https://example.com",
        None,
        1024,
        1000,
        Duration::from_secs(2)
    )
    .is_err());
    assert!(client.slots(10, 1034).await.is_err());
}

#[tokio::test]
async fn actual_http_adapter_refuses_ordering_null_and_byte_overrun() {
    let (url, task) = server(vec![json!([12, 10]), Value::Null]).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 2_000_000, Duration::from_secs(2)).unwrap();
    assert!(client
        .slots(10, 12)
        .await
        .unwrap_err()
        .to_string()
        .contains("unsorted_or_outside"));
    assert!(client
        .block(10)
        .await
        .unwrap_err()
        .to_string()
        .contains("unavailable_or_unconfirmed"));
    task.await.unwrap();
    let (url, task) = server(vec![raw_block()]).await;
    let client = ConfirmedHttpRecovery::new(&url, None, 1024, 128, Duration::from_secs(2)).unwrap();
    assert!(client
        .block(10)
        .await
        .unwrap_err()
        .to_string()
        .contains("response_limit"));
    task.await.unwrap();
}

#[test]
#[ignore = "reads sealed saved corpus only when explicitly requested"]
fn saved_mixed_version_complete_blocks_preserve_classification() {
    let path = std::env::var("COPYBOT_HTTP_RECOVERY_CORPUS_FILE").unwrap();
    let record: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let slot = record["params"][0]
        .as_u64()
        .or_else(|| record["slot"].as_u64())
        .unwrap();
    // This captured request disabled rewards. Its null reward fields cannot
    // satisfy a production rewards=true complete-block response.
    assert!(record["result"]["rewards"].is_null());
    assert!(block::parse(slot, &record["result"]).is_err());
    // Explicit fixture projection only; original saved bytes remain unchanged.
    // No historical reward/rent claim follows from modeled empty arrays.
    let mut projected = record["result"].clone();
    projected["rewards"] = json!([]);
    projected["numRewardPartitions"] = Value::Null;
    for tx in projected["transactions"].as_array_mut().unwrap() {
        if tx["meta"]["rewards"].is_null() {
            tx["meta"]["rewards"] = json!([]);
        }
    }
    let block = block::parse(slot, &projected).unwrap();
    assert_eq!(
        block.transactions.len(),
        record["result"]["transactions"].as_array().unwrap().len()
    );
    let marked = block
        .transactions
        .iter()
        .filter(|tx| crate::source::yellowstone_facts::unsupported_message_config(tx))
        .count();
    assert!(marked > 100);
    assert!(block.transactions.iter().all(|tx| tx.meta.is_some()));
    let rays = std::collections::HashSet::from([
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
        "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C".to_string(),
    ]);
    let pumps =
        std::collections::HashSet::from(
            ["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()],
        );
    let interested = rays.union(&pumps).cloned().collect();
    let mut decoded = 0;
    for info in &block.transactions {
        let tx = yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction {
            slot,
            transaction: Some(info.clone()),
        };
        let result = crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
            &tx,
            &interested,
            &rays,
            &pumps,
        );
        if crate::source::yellowstone_facts::unsupported_message_config(info) {
            assert!(result.facts.unwrap().is_none());
        } else if result.facts.unwrap().is_some() {
            decoded += 1;
        }
    }
    assert!(
        decoded > 0,
        "complete production normalization must retain compatible swaps"
    );
}

#[tokio::test]
async fn actual_http_adapter_keeps_rpc_refusal_and_does_not_retry_it() {
    let (url, task) = server(vec![
        json!({"error":{"code":-32004,"message":"Block not available"}}),
    ])
    .await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 1000, Duration::from_secs(2)).unwrap();
    let reason = client.block(10).await.unwrap_err().to_string();
    assert!(reason.contains("http_status=200"));
    assert!(reason.contains("code=-32004"));
    assert!(reason.contains("Block not available"));
    assert_eq!(task.await.unwrap().len(), 1);
}
