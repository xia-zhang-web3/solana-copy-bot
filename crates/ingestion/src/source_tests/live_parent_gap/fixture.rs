//! SYNTHETIC payloads: only slots/hashes 572..575 are retained readonly03 metadata.
//! Transaction574 reuses the accepted31 grammar with a MOCK signature/slot.
//! These are NOT historical blocks and cannot prove live SELL.
use super::*;
pub(super) const FIRST: u64 = 459_668_573;
fn swap() -> Value {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../../storage-core/tests/fixtures/canary31-source-buy-raw.json"
    ))
    .unwrap();
    value["transaction"]["signatures"] = json!([bs58::encode([44; 64]).into_string()]);
    value
}
pub(super) fn wallets() -> HashSet<String> {
    HashSet::from([swap()["transaction"]["message"]["accountKeys"][0]
        .as_str()
        .unwrap()
        .into()])
}
pub(super) fn key(slot: u64) -> String {
    match slot {
        459_668_572 => "EZMLpQ62egHGzbwWAWiccDA4HRGg5o469MugAn9i5cPU".into(),
        459_668_573 => "EKEHeLbXn2fU3Si4pAbk6wVd8oQM3HkfBkog5mGh4Xap".into(),
        459_668_574 => "J16RhQrHib46ApH49VeT5BfH79ZDtHPwLVTXy6Q6RLpc".into(),
        459_668_575 => "FRsaSQ2tr9vRwDiz7r5e45pDApcmnp4ukdsaoiGqcVXr".into(),
        _ => hash(slot),
    }
}
pub(super) fn raw_block(slot: u64) -> Value {
    json!({"parentSlot":slot-1,"previousBlockhash":key(slot-1),"blockhash":key(slot),
        "blockTime":1700000000+slot,"blockHeight":slot,
        "transactions":if slot == FIRST+1 { vec![swap()] } else { vec![] },"rewards":[]})
}
pub(super) fn complete(slot: u64) -> SubscribeUpdateBlock {
    crate::source::http_recovery::normalize_confirmed_http_block(slot, &raw_block(slot)).unwrap()
}
pub(super) async fn start(mode: &'static str) -> Servers {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(vec![]));
    let seen = calls.clone();
    let h = tokio::spawn(async move {
        while let Ok((mut socket, peer)) = listener.accept().await {
            assert!(peer.ip().is_loopback());
            let mut bytes = vec![];
            let mut chunk = [0; 4096];
            let (offset, length) = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|s| s.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
                assert!(bytes.len() < 1 << 20);
            };
            while bytes.len() < offset + length {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = if request["method"] == "getBlocks" {
                let from = request["params"][0].as_u64().unwrap();
                let to = request["params"][1].as_u64().unwrap();
                assert_eq!(request["params"][2]["commitment"], "confirmed");
                json!((FIRST - 1..=FIRST + 5)
                    .filter(|s| *s >= from
                        && *s <= to
                        && !(mode == "missing" && *s == FIRST + 1)
                        && !(mode == "overlap" && *s == FIRST))
                    .collect::<Vec<_>>())
            } else {
                assert_eq!(request["method"], "getBlock");
                assert_eq!(request["params"][1]["commitment"], "confirmed");
                let slot = request["params"][0].as_u64().unwrap();
                let mut raw = raw_block(slot);
                if mode == "stall" && slot == FIRST + 1 {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                }
                if mode == "wrong-hash" && slot == FIRST + 1 {
                    raw["blockhash"] = json!(hash(90));
                }
                if mode == "parent" && slot == FIRST + 1 {
                    raw["parentSlot"] = json!(FIRST - 1);
                    raw["previousBlockhash"] = json!(key(FIRST - 1));
                }
                if mode == "anchor" && slot == FIRST + 2 {
                    raw["blockhash"] = json!(hash(91));
                }
                if mode == "null" && slot == FIRST + 1 {
                    raw = Value::Null;
                }
                if mode == "info" && slot == FIRST + 2 {
                    raw["transactions"] = json!([swap()]);
                }
                raw
            };
            let body =
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
                    .unwrap();
            let status = if mode == "unavailable" {
                "503 Service Unavailable"
            } else {
                "200 OK"
            };
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let g = tokio::spawn(async move {
        let slots = if mode == "restart" {
            vec![FIRST + 2, FIRST + 3]
        } else if mode == "ack" {
            vec![FIRST - 1, FIRST, FIRST + 2]
        } else if mode == "repeat" {
            vec![FIRST, FIRST + 2, FIRST + 4, FIRST + 5]
        } else {
            vec![FIRST, FIRST + 2, FIRST + 3]
        };
        let messages = slots
            .into_iter()
            .map(|slot| {
                let mut block = complete(slot);
                if slot == FIRST + 2 {
                    if mode == "skipped" {
                        block.parent_slot = FIRST;
                        block.parent_blockhash = key(FIRST);
                    }
                    if mode == "live-hash" {
                        block.parent_slot = FIRST;
                        block.parent_blockhash = key(FIRST - 1);
                    }
                    if mode == "incomplete" {
                        block.executed_transaction_count = 1;
                    }
                }
                SubscribeUpdate {
                    update_oneof: Some(subscribe_update::UpdateOneof::Block(block)),
                    ..Default::default()
                }
            })
            .collect();
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live(Fixture {
                messages,
                terminal: None,
                reject_subscribe: None,
            })))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Servers {
        http,
        grpc,
        calls,
        tasks: vec![h, g],
    }
}
