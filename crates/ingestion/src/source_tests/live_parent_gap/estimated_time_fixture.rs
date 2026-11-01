//! LOCAL continuation: saved signed outer message + receipt, never a captured stream clock.
use super::*;
use copybot_core_types::{
    CopySignalRow, ExactSwapAmounts, Lamports, SignedLamports, TokenQuantity,
};
use copybot_storage_core::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(super) const SELL: &str =
    "4D3wSmgXJEgTC3XUxsS2Qy5P3WXZMR2qfed2T6pdTFKjDhGBgdVt3xE6imQhTxZDYqukXpEnYVmtwMC1NPTLihVv";
pub(super) const OWNED: u64 = 935_421;
pub(super) const PERSONAL: u64 = 100;
const SOL: &str = "So11111111111111111111111111111111111111112";

fn saved(name: &str, expected: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/http_estimated_block_time_pair03")
        .join(name);
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(&bytes)), expected);
    serde_json::from_slice(&bytes).unwrap()
}
fn base64(s: &str) -> Vec<u8> {
    let mut out = vec![];
    let (mut bits, mut count) = (0u32, 0u32);
    for c in s.bytes().take_while(|c| *c != b'=') {
        let n = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            .iter()
            .position(|x| *x == c)
            .expect("saved base64 alphabet") as u32;
        bits = (bits << 6) | n;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    out
}
struct Wire {
    bytes: Vec<u8>,
    at: usize,
}
impl Wire {
    fn take(&mut self, count: usize) -> Vec<u8> {
        let out = self.bytes[self.at..self.at + count].to_vec();
        self.at += count;
        out
    }
    fn byte(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn short(&mut self) -> usize {
        let mut n = 0usize;
        for shift in [0, 7, 14] {
            let b = self.byte();
            n |= usize::from(b & 127) << shift;
            if b & 128 == 0 {
                return n;
            }
        }
        panic!("invalid saved shortvec");
    }
    fn list(&mut self) -> Vec<u8> {
        let n = self.short();
        self.take(n)
    }
}
/// Outer instructions come from the saved signed wire. The receipt's only parsed
/// inner operations are SPL Transfer: their exact u64 operands compile losslessly.
/// This builds a test-only typed projection; no original gRPC Info/clock was saved.
pub(super) fn sell() -> SubscribeUpdateTransaction {
    let rpc = saved(
        "source_sell_rpc.json",
        "21d6cf37dc5ef8d06b127e837a600461cee6ece9ff2d453e7f2fe48c4c87f3d1",
    );
    let request = saved(
        "source_sell_send_request.json",
        "d9bcaefc323ddd2c84091f8bf5aaecd9b57ec5b13870be0ef862b8a9b5b6da67",
    );
    let mut w = Wire {
        bytes: base64(request["request"]["params"][0].as_str().unwrap()),
        at: 0,
    };
    assert_eq!(w.short(), 1);
    let signature = bs58::encode(w.take(64)).into_string();
    assert_eq!(signature, SELL);
    let header = [w.byte(), w.byte(), w.byte()];
    assert_eq!(header[0] & 128, 0, "this saved wire is legacy");
    let count = w.short();
    let keys: Vec<_> = (0..count)
        .map(|_| bs58::encode(w.take(32)).into_string())
        .collect();
    let previous = bs58::encode(w.take(32)).into_string();
    let count = w.short();
    let instructions: Vec<_> = (0..count).map(|_| {
        let program = w.byte(); let accounts = w.list(); let data = w.list();
        json!({"programIdIndex":program,"accounts":accounts,"data":bs58::encode(data).into_string()})
    }).collect();
    assert_eq!(
        w.at,
        w.bytes.len(),
        "all original signed message bytes consumed"
    );
    let mut result = rpc["result"].clone();
    assert_eq!(result["slot"], 464_224_269u64);
    assert_eq!(result["transactionIndex"], 38u64);
    let parsed_keys: Vec<_> = result["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["pubkey"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(keys, parsed_keys);
    assert_eq!(
        result["transaction"]["message"]["recentBlockhash"],
        previous
    );
    let index = |key: &Value| {
        keys.iter()
            .position(|k| Some(k.as_str()) == key.as_str())
            .unwrap()
    };
    for group in result["meta"]["innerInstructions"].as_array_mut().unwrap() {
        for ix in group["instructions"].as_array_mut().unwrap() {
            let program = index(&ix["programId"]);
            let (accounts, data) = if ix.get("parsed").is_some() {
                assert_eq!(ix["program"], "spl-token");
                assert_eq!(ix["parsed"]["type"], "transfer");
                let f = &ix["parsed"]["info"];
                let mut data = vec![3u8];
                data.extend(
                    f["amount"]
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
                        .to_le_bytes(),
                );
                (
                    vec![
                        index(&f["source"]),
                        index(&f["destination"]),
                        index(&f["authority"]),
                    ],
                    bs58::encode(data).into_string(),
                )
            } else {
                (
                    ix["accounts"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(index)
                        .collect(),
                    ix["data"].as_str().unwrap().to_owned(),
                )
            };
            *ix = json!({"programIdIndex":program,"accounts":accounts,"data":data,"stackHeight":ix["stackHeight"]});
        }
    }
    result["transaction"] = json!({"signatures":[signature],"message":{
        "header":{"numRequiredSignatures":header[0],"numReadonlySignedAccounts":header[1],"numReadonlyUnsignedAccounts":header[2]},
        "accountKeys":keys,"recentBlockhash":previous,"instructions":instructions}});
    result["version"] = json!("legacy");
    let raw = modeled(464_224_269, 464_224_115, &hash(211), &hash(212));
    let mut raw = raw;
    raw["blockTime"] = result["blockTime"].clone();
    raw["transactions"] = json!([result]);
    let mut block = normalize_confirmed_http_block(464_224_269, &raw).unwrap();
    let mut info = block.transactions.remove(0);
    info.index = 38;
    SubscribeUpdateTransaction {
        slot: block.slot,
        transaction: Some(info),
        ..Default::default()
    }
}

pub(super) async fn server(
    bodies: BTreeMap<u64, Vec<u8>>,
    messages: Vec<SubscribeUpdate>,
) -> Servers {
    let mut s = local_server(bodies, vec![]).await;
    s.tasks.pop().expect("owned empty gRPC server").abort();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    s.grpc = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    s.tasks.push(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live(Fixture {
                messages,
                terminal: None,
                reject_subscribe: None,
            })))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    }));
    s
}
pub(super) fn update(block: SubscribeUpdateBlock) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block)),
        ..Default::default()
    }
}
pub(super) fn continuation(
    tx: &SubscribeUpdateTransaction,
    parent: &SubscribeUpdateBlock,
) -> SubscribeUpdateBlock {
    let mut b = normalize_confirmed_http_block(
        tx.slot,
        &modeled(tx.slot, parent.slot, &parent.blockhash, &hash(213)),
    )
    .unwrap();
    let info = tx.transaction.as_ref().unwrap();
    b.executed_transaction_count = info.index + 1;
    // Explicit LOCAL fillers preserve the saved SELL execution index, never a claim
    // that the original intervening producer block or its other transactions survived.
    b.transactions = (0..info.index)
        .map(|index| SubscribeUpdateTransactionInfo {
            signature: vec![(index + 1) as u8; 64],
            index,
            is_vote: true,
            ..Default::default()
        })
        .chain(std::iter::once(info.clone()))
        .collect();
    b
}

pub(super) fn seed_owned(
    path: &std::path::Path,
    inbox: &mut AssociationInbox,
    sell: &AdmissionFacts,
    slot: u64,
    blockhash: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> CandidateGeneration {
    let store = SqliteStore::open(path).unwrap();
    let source_sig = bs58::encode([188; 64]).into_string();
    let receipt_sig = bs58::encode([189; 64]).into_string();
    let bot_wallet = bs58::encode([190; 32]).into_string();
    let mint = &sell.facts.token_in;
    let signal = format!("shadow:{source_sig}:{}:buy:{mint}", sell.facts.wallet);
    store
        .insert_copy_signal(&CopySignalRow {
            signal_id: signal.clone(),
            wallet_id: sell.facts.wallet.clone(),
            token: mint.clone(),
            side: "buy".into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "LOCAL-modeled-receipt".into(),
        })
        .unwrap();
    let order = store
        .reserve_execution_canary_order(&signal, "LOCAL-modeled-route", now)
        .unwrap()
        .order
        .order_id;
    store.mark_execution_canary_built(&order, now).unwrap();
    store
        .mark_execution_canary_simulated(&order, now, EXECUTION_SIMULATION_STATUS_PASSED, None)
        .unwrap();
    store
        .mark_execution_canary_submitted(&order, now, &receipt_sig)
        .unwrap();
    store
        .mark_execution_canary_confirmed_unreconciled(
            &order,
            &ExecutionCanaryReceiptProof {
                tx_signature: receipt_sig.clone(),
                wallet_pubkey: bot_wallet.clone(),
                token: mint.clone(),
                side: "buy".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(slot),
                confirmed_at: now,
                reason: "LOCAL-modeled-receipt".into(),
            },
            now,
        )
        .unwrap();
    store
        .record_execution_canary_receipt_facts(
            &ExecutionCanaryReceiptFacts {
                order_id: order.clone(),
                tx_signature: receipt_sig.clone(),
                wallet_pubkey: bot_wallet.clone(),
                token: mint.clone(),
                side: "buy".into(),
                slot,
                wallet_native_pre: Lamports::new(100_000_000),
                wallet_native_post: Lamports::new(90_000_000),
                wallet_native_delta: SignedLamports::new(-10_000_000),
                transaction_fee: Some(Lamports::new(5000)),
                fee_coverage: ReceiptFeeCoverage::Known,
                fee_payer: Some(bot_wallet.clone()),
                token_delta: Some(ReceiptTokenDelta {
                    raw: OWNED as i128,
                    decimals: 6,
                }),
                token_coverage: ReceiptTokenCoverage::PairedBalances,
                token_coverage_reason: None,
                wsol_coverage: ReceiptWsolCoverage::Unresolved,
                block_time: Some(now.timestamp()),
                decomposition: ReceiptDecomposition::Unresolved,
            },
            now,
        )
        .unwrap();
    store
        .confirm_execution_canary_buy_fill(
            &order,
            mint,
            OWNED as f64 / 1_000_000.,
            Some(TokenQuantity::new(OWNED, 6)),
            0.01,
            now,
            now,
            Some(Lamports::new(10_000_000)),
        )
        .unwrap();
    for (sequence, signature, wallet, amount) in [
        (0, source_sig, sell.facts.wallet.clone(), 935_344u64),
        (2, receipt_sig, bot_wallet, OWNED),
    ] {
        let mut a = sell.clone();
        a.facts.signature = signature.clone();
        a.facts.slot = slot;
        a.facts.wallet = wallet;
        a.facts.token_in = SOL.into();
        a.facts.token_out = mint.clone();
        a.facts.amount_in_bits = 0.01f64.to_bits();
        a.facts.amount_out_bits = (amount as f64 / 1_000_000.).to_bits();
        a.facts.exact_amounts = Some(ExactSwapAmounts {
            amount_in_raw: "10000000".into(),
            amount_in_decimals: 9,
            amount_out_raw: amount.to_string(),
            amount_out_decimals: 6,
        });
        a.info = InfoIdentity {
            encoded: format!("LOCAL-modeled-financial-anchor-{sequence}").into_bytes(),
            float_bits: vec![],
        };
        a.message_time = MessageTime::CreatedAt {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos(),
        };
        let d = |sequence, event| Delivery {
            session: "LOCAL-modeled-financial-anchors".into(),
            sequence,
            arrival_offset_ns: sequence,
            event,
        };
        inbox
            .persist_at(
                &d(sequence, DeliveryEvent::Admission(a.clone())),
                &CandidateGeneration::Unknown,
                now,
            )
            .unwrap();
        inbox
            .persist_at(
                &d(
                    sequence + 1,
                    DeliveryEvent::Terminal {
                        signature: signature.clone(),
                        expected: a,
                        result: Terminal::ProviderAsserted(ProviderAssertion {
                            signature,
                            slot,
                            blockhash: blockhash.into(),
                            transaction_index: sequence / 2,
                            block_time: BlockTime::Missing,
                        }),
                    },
                ),
                &CandidateGeneration::Unknown,
                now,
            )
            .unwrap();
    }
    store.association_candidate(&sell.facts)
}
