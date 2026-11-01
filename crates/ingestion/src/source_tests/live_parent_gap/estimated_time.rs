//! Actual pair03 gap, durable ACK, then an explicitly LOCAL saved-SELL continuation.
use super::super::anchor_complete_replay::{encoded, local_server, modeled};
use super::*;
use crate::source::{
    http_recovery::{
        estimated_block_time_tests::{pair03, pair03_http_body},
        normalize_confirmed_http_block,
    },
    tests::anchor_tail_consumers::policy,
    yellowstone_facts::decode_yellowstone_swap_facts,
};
use copybot_storage_core::{
    association_sell_preparation::{Check, MessageClockCheck},
    native_buy::{NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY},
    ordered_source_sell::{OrderedSellDecision, OrderedSellStage, PROVIDER_ORDER_STRICT_V1},
};
use prost::Message;
use std::collections::BTreeMap;
#[path = "estimated_time_fixture.rs"]
mod f;

fn info_identity(info: &SubscribeUpdateTransactionInfo) -> InfoIdentity {
    InfoIdentity {
        encoded: info.encode_to_vec(),
        float_bits: info
            .meta
            .iter()
            .flat_map(|m| m.pre_token_balances.iter().chain(&m.post_token_balances))
            .map(|r| r.ui_token_amount.as_ref().map(|a| a.ui_amount.to_bits()))
            .collect(),
    }
}
fn checked(tx: &SubscribeUpdateTransaction) -> AdmissionFacts {
    let (interest, rays, pumps) = policy();
    let decoded = decode_yellowstone_swap_facts(tx, &interest, &rays, &pumps);
    let f = decoded
        .facts
        .unwrap()
        .expect("saved transaction has checked native swap facts");
    let mut programs: Vec<_> = f.program_ids.into_iter().collect();
    programs.sort();
    AdmissionFacts {
        facts: CheckedFacts {
            signature: f.signature,
            slot: f.slot,
            wallet: f.signer,
            token_in: f.token_in,
            token_out: f.token_out,
            amount_in_bits: f.amount_in.to_bits(),
            amount_out_bits: f.amount_out.to_bits(),
            exact_amounts: f.exact_amounts,
            programs,
            dex: f.dex_hint,
            program_fallback: decoded.used_program_fallback,
        },
        info: info_identity(tx.transaction.as_ref().unwrap()),
        message_time: MessageTime::Missing,
    }
}
fn settings(server: &Servers) -> IngestionConfig {
    let mut c = config(server);
    let (interest, rays, pumps) = policy();
    c.yellowstone_program_ids = interest.iter().cloned().collect();
    c.subscribe_program_ids = interest.into_iter().collect();
    c.raydium_program_ids = rays.into_iter().collect();
    c.pumpswap_program_ids = pumps.into_iter().collect();
    let b = c.yellowstone_association.as_mut().unwrap();
    b.input_bytes = 8 << 20;
    b.blocks.bytes = 32 << 20;
    b.history.bytes = 8 << 20;
    b.outputs.bytes = 8 << 20;
    b.queue.bytes = 8 << 20;
    b.metadata_bytes = 16 << 20;
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    h.max_response_bytes = 8 << 20;
    h.range_slots = 4;
    h.timeout_ms = 10_000;
    c
}
fn count(path: &std::path::Path, table: &str) -> u64 {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn actual_pair03_gap_ack_then_local_native_source_sell_stages_once() {
    let (grpc, http) = pair03();
    assert_eq!(grpc.slot, 464_224_115);
    assert_ne!(
        grpc.block_time, http.block_time,
        "actual incident requires estimated metadata difference"
    );
    let anchor = grpc.slot;
    let first_slot = grpc.parent_slot - 1;
    let financial_slot = first_slot - 2;
    let financial_hash = hash(208);
    let first_raw = modeled(first_slot, first_slot - 1, &hash(210), &hash(209));
    let overlap_raw = modeled(first_slot - 1, financial_slot, &financial_hash, &hash(210));
    let missing_raw = modeled(
        grpc.parent_slot,
        first_slot,
        &hash(209),
        &grpc.parent_blockhash,
    );
    let first = normalize_confirmed_http_block(first_slot, &first_raw).unwrap();
    let tx = f::sell();
    let mut sell = checked(&tx);
    assert_eq!(sell.facts.signature, f::SELL);
    assert_eq!(
        sell.facts.exact_amounts.as_ref().unwrap().amount_in_raw,
        "935344"
    );
    assert_eq!(
        sell.facts.exact_amounts.as_ref().unwrap().amount_out_raw,
        "9949742"
    );
    assert_eq!(
        sell.facts.token_out,
        "So11111111111111111111111111111111111111112"
    );
    let now = chrono::Utc::now();
    let local_time = MessageTime::CreatedAt {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos(),
    };
    sell.message_time = local_time.clone();
    let live_sell = SubscribeUpdate {
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(tx.clone())),
        ..Default::default()
    };
    let continuation = f::continuation(&tx, &grpc);
    let server = f::server(
        BTreeMap::from([
            (first_slot - 1, encoded(&overlap_raw)),
            (first_slot, encoded(&first_raw)),
            (grpc.parent_slot, encoded(&missing_raw)),
            (anchor, pair03_http_body()),
        ]),
        vec![
            f::update(first),
            f::update(grpc.clone()),
            live_sell.clone(),
            live_sell,
            f::update(continuation),
        ],
    )
    .await;
    let c = settings(&server);
    let (interest, rays, pumps) = policy();
    let historical_buy = grpc
        .transactions
        .iter()
        .find_map(|info| {
            let decoded = decode_yellowstone_swap_facts(
                &SubscribeUpdateTransaction {
                    slot: anchor,
                    transaction: Some(info.clone()),
                    ..Default::default()
                },
                &interest,
                &rays,
                &pumps,
            );
            decoded.facts.ok().flatten().filter(|f| {
                f.token_in == "So11111111111111111111111111111111111111112"
                    && f.exact_amounts.is_some()
                    && !decoded.used_program_fallback
            })
        })
        .expect("actual pair03 contains a checked native BUY for the recovered-origin refusal");
    let historical_signature = historical_buy.signature.clone();
    let wallets = HashSet::from([sell.facts.wallet.clone(), historical_buy.signer.clone()]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pair03-gap-sell.sqlite");
    let (mut db, scope) = initialize_empty(&path, &c, &wallets);
    let candidate = f::seed_owned(
        &path,
        &mut db,
        &sell,
        financial_slot,
        &financial_hash,
        now - chrono::Duration::seconds(5),
    );
    assert!(matches!(candidate, CandidateGeneration::AppObserved { .. }));
    db.register_technical_cohort_authority(&TechnicalCohortAuthority {
        run_id: "LOCAL-pair03-recovered-refusal".into(),
        wallet_ids: vec![historical_buy.signer.clone()],
        mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
        activated_at: now - chrono::Duration::seconds(10),
        deadline: now + chrono::Duration::seconds(90),
        max_buy_count: 1,
        policy_identity: "LOCAL-pair03-policy".into(),
    })
    .unwrap();
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-pair03-gap-sell".into(),
        wallets,
        None,
        None,
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    let store = SqliteStore::open(&path).unwrap();
    let mut checkpoints = vec![];
    let mut historical = vec![];
    let mut anchor_ack = None;
    let mut sell_envelope = None;
    let mut released_reader = None;
    tokio::time::timeout(Duration::from_secs(25),async {
        loop {
            let e = r.next().await.unwrap().expect("LOCAL stream continues through SELL checkpoint");
            if matches!(&e.delivery.event,DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) {
                db.record_native_buy_fence_epoch(&NativeBuyFence { session:e.delivery.session.clone(),processed_slot:first_slot-1,
                    sampled_at:now,genesis_hash:"LOCAL-no-network-genesis".into(),policy_identity:"LOCAL-pair03-policy".into() }).unwrap();
            }
            let current_candidate = match &e.delivery.event {
                DeliveryEvent::Admission(a) => store.association_candidate(&a.facts),
                _ => CandidateGeneration::Unknown,
            };
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                if a.facts.slot == anchor {
                    assert_eq!(a.message_time,MessageTime::RecoveredBlock { block_time:grpc.block_time.as_ref().map(|t|t.timestamp) },
                        "current anchor keeps original LIVE metadata but remains recovered provenance");
                    let original = grpc.transactions.iter().find(|i|bs58::encode(&i.signature).into_string()==a.facts.signature).unwrap();
                    assert_eq!(a.info,info_identity(original));
                    historical.push(a.clone());
                } else {
                    assert_eq!(a,&sell,"same saved SELL projection and explicitly LOCAL CreatedAt");
                    assert!(anchor_ack.is_some(),"SELL cannot cross the unacknowledged gap anchor");
                    assert!(!hold.load(Ordering::Acquire));
                    assert_eq!(r.continuity_snapshot().context.active_reader,released_reader);
                    assert_eq!(current_candidate,candidate);
                    sell_envelope = Some(e.delivery.clone());
                }
            }
            if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event {
                let slot = p.observation.child.slot;
                checkpoints.push(slot);
                if slot == anchor {
                    assert!(hold.load(Ordering::Acquire));
                    assert_eq!(db.replay_checkpoint(&scope).unwrap().unwrap().block.observation.child.slot,grpc.parent_slot);
                    db.persist_at(&e.delivery,&current_candidate,now+chrono::Duration::seconds(1)).unwrap();
                    let committed = db.replay_checkpoint(&scope).unwrap().unwrap();
                    assert_eq!(committed.block.observation.child.hash,grpc.blockhash);
                    assert!(hold.load(Ordering::Acquire),"commit without durable ACK keeps financial hold");
                    assert_eq!(count(&path,"native_buy_decisions"),0,"recovered BUY cannot consume a registered/fenced one-shot cohort");
                    assert_eq!(count(&path,"native_buy_cohort_decisions"),0);
                    r.acknowledge_checkpoint(committed.clone()).unwrap(); r.acknowledge_parent(anchor);
                    tokio::time::timeout(Duration::from_secs(3),async { while hold.load(Ordering::Acquire) { tokio::task::yield_now().await; } }).await.unwrap();
                    released_reader = r.continuity_snapshot().context.active_reader;
                    assert!(released_reader.is_some()); anchor_ack = Some(committed);
                    continue;
                }
                db.persist_at(&e.delivery,&current_candidate,now+chrono::Duration::seconds(1)).unwrap();
                r.acknowledge_checkpoint(db.replay_checkpoint(&scope).unwrap().unwrap()).unwrap(); r.acknowledge_parent(slot);
                if slot == tx.slot { break; }
            } else {
                db.persist_at(&e.delivery,&current_candidate,now+chrono::Duration::seconds(1)).unwrap();
            }
        }
    }).await.unwrap();
    assert_eq!(
        checkpoints,
        vec![first_slot, first_slot, grpc.parent_slot, anchor, tx.slot]
    );
    assert!(historical
        .iter()
        .any(|a| a.facts.signature == historical_signature));
    assert!(server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|q| q["method"] == "getBlock" && q["params"][0] == anchor));
    let before = db.identity(f::SELL).unwrap().unwrap();
    let preparation = db.sell_preparation(f::SELL).unwrap().unwrap();
    assert_eq!(
        preparation.first.message_clock_check,
        MessageClockCheck::NotFutureVsAppDequeue
    );
    assert_eq!(preparation.first.sell.admission.message_time, local_time);
    assert_eq!(
        preparation.current.selected_chain,
        Check::ProviderOrderedAcrossBlocks
    );
    assert_eq!(preparation.first.contributors.len(), 1);
    assert_eq!(preparation.first.contributors[0].raw, f::OWNED.to_string());
    assert_eq!(preparation.first.contributors[0].decimals, 6);
    assert_eq!(preparation.current.trade_authority, "trade_authority_none");
    let OrderedSellStage::Inserted(intent) = db
        .stage_ordered_source_sell_intent(f::SELL, PROVIDER_ORDER_STRICT_V1)
        .unwrap()
    else {
        panic!("same-gap downstream native SELL consumer must stage once");
    };
    assert_eq!(intent.first.sell.admission, sell);
    assert_eq!(intent.first.contributors[0].raw, "935421");
    assert_ne!(
        intent.first.contributors[0].raw,
        (f::OWNED + f::PERSONAL).to_string(),
        "personal baseline is outside the modeled receipt-backed lot"
    );
    assert_eq!(
        db.stage_ordered_source_sell_intent(f::SELL, PROVIDER_ORDER_STRICT_V1)
            .unwrap(),
        OrderedSellStage::Existing(intent.clone())
    );
    let saved_head = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(saved_head.block.observation.child.slot, tx.slot);
    r.stop();
    assert!(hold.load(Ordering::Acquire));
    drop(r);
    drop(db);
    let mut reopened = inbox(&path);
    reopened
        .persist_at(
            &sell_envelope.expect("SELL Admission was delivered"),
            &CandidateGeneration::Unknown,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
    assert_eq!(
        reopened.identity(f::SELL).unwrap().unwrap(),
        before,
        "reopen/duplicate cannot retag a LOCAL clock or bind a new generation"
    );
    assert_eq!(
        reopened
            .stage_ordered_source_sell_intent(f::SELL, PROVIDER_ORDER_STRICT_V1)
            .unwrap(),
        OrderedSellStage::Existing(intent.clone())
    );
    assert_eq!(
        reopened
            .revalidate_ordered_source_sell_intent(&intent.intent_id)
            .unwrap(),
        OrderedSellDecision::ValidatedNow
    );
    assert_eq!(count(&path, "ordered_source_sell_intents"), 1);
    assert_eq!(
        count(&path, "orders"),
        1,
        "only the explicit LOCAL modeled BUY order exists"
    );
    assert_eq!(
        count(&path, "copy_signals"),
        1,
        "timeless intent creates no runnable SELL signal"
    );
    for table in [
        "execution_quote_canary_events",
        "execution_quote_canary_provider_samples",
        "execution_source_sell_promotions",
    ] {
        assert_eq!(count(&path, table), 0);
    }
    let proof = json!({"kind":"ACTUAL_PAIR03_GAP_ACK_THEN_LOCAL_SAVED_SOURCE_SELL_INTENT",
        "anchor":anchor,"live_block_time":grpc.block_time.as_ref().unwrap().timestamp,"http_block_time":http.block_time.as_ref().unwrap().timestamp,
        "historical_admissions":historical.len(),"recovered_native_buy_refused":historical_signature,"anchor_ACK":anchor_ack,
        "checkpoints":checkpoints,"same_active_reader_after_ACK":true,"source_sell_signature":f::SELL,
        "source_sell_raw":935344,"modeled_owned_BOT_receipt_raw":f::OWNED,"personal_baseline_excluded_raw":f::PERSONAL,
        "LOCAL_created_at":local_time,"original_SELL_stream_created_at_captured":false,
        "LOCAL_parent_and_other_block_transactions":true,"saved_outer_signed_wire_and_receipt_projected_to_typed_Info":true,
        "ordered_intents":1,"reopen_idempotent":true,"new_SELL_signals":0,"quotes":0,"sends":0,
        "historical_SELL_stream_identity_or_freshness_proven":false,"financial_SELL_ready_proven":false,"trade_authority":"trade_authority_none"});
    if let Ok(path) = std::env::var("COPYBOT_PAIR03_GAP_SELL_OUTPUT") {
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        use std::io::Write;
        serde_json::to_writer_pretty(&mut out, &proof).unwrap();
        out.write_all(b"\n").unwrap();
        out.sync_all().unwrap();
    }
    eprintln!("PAIR03_GAP_NATIVE_SELL {proof}");
}
