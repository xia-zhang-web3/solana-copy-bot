//! A previously committed truncated Info cannot be replaced by complete HTTP.
use super::anchor_complete_replay::{encoded, local_server, modeled, wallet};
use super::*;
use crate::source::{http_recovery::normalize_confirmed_http_block, tests::anchor_tail_pair::load};
use std::collections::BTreeMap;

#[tokio::test]
#[ignore = "pinned COPY09 pair; LOCAL durable variant, no historical fresh admission"]
async fn copy09_anchor_complete_old_truncated_durable_info_refuses_without_overwrite() {
    let pair = load();
    let anchor = pair.grpc.slot;
    let prior = pair.grpc.parent_slot;
    let tail = anchor + 1;
    let prior_raw = modeled(prior, prior - 1, &hash(201), &pair.grpc.parent_blockhash);
    let tail_raw = modeled(tail, anchor, &pair.grpc.blockhash, &hash(202));
    let mut truncated = pair.grpc.clone();
    let empty_server = local_server(BTreeMap::new(), vec![]).await;
    let initial = config(&empty_server);
    let selected_wallet = wallet(&pair.http, &initial);
    // Actual checked saved Info, with one explicit LOCAL closing-record truncation.
    // This does not alter the original payloads, decoded balances or transaction.
    let rays: HashSet<_> = initial.raydium_program_ids.iter().cloned().collect();
    let pumps: HashSet<_> = initial.pumpswap_program_ids.iter().cloned().collect();
    let interest = rays.union(&pumps).cloned().collect();
    let checked = truncated
        .transactions
        .iter_mut()
        .find(|info| {
            crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
                &SubscribeUpdateTransaction {
                    slot: anchor,
                    transaction: Some((*info).clone()),
                    ..Default::default()
                },
                &interest,
                &rays,
                &pumps,
            )
            .facts
            .ok()
            .flatten()
            .is_some_and(|f| f.signer == selected_wallet)
                && info.meta.as_ref().is_some_and(|m| {
                    m.log_messages
                        .last()
                        .is_some_and(|l| l.ends_with(" success"))
                })
        })
        .expect("actual checked selected saved transaction has terminal close");
    let signature = bs58::encode(&checked.signature).into_string();
    *checked
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .last_mut()
        .unwrap() = "Log truncated".into();
    let first = local_server(BTreeMap::new(), vec![truncated]).await;
    let mut c = config(&first);
    let b = c.yellowstone_association.as_mut().unwrap();
    b.input_bytes = 8 << 20;
    b.blocks.bytes = 32 << 20;
    b.history.bytes = 8 << 20;
    b.outputs.bytes = 8 << 20;
    b.queue.bytes = 8 << 20;
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    h.max_response_bytes = 8 << 20;
    h.range_slots = 4;
    h.timeout_ms = 10_000;
    let wallets = HashSet::from([selected_wallet]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("previous-truncated.sqlite");
    let (mut db, scope) = initialize_empty(&path, &c, &wallets);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-truncated-bootstrap".into(),
        wallets.clone(),
        None,
        None,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let e=r.next().await.unwrap().unwrap();
            persist(&mut db,&r,&e,&scope);
            if matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==anchor){break;}
        }
    }).await.unwrap();
    let original = db
        .identity(&signature)
        .unwrap()
        .expect("original real association was durably admitted");
    let saved = db.replay_checkpoint(&scope).unwrap().unwrap();
    let raw_head = || {
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap()
            .query_row(
                "SELECT head FROM association_replay_cursor WHERE id=1",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
    };
    let committed_head = raw_head();
    r.stop();
    drop(r);
    drop(first);
    drop(empty_server);
    let next = normalize_confirmed_http_block(tail, &tail_raw).unwrap();
    let server = local_server(
        BTreeMap::from([
            (prior, encoded(&prior_raw)),
            (anchor, pair.raw_http.clone()),
            (tail, encoded(&tail_raw)),
        ]),
        vec![next],
    )
    .await;
    c.yellowstone_grpc_url = server.grpc.clone();
    c.yellowstone_http_recovery.as_mut().unwrap().broker_url = server.http.clone();
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-truncated-restart".into(),
        wallets,
        None,
        Some(saved.clone()),
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    let mut duplicate = false;
    let failure = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match r.next().await {
                Ok(Some(e)) => {
                    match &e.delivery.event {
                        DeliveryEvent::Duplicate { original: o, .. } => {
                            assert_eq!(o.facts.signature, signature);
                            duplicate = true;
                        }
                        DeliveryEvent::Admission(_) | DeliveryEvent::ParentCheckpoint(_) => {
                            panic!("changed durable Info cannot advance")
                        }
                        _ => {}
                    }
                    db.persist(&e.delivery, &CandidateGeneration::Unknown)
                        .unwrap();
                }
                Err(e) => break format!("{e:#}"),
                Ok(None) => panic!("refusal was silent"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        failure.contains("replay_durable_info_conflict"),
        "{failure}"
    );
    assert!(duplicate);
    assert!(hold.load(Ordering::Acquire));
    assert_eq!(
        raw_head(),
        committed_head,
        "refused complete HTTP cannot commit a new head"
    );
    assert!(
        db.replay_checkpoint(&scope)
            .err()
            .unwrap()
            .to_string()
            .contains("association_replay_claim_changed"),
        "persisted Info conflict remains fail-closed for subsequent restart"
    );
    let kept = db.identity(&signature).unwrap().unwrap();
    assert_eq!(kept.admission, original.admission);
    assert_eq!(
        (&kept.first_session, kept.first_sequence),
        (&original.first_session, original.first_sequence)
    );
    assert!(
        kept.conflict,
        "conflict is recorded, never used to overwrite accepted facts"
    );
    r.stop();
    drop(r);
    drop(db);
    let sql = rusqlite::Connection::open(&path).unwrap();
    for table in ["orders", "fills", "positions", "copy_signals"] {
        assert_eq!(
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
    let proof = json!({"kind":"LOCAL_PREVIOUSLY_DURABLE_TRUNCATED_INFO_REFUSED","failure":failure,
        "actual_saved_checked_signature":signature,"last_committed_checkpoint":saved,
        "canonical_admission_unchanged":true,"committed_cursor_bytes_unchanged":true,"subsequent_replay_guard":"association_replay_claim_changed","conflict_recorded":true,"hold_remains":true,
        "new_checkpoint_commits":0,"financial_rows":0,"provider_requests":0,
        "LOCAL_variant":"one checked saved transaction terminal close replaced by bare truncation before bootstrap association; never a fresh historical signal"});
    if let Ok(path) = std::env::var("COPYBOT_ANCHOR_DURABLE_NEGATIVE_OUTPUT") {
        assert!(!std::path::Path::new(&path).exists());
        std::fs::write(path, serde_json::to_vec_pretty(&proof).unwrap()).unwrap();
    }
    eprintln!("COPY09_OLD_DURABLE_REFUSAL {proof}");
}
