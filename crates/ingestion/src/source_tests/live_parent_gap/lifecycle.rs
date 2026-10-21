//! Changes to ACK/STOP/restart around the new recovery edge, no financial work.
use super::*;

#[tokio::test]
async fn typed_live_parent_gap_delayed_ack_precedes_http_and_release() {
    let server = fixture::start("ack").await;
    let c = settings(&server);
    let wallets = fixture::wallets();
    let tmp = tempfile::tempdir().unwrap();
    let (mut db, scope) = initialize_empty(&tmp.path().join("delay.sqlite"), &c, &wallets);
    let mut r =
        DeliveryReceiver::start_recovering_labeled(&c, "delayed-ack".into(), wallets, None, None)
            .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let e = r.next().await.unwrap().unwrap();
            if e.delivery.session.ends_with(":0") && matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST) {
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert!(r.http_continuity_hold().unwrap().load(Ordering::Acquire));
                assert!(server.calls.lock().unwrap().is_empty(), "received573 cannot start HTTP before ACK");
                assert_eq!(r.ingress_snapshot().last_durably_stored_parent_slot, fixture::FIRST-1);
            }
            persist(&mut db, &r, &e, &scope);
            if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST+2) { break; }
        }
        while r.http_continuity_hold().unwrap().load(Ordering::Acquire) { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert!(
        r.ingress_snapshot()
            .processing
            .http_recovery
            .caught_up_to_anchor
    );
    r.stop();
}

#[test]
fn typed_live_parent_gap_capture_refresh_keeps_changed_durable_info_bounded() {
    use crate::source::durable::capture_scope::CaptureScope;
    let wallets: Vec<_> = fixture::wallets().into_iter().collect();
    let scope = CaptureScope::new(&wallets, std::iter::empty(), HashSet::new(), None);
    let mut info = fixture::complete(fixture::FIRST + 1).transactions.remove(0);
    info.transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .account_keys[0] = vec![88; 32];
    assert!(
        !scope.keep(&info),
        "unrelated signer before becoming durable"
    );
    scope.replace_durable(std::iter::once(info.signature.clone()));
    scope.replace_known(HashSet::new()); // association reset clears known keys
    assert!(
        scope.keep(&info),
        "changed signer must reach replay Info verification"
    );
    scope.replace_durable(std::iter::empty());
    assert!(
        !scope.keep(&info),
        "replace bounded overlap rather than accumulating history"
    );
}

#[tokio::test]
async fn typed_live_parent_gap_stop_then_restart_uses_only_committed_checkpoint() {
    let server = fixture::start("valid").await;
    let c = settings(&server);
    let wallets = fixture::wallets();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("restart.sqlite");
    let (mut db, scope) = initialize_empty(&path, &c, &wallets);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "before-stop".into(),
        wallets.clone(),
        None,
        None,
    )
    .unwrap();
    let mut admissions = 0;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let e = r.next().await.unwrap().unwrap();
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                assert!(matches!(a.message_time, MessageTime::RecoveredBlock { .. }));
                admissions += 1;
            }
            persist(&mut db, &r, &e, &scope);
            if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST+1) { break; }
        }
    }).await.unwrap();
    assert_eq!(admissions, 1);
    let first_identity = identity_snapshot(&path);
    r.stop();
    assert!(r.http_continuity_hold().unwrap().load(Ordering::Acquire));
    drop(r);
    let head = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(head.block.observation.child.slot, fixture::FIRST + 1);
    let restart = fixture::start("restart").await;
    let c = settings(&restart);
    let mut replayed_envelopes = 0;
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "after-stop".into(),
        wallets,
        None,
        Some(head),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let e = r.next().await.unwrap().unwrap();
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                assert!(matches!(a.message_time, MessageTime::RecoveredBlock { .. }));
                replayed_envelopes += 1;
            }
            persist(&mut db, &r, &e, &scope);
            if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST+3) { break; }
        }
    }).await.unwrap();
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        fixture::FIRST + 3
    );
    assert_eq!(
        replayed_envelopes, 1,
        "accepted overlap is an explicit replay envelope"
    );
    assert_eq!(
        first_identity,
        identity_snapshot(&path),
        "immutable first admission survives restart and replay"
    );
    let sql = rusqlite::Connection::open(path).unwrap();
    for table in ["orders", "fills", "positions"] {
        let count: u64 = sql
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "no financial/UNKNOWN mutation: {table}");
    }
    r.stop();
}

#[tokio::test]
async fn typed_live_parent_gap_stop_cancels_pending_http_and_keeps_hold() {
    let server = fixture::start("stall").await;
    let c = settings(&server);
    let wallets = fixture::wallets();
    let tmp = tempfile::tempdir().unwrap();
    let (mut db, scope) = initialize_empty(&tmp.path().join("stop.sqlite"), &c, &wallets);
    let mut r =
        DeliveryReceiver::start_recovering_labeled(&c, "stop-http".into(), wallets, None, None)
            .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            tokio::select! {
                e = r.next() => { persist(&mut db, &r, &e.unwrap().unwrap(), &scope); }
                _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    if server.calls.lock().unwrap().iter().any(|v| v["method"] == "getBlock" && v["params"][0] == fixture::FIRST+1) { break; }
                }
            }
        }
    }).await.unwrap();
    r.stop();
    assert!(r.http_continuity_hold().unwrap().load(Ordering::Acquire));
    assert!(r.next().await.unwrap().is_none());
    let calls = server.calls.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.calls.lock().unwrap().len(), calls);
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        fixture::FIRST
    );
}

#[test]
fn typed_live_parent_gap_is_not_overlap_or_same_parent_hash_corruption() {
    use crate::source::durable::recovery::{LiveParentGap, RecoveryGate};
    let block = fixture::complete(fixture::FIRST);
    let observation = copybot_core_types::association_parent::ParentObservation {
        child: copybot_core_types::association_parent::BlockKey {
            slot: block.slot,
            hash: block.blockhash.clone(),
        },
        parent: copybot_core_types::association_parent::BlockKey {
            slot: block.parent_slot,
            hash: block.parent_blockhash.clone(),
        },
        issue: None,
    };
    let scope = ReplayScope {
        policy: "durable_checkpoint_replay_v1".into(),
        wallets: fixture::wallets().into_iter().collect(),
        programs: vec![],
        raydium_programs: vec![],
        pumpswap_programs: vec![],
    };
    let head = DurableCheckpoint {
        session: "synthetic-durable".into(),
        sequence: 0,
        from_slot: fixture::FIRST - 1,
        block: BlockCheckpoint {
            scope,
            observation,
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        },
        overlap: vec![],
    };
    let mut overlap = RecoveryGate::new(Some(head), 16);
    assert!(!overlap
        .block(&fixture::complete(fixture::FIRST - 1))
        .unwrap());
    let error = overlap
        .block(&fixture::complete(fixture::FIRST + 2))
        .unwrap_err();
    assert!(error.downcast_ref::<LiveParentGap>().is_none());
    assert!(!overlap.ready());
    let mut live = RecoveryGate::new(None, 16);
    live.block(&block).unwrap();
    let gap = live
        .block(&fixture::complete(fixture::FIRST + 2))
        .unwrap_err();
    assert!(gap.downcast_ref::<LiveParentGap>().is_some());
    let mut corruption = fixture::complete(fixture::FIRST + 2);
    corruption.parent_slot = fixture::FIRST;
    corruption.parent_blockhash = fixture::key(fixture::FIRST - 1);
    let error = live.block(&corruption).unwrap_err();
    assert!(error.downcast_ref::<LiveParentGap>().is_none());
    // Gate(None) alone does not authorize recovery: restart_live_gap also needs
    // the exact previous child from a validated durable cursor, never receipt.
}
