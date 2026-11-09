use super::*;

#[test]
fn pending_member_waits_for_actual_new_scope_baseline_and_restart_preserves_old_head() {
    let mut f = Fixture::new();
    let request = f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    assert_eq!(
        f.db.query_row(
            "SELECT state FROM capture_requests WHERE id=?",
            [request],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "PENDING"
    );
    assert_eq!(f.store.prepare_scope().unwrap(), scope);
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(vec!["bob".into()], 7), 1.)
        .is_err());
    let head = Fixture::head(scope.wallets.clone(), 7);
    f.store.commit_scope(&scope, &head, 1.).unwrap();
    assert!(!f.store.needs_transition().unwrap());
    let updated = Fixture::head(scope.wallets.clone(), 8);
    f.store.record_checkpoint(scope.revision, &updated).unwrap();
    assert_eq!(
        f.store.active_scope().unwrap().unwrap().baseline,
        Some(updated.clone())
    );
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    restarted.start().unwrap();
    assert!(restarted.needs_transition().unwrap());
    let next = restarted.prepare_scope().unwrap();
    assert!(next.revision > scope.revision);
    assert!(next.baseline.is_none());
    assert_eq!(restarted.latest_checkpoint().unwrap(), Some(updated));
}

#[test]
fn demotion_retains_unsettled_pin_in_frozen_union_and_rejects_scope_omission() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    let seq = f
        .store
        .receive(Some("buy"), "alice", 7, b"buy", "fingerprint", 2., Some(2.))
        .unwrap()
        .unwrap()
        .seq;
    f.store.finish(seq, None, "fixture pin owner").unwrap();
    f.bind_stream(scope.revision, seq, "buy", b"buy");
    f.db.execute(
        "INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(?,'alice','mint')",
        [seq],
    )
    .unwrap();
    f.request("bob");
    let new_scope = f.store.prepare_scope().unwrap();
    assert_eq!(new_scope.wallets, vec!["alice", "bob"]);
    f.store
        .commit_scope(&new_scope, &Fixture::head(new_scope.wallets.clone(), 8), 3.)
        .unwrap();
    assert_eq!(f.store.desired_wallets().unwrap(), vec!["alice", "bob"]);
    f.db.execute("UPDATE capture_obligations SET state='SETTLED'", [])
        .unwrap();
    assert!(f.store.needs_transition().unwrap());
    assert_eq!(f.store.prepare_scope().unwrap().wallets, vec!["bob"]);
}

#[test]
fn crash_after_receipt_before_checkpoint_resumes_old_plan_without_old_member_ack() {
    let mut f = Fixture::new();
    let request = f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let evidence = f.evidence();
    let observation = f.observation(&evidence);
    f.store.commit_evidence(scope.revision, &evidence).unwrap();
    let first = f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .unwrap();
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    restarted.start().unwrap();
    assert!(restarted.prepare_scope().is_err()); // Outbox must actually drain before transition.
    f.finish_no_swap_outbox();
    assert!(restarted.verify_forward_dispositions().unwrap());
    let stale = restarted.prepare_scope().unwrap();
    assert_eq!(stale, scope);
    assert_eq!(
        restarted
            .commit_observation(scope.revision, &observation, None, |a, b| a == b)
            .unwrap()
            .id,
        first.id
    );
    assert!(restarted
        .commit_scope(&stale, &Fixture::head(stale.wallets.clone(), 7), 2.)
        .is_err());
    restarted
        .recover_planned(&stale, &Fixture::head(stale.wallets.clone(), 7))
        .unwrap();
    assert_eq!(
        f.db.query_row(
            "SELECT state FROM capture_requests WHERE id=?",
            [request],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "PENDING"
    );
    let next = restarted.prepare_scope().unwrap();
    assert!(next.revision > stale.revision);
    assert!(next.baseline.is_none());
}

#[test]
fn raw_finish_before_identity_crash_restores_retired_scope_without_recovery_disposition() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    let mut evidence = f.evidence();
    evidence.source_kind = ProtectedSourceKind::Stream;
    f.store.commit_evidence(scope.revision, &evidence).unwrap();
    let mut observation = f.observation(&evidence);
    observation.message_time = MessageTime::CreatedAt {
        seconds: 2,
        nanos: 0,
    };
    observation.block = None;
    observation.transaction_fingerprint = "fingerprint".into();
    let seq = f
        .store
        .receive(
            Some("signature"),
            "alice",
            7,
            &evidence.raw,
            "fingerprint",
            2.,
            Some(2.),
        )
        .unwrap()
        .unwrap()
        .seq;
    f.store
        .finish(seq, None, "controlled undecodable stream")
        .unwrap();
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    restarted.start().unwrap();
    let unbound = restarted.unbound_stream_receipts(128).unwrap();
    assert_eq!(unbound.len(), 1);
    assert_eq!(unbound[0].seq, seq);
    assert_eq!(unbound[0].raw, evidence.raw);
    let next = restarted.prepare_scope().unwrap();
    assert!(restarted
        .commit_scope(&next, &Fixture::head(next.wallets.clone(), 8), 3.)
        .is_err());
    assert_eq!(restarted.stream_receipt_scope(seq).unwrap(), scope.revision);
    restarted
        .commit_evidence(scope.revision, &evidence)
        .unwrap();
    let restored = restarted
        .commit_observation(scope.revision, &observation, Some(seq), |a, b| a == b)
        .unwrap();
    restarted
        .commit_scope(&next, &Fixture::head(next.wallets.clone(), 8), 3.)
        .unwrap();
    assert_eq!(
        restarted
            .commit_observation(scope.revision, &observation, Some(seq), |a, b| a == b)
            .unwrap()
            .id,
        restored.id
    );
    assert!(!restored.recovery_first);
    assert_eq!(restored.live_seq, Some(seq));
    assert!(!restarted.recovery_outbox_pending().unwrap());
    assert!(restarted.unbound_stream_receipts(128).unwrap().is_empty());
    let mut recovered_evidence = f.evidence();
    // Distinct source representations cannot share invented identical raw bytes.
    recovered_evidence.raw =
        b"complete block representation after original stream receipt".to_vec();
    recovered_evidence.sha256 = format!("{:x}", Sha256::digest(&recovered_evidence.raw));
    recovered_evidence.available_at_micros = 4000000;
    restarted
        .commit_evidence(next.revision, &recovered_evidence)
        .unwrap();
    let recovered = f.observation(&recovered_evidence);
    assert_eq!(
        restarted
            .commit_observation(next.revision, &recovered, None, |a, b| a == b)
            .unwrap()
            .id,
        restored.id
    );
    assert!(!restarted.recovery_outbox_pending().unwrap());
}

#[test]
fn transport_gap_revokes_ack_until_new_revision_and_verified_baseline() {
    let mut f = Fixture::new();
    let request = f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    let old_gap: i64 =
        f.db.query_row(
            "SELECT gap FROM capture_requests WHERE id=?",
            [request],
            |r| r.get(0),
        )
        .unwrap();
    f.store.gap("transport gap", false).unwrap();
    assert!(f.store.needs_transition().unwrap());
    let gap: i64 =
        f.db.query_row("SELECT gap FROM capture_meta WHERE id=1", [], |r| r.get(0))
            .unwrap();
    assert!(gap > old_gap);
    f.store
        .record_checkpoint(scope.revision, &Fixture::head(scope.wallets.clone(), 8))
        .unwrap();
    assert!(f.store.needs_transition().unwrap());
    let next = f.store.prepare_scope().unwrap();
    assert!(next.revision > scope.revision);
    assert!(next.ack_gap.is_none());
    let still_old: i64 =
        f.db.query_row(
            "SELECT gap FROM capture_requests WHERE id=?",
            [request],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(still_old, old_gap);
    f.store
        .commit_scope(&next, &Fixture::head(next.wallets.clone(), 9), 2.)
        .unwrap();
    assert!(!f.store.needs_transition().unwrap());
}

#[test]
fn scope_snapshot_fences_old_admission_before_pin_union_is_read() {
    let mut f = Fixture::new();
    let request = f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    let seq = f
        .store
        .receive(Some("buy"), "alice", 7, b"buy", "fingerprint", 2., Some(2.))
        .unwrap()
        .unwrap()
        .seq;
    f.store.finish(seq, None, "fixture pin owner").unwrap();
    f.bind_stream(scope.revision, seq, "buy", b"buy");
    f.db.execute(
        "INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(?,'alice','mint')",
        [seq],
    )
    .unwrap();
    f.request("bob");
    let next = f.store.prepare_scope().unwrap();
    assert_eq!(next.wallets, vec!["alice", "bob"]);
    let (old_ack, current): (i64, i64) =
        f.db.query_row(
            "SELECT r.gap,m.gap FROM capture_requests r CROSS JOIN capture_meta m WHERE r.id=?",
            [request],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(current > old_ack);
    let repeat = f.store.prepare_scope().unwrap();
    assert_eq!(repeat, next);
    assert_eq!(
        f.db.query_row("SELECT gap FROM capture_meta", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        current
    );
}
