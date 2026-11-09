use super::*;

#[test]
fn receipt_outbox_commit_before_ack_and_conflicts_do_not_upgrade_identity() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let evidence = f.evidence();
    let observation = f.observation(&evidence);
    assert!(f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .is_err());
    f.store.commit_evidence(scope.revision, &evidence).unwrap();
    assert_eq!(f.store.evidence(&evidence.sha256).unwrap(), evidence);
    let first = f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .unwrap();
    assert!(first.recovery_first);
    assert!(f.store.recovery_outbox_pending().unwrap());
    let duplicate = f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .unwrap();
    assert_eq!(duplicate.id, first.id);
    assert_eq!(
        f.db.query_row("SELECT count(*) FROM capture_recovery_outbox", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let mut conflict = observation.clone();
    conflict.info.encoded.push(4);
    assert!(f
        .store
        .commit_observation(scope.revision, &conflict, None, |a, b| a == b)
        .is_err());
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .is_err());
    f.finish_no_swap_outbox();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
}

#[test]
fn recovery_first_then_original_stream_keeps_original_missed_outbox() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let evidence = f.evidence();
    let observation = f.observation(&evidence);
    f.store.commit_evidence(scope.revision, &evidence).unwrap();
    let recovered = f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .unwrap();
    let mut original = evidence.clone();
    original.source_kind = ProtectedSourceKind::Stream;
    original.raw = b"original-stream-update".to_vec();
    original.sha256 = format!("{:x}", Sha256::digest(&original.raw));
    f.store.commit_evidence(scope.revision, &original).unwrap();
    let mut stream = f.observation(&original);
    stream.message_time = MessageTime::CreatedAt {
        seconds: 2,
        nanos: 0,
    };
    stream.block = None;
    let duplicate = f
        .store
        .commit_observation(scope.revision, &stream, None, |a, b| a == b)
        .unwrap();
    assert_eq!(duplicate.id, recovered.id);
    assert!(duplicate.recovery_first);
    assert!(duplicate.live_seq.is_none());
    assert_eq!(
        f.db.query_row("SELECT count(*) FROM capture_recovery_outbox", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn anchor_raw_pair_commits_without_rewriting_first_source_availability() {
    use copybot_core_types::association_delivery::ProtectedAnchorLink;
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let live = f.evidence();
    f.store.commit_evidence(scope.revision, &live).unwrap();
    let mut http = live.clone();
    http.source_kind = ProtectedSourceKind::ConfirmedHttp;
    http.raw = b"exact rpc response fixture".to_vec();
    http.sha256 = format!("{:x}", Sha256::digest(&http.raw));
    http.available_at_micros = 2000000;
    let mut linked = live.clone();
    linked.anchor = Some(ProtectedAnchorLink {
        other_evidence_sha256: http.sha256.clone(),
        comparison: "ESTIMATED_BLOCK_TIME_METADATA".into(),
        selected_source: ProtectedSourceKind::CompleteLiveBlock,
    });
    f.store.commit_evidence(scope.revision, &linked).unwrap();
    assert_eq!(f.store.evidence(&live.sha256).unwrap(), live);
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 3.)
        .is_err());
    let obs = f.observation(&live);
    assert!(f
        .store
        .commit_observation(scope.revision, &obs, None, |a, b| a == b)
        .is_err());
    f.store.commit_evidence(scope.revision, &http).unwrap();
    let receipt = f
        .store
        .commit_observation(scope.revision, &obs, None, |a, b| a == b)
        .unwrap();
    let proof: String =
        f.db.query_row(
            "SELECT provenance_json FROM capture_recovery WHERE id=?",
            [receipt.id],
            |r| r.get(0),
        )
        .unwrap();
    let proof: serde_json::Value = serde_json::from_str(&proof).unwrap();
    assert_eq!(proof["anchor_links"].as_array().unwrap().len(), 1);
    assert_eq!(
        proof["anchor_links"][0]["other_evidence_sha256"],
        http.sha256
    );
    assert!(proof["evidence"]["anchor"].is_null());
    assert_eq!(
        f.store.evidence(&live.sha256).unwrap().available_at_micros,
        1000000
    );
    linked.anchor.as_mut().unwrap().comparison = "EXACT".into();
    assert!(f.store.commit_evidence(scope.revision, &linked).is_err());
}

#[test]
fn new_mode_raw_receive_cannot_exceed_total_record_capacity_before_identity_commit() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    f.db.execute("UPDATE capture_meta SET max_rows=2", [])
        .unwrap();
    assert!(f
        .store
        .receive(
            Some("signature"),
            "alice",
            7,
            b"raw",
            "fingerprint",
            2.,
            Some(2.)
        )
        .is_err());
    assert_eq!(
        f.db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        f.db.query_row("SELECT status FROM capture_meta", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "failed"
    );
    assert!(f.store.prepare_scope().is_err());
}
