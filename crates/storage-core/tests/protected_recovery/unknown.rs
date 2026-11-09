use super::*;

#[test]
fn commit_readback_mismatch_is_unknown_and_cannot_ack_or_resume_using_same_writer() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let evidence = f.evidence();
    f.store.commit_evidence(scope.revision, &evidence).unwrap();
    f.db.execute_batch("CREATE TRIGGER corrupt_readback AFTER INSERT ON capture_recovery BEGIN UPDATE capture_recovery SET observation_json='null' WHERE id=NEW.id; END;").unwrap();
    assert!(f
        .store
        .commit_observation(scope.revision, &f.observation(&evidence), None, |a, b| a
            == b)
        .is_err());
    assert_eq!(
        f.db.query_row(
            "SELECT count(*) FROM capture_recovery_outbox WHERE state='PENDING'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert!(f
        .store
        .prepare_scope()
        .unwrap_err()
        .to_string()
        .contains("outcome unknown"));
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 2.)
        .is_err());
    assert!(f.store.start().is_err());
}

#[test]
fn raw_receive_readback_unknown_does_not_issue_a_followup_fail_closed_write() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    f.db.execute_batch("CREATE TRIGGER corrupt_raw AFTER INSERT ON capture_events BEGIN UPDATE capture_events SET fingerprint='changed' WHERE seq=NEW.seq; END;").unwrap();
    assert!(f
        .store
        .receive(Some("buy"), "alice", 7, b"buy", "fingerprint", 2., Some(2.))
        .is_err());
    let before: (i64, String) =
        f.db.query_row("SELECT gap,status FROM capture_meta", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert!(f.store.fail_closed("must not write on unknown").is_err());
    assert_eq!(
        f.db.query_row("SELECT gap,status FROM capture_meta", [], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?
        )))
        .unwrap(),
        before
    );
}

#[test]
fn decoder_finish_readback_unknown_retains_primary_and_blocks_followup_write() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    f.store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 1.)
        .unwrap();
    let seq = f
        .store
        .receive(
            Some("signature"),
            "alice",
            7,
            b"raw",
            "fingerprint",
            2.,
            Some(2.),
        )
        .unwrap()
        .unwrap()
        .seq;
    f.db.execute_batch("CREATE TRIGGER corrupt_finish AFTER UPDATE OF stage ON capture_events BEGIN UPDATE capture_events SET event_json='{}' WHERE seq=NEW.seq; END;").unwrap();
    let failure = f.store.finish(seq, None, "decode refused").unwrap_err();
    assert!(failure.to_string().contains("readback mismatch"));
    let before: (i64, String) =
        f.db.query_row("SELECT gap,status FROM capture_meta", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert!(f.store.fail_closed("followup forbidden").is_err());
    assert_eq!(
        f.db.query_row("SELECT gap,status FROM capture_meta", [], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?
        )))
        .unwrap(),
        before
    );
}

#[test]
fn start_commit_readback_mismatch_latches_unknown_before_followup_close() {
    let f = Fixture::new();
    f.db.execute_batch("CREATE TRIGGER corrupt_start AFTER UPDATE OF epoch ON capture_meta BEGIN UPDATE capture_meta SET status='stopped' WHERE id=NEW.id; END;").unwrap();
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    assert!(restarted
        .start()
        .unwrap_err()
        .to_string()
        .contains("start readback mismatch"));
    let before: (i64, i64, String) =
        f.db.query_row("SELECT epoch,gap,status FROM capture_meta", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert!(restarted.fail_closed("followup forbidden").is_err());
    assert_eq!(
        f.db.query_row("SELECT epoch,gap,status FROM capture_meta", [], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?
        )))
        .unwrap(),
        before
    );
}

#[test]
fn continuity_gap_readback_unknown_does_not_issue_second_gap_write() {
    let mut f = Fixture::new();
    f.db.execute_batch("CREATE TRIGGER corrupt_gap AFTER UPDATE OF gap ON capture_meta BEGIN UPDATE capture_meta SET reason='unexpected' WHERE id=NEW.id; END;").unwrap();
    assert!(f
        .store
        .gap("transport", false)
        .unwrap_err()
        .to_string()
        .contains("gap readback mismatch"));
    let before: i64 =
        f.db.query_row("SELECT gap FROM capture_meta", [], |r| r.get(0))
            .unwrap();
    assert!(f.store.fail_closed("must not repeat").is_err());
    assert_eq!(
        f.db.query_row("SELECT gap FROM capture_meta", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        before
    );
}

#[test]
fn duplicate_identity_ack_refuses_ignored_evidence_link_commit() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let live = f.evidence();
    let observation = f.observation(&live);
    f.store.commit_evidence(scope.revision, &live).unwrap();
    let first = f
        .store
        .commit_observation(scope.revision, &observation, None, |a, b| a == b)
        .unwrap();
    let mut http = live.clone();
    http.source_kind = ProtectedSourceKind::ConfirmedHttp;
    http.raw = b"http body for same checked identity".to_vec();
    http.sha256 = format!("{:x}", Sha256::digest(&http.raw));
    f.store.commit_evidence(scope.revision, &http).unwrap();
    let duplicate = f.observation(&http);
    f.db.execute_batch("CREATE TRIGGER ignore_evidence_link BEFORE INSERT ON capture_recovery_links BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    let error = f
        .store
        .commit_observation(scope.revision, &duplicate, None, |a, b| a == b)
        .unwrap_err();
    assert!(error.to_string().contains("link commit readback mismatch"));
    assert_eq!(
        f.db.query_row(
            "SELECT count(*) FROM capture_recovery WHERE id=?",
            [first.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 3.)
        .is_err());
}
