#[path = "common/session_reset_case.rs"]
mod c;
use c::*;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{native_buy::*, SqliteStore};

#[test]
fn saved_partial_transaction_recovers_checkpoint_without_replacing_first_terminal() {
    let (_tmp, path) = f::db();
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    inbox
        .persist(&checkpoint(), &CandidateGeneration::Unknown)
        .unwrap();
    let row = inbox.identity(&signature()).unwrap().unwrap();
    assert!(!row.conflict && !row.recovery);
    assert_eq!(
        row.terminal,
        Some(Terminal::Unresolved(Unresolved::SessionReset))
    );
    let head = inbox.replay_checkpoint(&scope()).unwrap().unwrap();
    assert_eq!(head.block.observation.child.slot, assertion().slot);
    assert_eq!(head.block.claims[0].info, row.admission.info);
}

#[test]
fn recovered_terminal_alone_has_no_checkpoint_or_sell_observation_authority() {
    let (_tmp, path) = f::db();
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        assertion().slot - 1
    );
    let row = inbox.identity(&signature()).unwrap().unwrap();
    assert_eq!(
        row.terminal,
        Some(Terminal::Unresolved(Unresolved::SessionReset))
    );
    assert!(!row.conflict);
}

#[test]
fn changed_checkpoint_info_float_index_slot_hash_scope_count_or_branch_refuses() {
    for mode in [
        "info", "float", "index", "slot", "hash", "scope", "count", "branch", "session",
    ] {
        let (_tmp, path) = f::db();
        let mut inbox = open(&path);
        feed(&mut inbox, &events());
        let mut d = checkpoint();
        let DeliveryEvent::ParentCheckpoint(ref mut b) = d.event else {
            panic!()
        };
        match mode {
            "info" => b.claims[0].info.encoded.push(1),
            "float" => b.claims[0].info.float_bits.push(Some((-0.0f64).to_bits())),
            "index" => b.claims[0].transaction_index += 1,
            "slot" => b.observation.child.slot += 1,
            "hash" => b.observation.child.hash = b.observation.parent.hash.clone(),
            "scope" => b.scope.wallets.clear(),
            "count" => b.supplied_transaction_count += 1,
            "branch" => b.observation.parent.slot -= 2,
            "session" => d.session = "unrelated-session".into(),
            _ => unreachable!(),
        }
        assert!(
            inbox.persist(&d, &CandidateGeneration::Unknown).is_err(),
            "{mode}"
        );
        assert_eq!(
            inbox
                .replay_checkpoint(&scope())
                .unwrap()
                .unwrap()
                .block
                .observation
                .child
                .slot,
            assertion().slot - 1
        );
        assert!(!inbox.identity(&signature()).unwrap().unwrap().conflict);
    }
}

#[test]
fn changed_recovered_identity_and_incompatible_provider_assertions_stay_conflicting() {
    for mode in [
        "facts",
        "info",
        "float",
        "slot",
        "assertion",
        "not-recovered",
    ] {
        let (_tmp, path) = f::db();
        let mut inbox = open(&path);
        let all = events();
        feed(&mut inbox, &all[..all.len() - 1]);
        if mode == "assertion" {
            feed(&mut inbox, &all[all.len() - 1..]);
        }
        let mut d = all.last().unwrap().clone();
        d.sequence += 1;
        let DeliveryEvent::Terminal {
            ref mut expected,
            result: Terminal::ProviderAsserted(ref mut a),
            ..
        } = d.event
        else {
            panic!()
        };
        match mode {
            "facts" => expected.facts.wallet.push('x'),
            "info" => expected.info.encoded.push(1),
            "float" => expected.info.float_bits.push(None),
            "slot" => expected.facts.slot += 1,
            "assertion" => a.transaction_index += 1,
            "not-recovered" => {
                expected.message_time = MessageTime::CreatedAt {
                    seconds: 1,
                    nanos: 0,
                }
            }
            _ => unreachable!(),
        }
        inbox.persist(&d, &CandidateGeneration::Unknown).unwrap();
        assert!(
            inbox.identity(&signature()).unwrap().unwrap().conflict,
            "{mode}"
        );
        assert!(inbox
            .persist(&checkpoint(), &CandidateGeneration::Unknown)
            .is_err());
    }
}

#[test]
fn other_unresolved_reasons_are_not_refined() {
    for reason in [
        Unresolved::Expired,
        Unresolved::PendingCapacity,
        Unresolved::EndOfStream,
        Unresolved::ConflictingTransaction,
        Unresolved::ConflictingAssertions,
        Unresolved::Association("bad".into()),
    ] {
        let (_tmp, path) = f::db();
        let mut inbox = open(&path);
        let mut all = events();
        let DeliveryEvent::Terminal { ref mut result, .. } = all[2].event else {
            panic!()
        };
        *result = Terminal::Unresolved(reason.clone());
        feed(&mut inbox, &all);
        let row = inbox.identity(&signature()).unwrap().unwrap();
        assert_eq!(row.terminal, Some(Terminal::Unresolved(reason)));
        assert!(row.conflict);
        assert!(inbox
            .persist(&checkpoint(), &CandidateGeneration::Unknown)
            .is_err());
    }
}

#[test]
fn restart_and_commit_before_ack_duplicates_preserve_original_identity_and_checkpoint() {
    let (_tmp, path) = f::db();
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    let before = inbox.identity(&signature()).unwrap().unwrap();
    drop(inbox); // Recovered terminal committed, checkpoint not committed.
    let mut inbox = open(&path);
    inbox
        .persist(&checkpoint(), &CandidateGeneration::Unknown)
        .unwrap();
    let head = inbox.replay_checkpoint(&scope()).unwrap();
    drop(inbox); // Checkpoint committed, ACK lost.
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    inbox
        .persist(&checkpoint(), &f::candidate("must-not-rebind"))
        .unwrap();
    assert_eq!(inbox.identity(&signature()).unwrap().unwrap(), before);
    assert_eq!(inbox.replay_checkpoint(&scope()).unwrap(), head);
}

#[test]
fn ignored_cursor_write_rolls_back_refinement_proof_and_retry_is_idempotent() {
    let (_tmp, path) = f::db();
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER no_cursor BEFORE UPDATE ON association_replay_cursor BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(inbox
        .persist(&checkpoint(), &CandidateGeneration::Unknown)
        .is_err());
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM association_inbox_events WHERE sequence=72",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    sql.execute_batch("DROP TRIGGER no_cursor;").unwrap();
    inbox
        .persist(&checkpoint(), &CandidateGeneration::Unknown)
        .unwrap();
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        assertion().slot
    );
}

#[test]
fn benign_late_observation_uses_committed_recovery_but_conflicting_late_is_sticky() {
    let (_tmp, path) = f::db();
    let mut inbox = open(&path);
    feed(&mut inbox, &events());
    inbox
        .persist(&checkpoint(), &CandidateGeneration::Unknown)
        .unwrap();
    let mut late = checkpoint();
    late.sequence += 1;
    late.event = DeliveryEvent::Late {
        signature: signature(),
        original: Terminal::ProviderAsserted(assertion()),
        evidence: Late::ProviderAssertion(assertion()),
    };
    inbox.persist(&late, &CandidateGeneration::Unknown).unwrap();
    assert!(!inbox.identity(&signature()).unwrap().unwrap().conflict);
    late.sequence += 1;
    let DeliveryEvent::Late {
        evidence: Late::ProviderAssertion(ref mut a),
        ..
    } = late.event
    else {
        panic!()
    };
    a.transaction_index += 1;
    inbox.persist(&late, &CandidateGeneration::Unknown).unwrap();
    assert!(inbox.identity(&signature()).unwrap().unwrap().conflict);
    assert!(inbox.replay_checkpoint(&scope()).is_err());
}

#[test]
fn crash_after_reset_terminal_before_reset_marker_never_rearms_original_buy() {
    let (_tmp, path) = f::db();
    let all = events();
    let now = chrono::Utc::now();
    let mut inbox = open(&path);
    let DeliveryEvent::Admission(ref first) = all[1].event else {
        panic!()
    };
    inbox
        .register_technical_cohort_authority(&TechnicalCohortAuthority {
            run_id: "session-reset-once".into(),
            wallet_ids: vec![first.facts.wallet.clone()],
            mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
            activated_at: now - chrono::Duration::seconds(10),
            deadline: now + chrono::Duration::seconds(60),
            max_buy_count: 1,
            policy_identity: "policy".into(),
        })
        .unwrap();
    inbox
        .record_native_buy_fence_epoch(&NativeBuyFence {
            session: all[1].session.clone(),
            processed_slot: first.facts.slot - 1,
            sampled_at: now - chrono::Duration::seconds(1),
            genesis_hash: "genesis".into(),
            policy_identity: "policy".into(),
        })
        .unwrap();
    for d in &all[..3] {
        inbox
            .persist_at(d, &CandidateGeneration::Unknown, now)
            .unwrap();
    }
    let sql = rusqlite::Connection::open(&path).unwrap();
    let saved: String = sql.query_row("SELECT json_array(signature,first_session,first_sequence,admission,admitted_at,decision_id,finalized_at,late) FROM native_buy_decisions",[],|r|r.get(0)).unwrap();
    let original = inbox.identity(&signature()).unwrap().unwrap();
    drop(inbox); // No SessionGap::Reset, active_session still names the old reader.
    let mut inbox = open(&path);
    let new_session = "fresh-startup-uuid:0";
    inbox
        .record_native_buy_fence_epoch(&NativeBuyFence {
            session: new_session.into(),
            processed_slot: first.facts.slot,
            sampled_at: now,
            genesis_hash: "genesis".into(),
            policy_identity: "policy".into(),
        })
        .unwrap();
    for d in &all[4..] {
        let mut d = d.clone();
        d.session = new_session.into();
        inbox
            .persist_at(&d, &CandidateGeneration::Unknown, now)
            .unwrap();
    }
    let mut d = checkpoint();
    d.session = new_session.into();
    inbox.persist(&d, &CandidateGeneration::Unknown).unwrap();
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        assertion().slot
    );
    assert_eq!(inbox.identity(&signature()).unwrap().unwrap(), original);
    let store = SqliteStore::open(&path).unwrap();
    assert!(store.list_native_buy_pending(2).unwrap().is_empty());
    assert!(!store
        .native_buy_record_finalized(&signature(), assertion().slot, SPL_TOKEN_PROGRAM, now)
        .unwrap());
    assert!(store
        .native_buy_ready(&signature(), now, 120)
        .unwrap()
        .is_none());
    let after: String = sql.query_row("SELECT json_array(signature,first_session,first_sequence,admission,admitted_at,decision_id,finalized_at,late) FROM native_buy_decisions",[],|r|r.get(0)).unwrap();
    assert_eq!(saved, after);
    assert_eq!(
        sql.query_row("SELECT count(*) FROM native_buy_decisions", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}
