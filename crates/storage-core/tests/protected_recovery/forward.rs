//! R4: the exact committed disposition is required, never just terminal state.
use super::*;

fn one(f: &mut Fixture) -> (copybot_storage_core::capture_scope::ProtectedScope, i64) {
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let e = f.evidence();
    f.store.commit_evidence(scope.revision, &e).unwrap();
    let o = f.observation(&e);
    let receipt = f
        .store
        .commit_observation(scope.revision, &o, None, |a, b| a == b)
        .unwrap();
    (scope, receipt.id)
}
#[test]
fn committed_terminal_corruption_never_counts_as_drained_or_checkpoint_ack() {
    let mut f = Fixture::new();
    let (scope, _) = one(&mut f);
    f.db.execute_batch("CREATE TRIGGER corrupt_forward AFTER UPDATE OF state ON capture_recovery_outbox BEGIN UPDATE capture_recovery_outbox SET disposition_json='corrupt-after-commit' WHERE recovery_id=NEW.recovery_id; END;").unwrap();
    f.finish_no_swap_outbox();
    assert!(f.store.recovery_outbox_pending().unwrap());
    assert!(f
        .store
        .verify_forward_dispositions()
        .unwrap_err()
        .to_string()
        .contains("tuple hash mismatch"));
    assert!(f
        .store
        .commit_scope(&scope, &Fixture::head(scope.wallets.clone(), 7), 3.)
        .is_err());
    assert_eq!(
        f.db.query_row(
            "SELECT count(*) FROM capture_recovery_outbox WHERE acknowledged_sha256 IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
#[test]
fn recomputed_digest_does_not_authorize_a_trading_flag_or_extra_claim() {
    let mut f = Fixture::new();
    one(&mut f);
    f.finish_no_swap_outbox();
    let (id, reason, wire): (i64, String, String) =
        f.db.query_row(
            "SELECT recovery_id,reason,disposition_json FROM capture_recovery_outbox",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let mut event: serde_json::Value = serde_json::from_str(&wire).unwrap();
    event["trading_admission"] = serde_json::json!(true);
    let altered = serde_json::to_string(&event).unwrap();
    let hash = forward_hash("REJECTED", &reason, &altered);
    f.db.execute("UPDATE capture_recovery_outbox SET disposition_json=?,disposition_sha256=? WHERE recovery_id=?",params![altered,hash,id]).unwrap();
    assert!(f
        .store
        .verify_forward_dispositions()
        .unwrap_err()
        .to_string()
        .contains("canonical/nontrading"));
    assert!(f.store.recovery_outbox_pending().unwrap());
}
#[test]
fn verified_receipt_mutation_invalidates_partial_index_marker_even_after_restart() {
    let mut f = Fixture::new();
    let (_, id) = one(&mut f);
    f.finish_no_swap_outbox();
    assert!(f.store.verify_forward_dispositions().unwrap());
    assert!(!f.store.recovery_outbox_pending().unwrap());
    f.db.execute(
        "UPDATE capture_recovery_outbox SET disposition_json='later-corrupt' WHERE recovery_id=?",
        [id],
    )
    .unwrap();
    assert!(f.store.recovery_outbox_pending().unwrap());
    assert!(f
        .db
        .query_row(
            "SELECT acknowledged_sha256 FROM capture_recovery_outbox",
            [],
            |r| r.get::<_, Option<String>>(0)
        )
        .unwrap()
        .is_none());
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    restarted.start().unwrap();
    assert!(restarted.verify_forward_dispositions().is_err());
}
#[test]
fn rust_acknowledgement_readback_fault_is_unknown_and_does_not_cleanup_write() {
    let mut f = Fixture::new();
    one(&mut f);
    f.finish_no_swap_outbox();
    f.db.execute_batch("CREATE TRIGGER corrupt_rust_forward AFTER UPDATE OF acknowledged_sha256 ON capture_recovery_outbox WHEN NEW.acknowledged_sha256 IS NOT NULL BEGIN UPDATE capture_recovery_outbox SET disposition_json='corrupt-during-ack-readback' WHERE recovery_id=NEW.recovery_id; END;").unwrap();
    assert!(f
        .store
        .verify_forward_dispositions()
        .unwrap_err()
        .to_string()
        .contains("acknowledgement commit readback mismatch"));
    let before: i64 =
        f.db.query_row("SELECT gap FROM capture_meta", [], |r| r.get(0))
            .unwrap();
    assert!(f.store.fail_closed("must not write after unknown").is_err());
    assert_eq!(
        f.db.query_row("SELECT gap FROM capture_meta", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        before
    );
}
#[test]
fn forward_validation_uses_bounded_unverified_batches_and_partial_indexes() {
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let e = f.evidence();
    f.store.commit_evidence(scope.revision, &e).unwrap();
    // Synthetic storage-only identities; this is not a complete-block/live fixture.
    for i in 0..129 {
        let mut o = f.observation(&e);
        o.signature = Some(format!("bounded-{i}"));
        f.store
            .commit_observation(scope.revision, &o, None, |a, b| a == b)
            .unwrap();
    }
    f.finish_no_swap_outbox();
    assert!(!f.store.verify_forward_dispositions().unwrap());
    assert_eq!(
        f.db.query_row(
            "SELECT count(*) FROM capture_recovery_outbox WHERE acknowledged_sha256 IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        128
    );
    assert!(f.store.verify_forward_dispositions().unwrap());
    let plan:String=f.db.query_row("EXPLAIN QUERY PLAN SELECT recovery_id FROM capture_recovery_outbox INDEXED BY capture_recovery_forward_unverified WHERE state!='PENDING' AND (acknowledged_sha256 IS NULL OR acknowledged_sha256 IS NOT disposition_sha256) ORDER BY recovery_id LIMIT 128",[],|r|r.get(3)).unwrap();
    assert!(plan.contains("capture_recovery_forward_unverified"));
}
#[test]
fn exact_partial_token_debit_is_bound_without_fabricating_joint_sol_exactness() {
    use copybot_core_types::association_delivery::{CheckedFacts, ProtectedTokenQuantity};
    let mut f = Fixture::new();
    f.request("alice");
    let scope = f.store.prepare_scope().unwrap();
    let e = f.evidence();
    f.store.commit_evidence(scope.revision, &e).unwrap();
    let mut o = f.observation(&e);
    o.facts = Some(CheckedFacts {
        signature: "signature".into(),
        wallet: "alice".into(),
        slot: 7,
        token_in: "mint".into(),
        token_out: "So11111111111111111111111111111111111111112".into(),
        amount_in_bits: 1.0f64.to_bits(),
        amount_out_bits: 0.01f64.to_bits(),
        exact_amounts: None,
        programs: vec![],
        dex: "fixture".into(),
        program_fallback: false,
    });
    o.token_quantity = Some(ProtectedTokenQuantity {
        side: "SELL".into(),
        mint: "mint".into(),
        raw: "600".into(),
        decimals: 6,
    });
    let id = f
        .store
        .commit_observation(scope.revision, &o, None, |a, b| a == b)
        .unwrap()
        .id;
    let (provenance, raw_hash): (String, String) =
        f.db.query_row(
            "SELECT provenance_json,raw_hash FROM capture_recovery WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
    let reason = "recovered_historical_sell_unexecuted";
    let mut event = serde_json::json!({"version":1,"recovery_id":id,"signature":"signature","wallet":"alice","slot":7,"source_kind":"complete_live_block","provenance":provenance,"block":o.block,"raw_sha256":raw_hash,"timeliness":"not_established","trading_admission":false,"follower_action":"none","status":"MISSED_UNEXECUTED","reason":reason,"side":"SELL","mint":"mint","token_qty_raw":"601"});
    let wrong = serde_json::to_string(&event).unwrap();
    f.db.execute("UPDATE capture_recovery_outbox SET state='MISSED',reason=?,disposition_json=?,disposition_sha256=? WHERE recovery_id=?",params![reason,wrong,forward_hash("MISSED",reason,&wrong),id]).unwrap();
    assert!(f.store.verify_forward_dispositions().is_err());
    event["token_qty_raw"] = serde_json::json!("600");
    let exact = serde_json::to_string(&event).unwrap();
    f.db.execute("UPDATE capture_recovery_outbox SET disposition_json=?,disposition_sha256=? WHERE recovery_id=?",params![exact,forward_hash("MISSED",reason,&exact),id]).unwrap();
    assert!(f.store.verify_forward_dispositions().unwrap());
    assert!(o.facts.as_ref().unwrap().exact_amounts.is_none());
}
