#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;
#[path = "common/buy_receipt_history.rs"]
mod history;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::Db;

fn read(db: &Db) -> Result<OpenPositionBuyAttribution> {
    let before = db.snapshot()?;
    let result = db.store.load_execution_canary_buy_attribution("mint")?;
    assert_eq!(db.snapshot()?, before);
    let ExecutionCanaryBuyAttribution::Open(value) = result else {
        panic!("missing position")
    };
    Ok(value)
}

fn ambiguous(db: &Db, id: &str) -> Result<()> {
    let value = read(db)?;
    assert!(!value.proven_contributors.iter().any(|c| c.order_id == id));
    assert!(
        value
            .unproven_links
            .iter()
            .any(|c| c.order_id.as_deref() == Some(id)
                && c.reason == BuyAttributionIssue::AmbiguousReceipt),
        "{value:?}"
    );
    Ok(())
}

#[test]
fn ambiguous_sources_are_excluded_while_independent_source_stays_proven() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    history::buy(&db, &b, "mint")?;
    let c = db.seed("c", "source-c", "buy")?;
    db.buy(&c)?;
    db.reopen()?;
    let value = read(&db)?;
    assert_eq!(value.coverage, BuyAttributionCoverage::ProvenSubset);
    assert_eq!(value.proven_contributors.len(), 1);
    assert_eq!(value.proven_contributors[0].order_id, c);
    ambiguous(&db, &a)?;
    ambiguous(&db, &b)?;
    let before = db.snapshot()?;
    for id in [&a, &b, &c] {
        assert_eq!(
            db.buy(id)?.outcome,
            ExecutionCanaryPositionRecordOutcome::Existing
        );
        assert_eq!(read(&db)?, value);
    }
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn closed_null_and_other_token_claims_are_global_conflicts() -> Result<()> {
    for case in ["closed", "null", "other_token", "closed_null_other_token"] {
        let mut db = Db::new()?;
        let token = if case.contains("other_token") {
            "other-mint"
        } else {
            "mint"
        };
        let a = db.seed_claim(
            "a",
            "source-a",
            "buy",
            token,
            "execution-wallet",
            "shared-signature",
        )?;
        db.buy_claim(&a, token)?;
        if case.contains("closed") {
            db.store.record_execution_canary_manual_terminal_write_off(
                token,
                "tiny",
                "fixture_close",
                db.now,
            )?;
        }
        if case.contains("null") {
            // Historical NULL is a test-only downgrade; the two root REDs use no SQL changes.
            db.conn()?
                .execute("UPDATE fills SET position_id=NULL WHERE order_id=?1", [&a])?;
        }
        let b = db.seed("dup-b", "source-b", "buy")?;
        history::buy(&db, &b, "mint")?;
        db.reopen()?;
        ambiguous(&db, &b)?;
        let value = read(&db)?;
        assert_eq!(
            value.coverage,
            BuyAttributionCoverage::Unknown,
            "{case}: {value:?}"
        );
        assert!(value.proven_contributors.is_empty());
    }
    Ok(())
}

#[test]
fn same_signature_in_distinct_execution_wallets_remains_independent() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed_claim(
        "a",
        "source-a",
        "buy",
        "mint",
        "execution-a",
        "shared-signature",
    )?;
    db.buy(&a)?;
    let b = db.seed_claim(
        "b",
        "source-b",
        "buy",
        "mint",
        "execution-b",
        "shared-signature",
    )?;
    db.buy(&b)?;
    db.reopen()?;
    let before = db.snapshot()?;
    let value = read(&db)?;
    assert_eq!(value.coverage, BuyAttributionCoverage::ProvenSubset);
    assert_eq!(value.proven_contributors.len(), 2);
    assert!(value.unproven_links.is_empty());
    for id in [&a, &b] {
        db.buy(id)?;
    }
    assert_eq!(read(&db)?, value);
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn same_source_wallet_cannot_resolve_two_order_claims_of_one_receipt() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("dup-a", "same-source", "buy")?;
    db.buy(&a)?;
    let b = db.seed("dup-b", "same-source", "buy")?;
    history::buy(&db, &b, "mint")?;
    ambiguous(&db, &a)?;
    ambiguous(&db, &b)?;
    Ok(())
}

#[test]
fn incomplete_or_contradictory_counterparts_cannot_disappear_through_joins() -> Result<()> {
    for sql in [
        "DELETE FROM execution_canary_receipt_facts WHERE order_id='exec-canary:dup-a'",
        "DELETE FROM execution_canary_receipt_proofs WHERE order_id='exec-canary:dup-a'",
        "DELETE FROM orders WHERE order_id='exec-canary:dup-a'",
        "DELETE FROM copy_signals WHERE signal_id='dup-a'",
        "DELETE FROM fills WHERE order_id='exec-canary:dup-a'",
        "UPDATE copy_signals SET side='sell',token='wrong' WHERE signal_id='dup-a'",
        "UPDATE orders SET status='execution_canary_failed',signal_id='missing' WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_proofs SET side='sell',token='wrong',confirmed_at='1999-01-01T00:00:00Z' WHERE order_id='exec-canary:dup-a'; UPDATE execution_canary_receipt_facts SET side='sell',token='wrong',block_time=0 WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_facts SET tx_signature='different-facts' WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_proofs SET tx_signature='different-proof' WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_facts SET tx_signature='different-facts' WHERE order_id='exec-canary:dup-a'; UPDATE execution_canary_receipt_proofs SET tx_signature='different-proof' WHERE order_id='exec-canary:dup-a'",
    ] {
        let mut db = Db::new()?;
        let a = db.seed("dup-a", "source-a", "buy")?;
        db.buy(&a)?;
        db.close()?;
        let b = db.seed("dup-b", "source-b", "buy")?;
        history::buy(&db, &b, "mint")?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        db.reopen()?;
        ambiguous(&db, &b)?;
    }
    Ok(())
}

#[test]
fn unaccounted_proof_claim_alone_already_makes_membership_ambiguous() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    // No fill; facts removed to exercise the remaining durable proof claim.
    db.conn()?.execute(
        "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&a],
    )?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    history::buy(&db, &b, "mint")?;
    ambiguous(&db, &b)?;
    Ok(())
}

#[test]
fn malformed_counterpart_identity_and_sql_errors_are_not_swallowed() -> Result<()> {
    for sql in [
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey=x'00' WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_facts SET tx_signature=' ' WHERE order_id='exec-canary:dup-a'",
        "UPDATE execution_canary_receipt_facts SET order_id='' WHERE order_id='exec-canary:dup-a'",
        "ALTER TABLE execution_canary_receipt_proofs RENAME COLUMN wallet_pubkey TO unavailable",
    ] {
        let db = Db::new()?;
        let a = db.seed("dup-a", "source-a", "buy")?;
        db.buy(&a)?;
        db.close()?;
        let b = db.seed("b", "source-b", "buy")?;
        db.buy(&b)?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        let before = db.snapshot()?;
        assert!(db.store.load_execution_canary_buy_attribution("mint").is_err(), "{sql}");
        assert_eq!(db.snapshot()?, before);
    }
    Ok(())
}
