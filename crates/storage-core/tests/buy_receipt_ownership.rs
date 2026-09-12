#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::{Lamports, TokenQuantity};
use copybot_storage_core::*;
use fixture::Db;

fn denied<T: std::fmt::Debug>(db: &Db, run: impl FnOnce() -> Result<T>) -> Result<()> {
    let before = db.snapshot()?;
    let result = run();
    println!(
        "BUY_OWNERSHIP result={result:?} unchanged={}",
        db.snapshot()? == before
    );
    assert!(
        result.is_err(),
        "duplicate receipt must not write money: {result:?}"
    );
    assert_eq!(
        db.snapshot()?,
        before,
        "all money, fills, facts and confirmation rows must roll back"
    );
    Ok(())
}

#[test]
fn public_writer_duplicate_receipt_cannot_merge_again() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    let first = db.buy(&a)?;
    assert_eq!(first.position.qty_exact, Some(TokenQuantity::new(7000, 3)));
    assert_eq!(first.position.cost_lamports, Some(Lamports::new(1000)));
    assert_eq!(first.position.qty, 7.0);
    assert_eq!(first.position.cost_sol, 0.000001);
    let b = db.seed("dup-b", "source-b", "buy")?;
    denied(&db, || db.buy(&b))?;
    assert_eq!(db.links()?, vec![(a, Some(first.position.position_id))]);
    assert!(!db.store.execution_canary_fill_exists(&b)?);
    Ok(())
}

#[test]
fn public_writer_duplicate_receipt_cannot_open_after_closed_reopen() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    let first = db.buy(&a)?;
    db.close()?;
    db.reopen()?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    denied(&db, || db.buy(&b))?;
    let before = db.snapshot()?;
    let replay = db.buy(&a)?;
    assert_eq!(replay.position.position_id, first.position.position_id);
    assert_eq!(replay.position.state, "closed");
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn all_public_buy_wrappers_reject_other_order_claim() -> Result<()> {
    for wrapper in ["open", "confirmed", "confirm"] {
        let db = Db::new()?;
        let a = db.seed("dup-a", "source-a", "buy")?;
        db.buy(&a)?;
        let b = db.seed("dup-b", "source-b", "buy")?;
        if wrapper == "confirmed" {
            // Explicit historical proof-only confirmed row, after valid public setup.
            db.conn()?.execute(
                "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
                [&b],
            )?;
            db.conn()?.execute(
                "UPDATE orders SET status='execution_canary_confirmed' WHERE order_id=?1",
                [&b],
            )?;
        }
        denied(&db, || match wrapper {
            "open" => db.store.record_execution_canary_open_position(
                &b,
                "mint",
                7.0,
                Some(TokenQuantity::new(7000, 3)),
                0.000001,
                db.now,
            ),
            "confirmed" => db.store.record_execution_canary_confirmed_buy_fill(
                &b,
                "mint",
                7.0,
                Some(TokenQuantity::new(7000, 3)),
                0.000001,
                db.now,
            ),
            _ => db.buy(&b),
        })?;
    }
    Ok(())
}

#[test]
fn pending_or_orphaned_claims_ignore_mint_leader_route_day_and_side() -> Result<()> {
    for mutation in [
        "SELECT 1",
        "DELETE FROM execution_canary_receipt_facts WHERE order_id='exec-canary:a'",
        "DELETE FROM execution_canary_receipt_proofs WHERE order_id='exec-canary:a'",
        "DELETE FROM orders WHERE order_id='exec-canary:a'",
        "DELETE FROM copy_signals WHERE signal_id='a'",
        "UPDATE orders SET route='historical-route',status='execution_canary_failed',submit_ts='1999-01-01' WHERE order_id='exec-canary:a'",
        "UPDATE execution_canary_receipt_proofs SET tx_signature='proof-other' WHERE order_id='exec-canary:a'; UPDATE execution_canary_receipt_facts SET tx_signature='facts-other' WHERE order_id='exec-canary:a'",
    ] {
        let mut db = Db::new()?;
        db.seed_claim("a", "other-leader", "sell", "other-mint", "execution-wallet", "shared-signature")?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; {mutation}"))?;
        db.now += chrono::Duration::days(2);
        let b = db.seed("dup-b", "source-b", "buy")?;
        denied(&db, || db.buy(&b))?;
    }
    Ok(())
}

#[test]
fn invalid_current_or_related_claim_and_sql_errors_are_not_empty_success() -> Result<()> {
    for mutation in [
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey=x'00' WHERE order_id='exec-canary:a'",
        "UPDATE execution_canary_receipt_facts SET tx_signature=' ' WHERE order_id='exec-canary:a'",
        "UPDATE execution_canary_receipt_facts SET order_id='' WHERE order_id='exec-canary:a'",
        "UPDATE orders SET tx_signature=x'00' WHERE order_id='exec-canary:a'",
        "ALTER TABLE execution_canary_receipt_proofs RENAME COLUMN wallet_pubkey TO unavailable",
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey=' ' WHERE order_id='exec-canary:b'; UPDATE execution_canary_receipt_facts SET wallet_pubkey=' ' WHERE order_id='exec-canary:b'",
    ] {
        let db = Db::new()?;
        db.seed_claim("a", "source-a", "buy", "mint", "execution-wallet", "shared-signature")?;
        let b = db.seed_claim("b", "source-b", "buy", "mint", "execution-wallet", "shared-signature")?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; {mutation}"))?;
        denied(&db, || db.buy(&b))?;
    }
    Ok(())
}

#[test]
fn proof_only_current_identity_requires_durable_order_binding() -> Result<()> {
    for mutation in [
        "UPDATE execution_canary_receipt_proofs SET tx_signature='wrong' WHERE order_id='exec-canary:a'",
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey=' ' WHERE order_id='exec-canary:a'",
        "UPDATE execution_canary_receipt_proofs SET token='wrong' WHERE order_id='exec-canary:a'",
        "DELETE FROM orders WHERE order_id='exec-canary:a'",
    ] {
        let db = Db::new()?;
        let a = db.seed("a", "source-a", "buy")?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; DELETE FROM execution_canary_receipt_facts; {mutation}"))?;
        denied(&db, || db.store.record_execution_canary_open_position(
            &a, "mint", 7.0, Some(TokenQuantity::new(7000, 3)), 0.000001, db.now))?;
    }
    Ok(())
}

#[test]
fn distinct_receipts_and_execution_wallets_account_once_and_replay_original_generation(
) -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed_claim(
        "a",
        "source",
        "buy",
        "mint",
        "execution-a",
        "same-signature",
    )?;
    let first = db.buy(&a)?;
    let b = db.seed_claim(
        "b",
        "source",
        "buy",
        "mint",
        "execution-b",
        "same-signature",
    )?;
    let merged = db.buy(&b)?;
    assert_eq!(
        merged.position.qty_exact,
        Some(TokenQuantity::new(14000, 3))
    );
    assert_eq!(merged.position.cost_lamports, Some(Lamports::new(2000)));
    db.close()?;
    let c = db.seed_claim(
        "c",
        "source",
        "buy",
        "mint",
        "execution-a",
        "distinct-signature",
    )?;
    let next = db.buy(&c)?;
    assert_ne!(next.position.position_id, first.position.position_id);
    db.reopen()?;
    let before = db.snapshot()?;
    for id in [&a, &b] {
        assert_eq!(db.buy(id)?.position.position_id, first.position.position_id);
    }
    assert_eq!(db.snapshot()?, before);
    denied(&db, || db.buy_claim(&a, "other-mint"))?;
    Ok(())
}
