#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::Db;

#[test]
fn malformed_only_counterpart_is_an_identity_error_not_an_invisible_other_wallet() -> Result<()> {
    for (table, column, value, reason) in [
        ("proofs", "wallet_pubkey", "x'00'", "Invalid column type"),
        (
            "proofs",
            "wallet_pubkey",
            "' '",
            "invalid BUY receipt claim identity",
        ),
        (
            "facts",
            "order_id",
            "''",
            "invalid BUY receipt claim identity",
        ),
        (
            "facts",
            "tx_signature",
            "' '",
            "invalid BUY receipt claim identity",
        ),
    ] {
        let db = Db::new()?;
        let a = db.seed_claim(
            "a",
            "source-a",
            "buy",
            "mint",
            "other-wallet",
            "shared-signature",
        )?;
        let b = db.seed("dup-b", "source-b", "buy")?;
        let remove = if table == "proofs" { "facts" } else { "proofs" };
        db.conn()?.execute_batch(&format!(
            "PRAGMA foreign_keys=OFF;
            DELETE FROM execution_canary_receipt_{remove} WHERE order_id='{a}';
            UPDATE execution_canary_receipt_{table} SET {column}={value} WHERE order_id='{a}';"
        ))?;
        let before = db.snapshot()?;
        let result = db.buy(&b).unwrap_err();
        assert!(format!("{result:#}").contains(reason), "{result:#}");
        assert_eq!(db.snapshot()?, before);
    }
    Ok(())
}

#[test]
fn contradictory_linked_signature_cannot_hide_the_claim_and_both_pending_owners_are_blocked(
) -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    let before = db.snapshot()?;
    for id in [&a, &b] {
        let error = db.buy(id).unwrap_err();
        assert!(format!("{error:#}").contains("already claimed by another order"));
        assert_eq!(db.snapshot()?, before);
    }
    // Only the linked orders signature still asserts B's identity for A.
    db.conn()?.execute_batch("UPDATE execution_canary_receipt_proofs SET tx_signature='different-proof' WHERE order_id='exec-canary:dup-a';
        UPDATE execution_canary_receipt_facts SET tx_signature='different-facts' WHERE order_id='exec-canary:dup-a'")?;
    let before = db.snapshot()?;
    let error = db.buy(&b).unwrap_err();
    assert!(format!("{error:#}").contains("already claimed by another order"));
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn malformed_linked_order_signature_does_not_become_an_absent_join() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed_claim(
        "a",
        "source-a",
        "buy",
        "mint",
        "other-wallet",
        "shared-signature",
    )?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    db.conn()?.execute(
        "UPDATE orders SET tx_signature=x'00' WHERE order_id=?1",
        [&a],
    )?;
    let before = db.snapshot()?;
    let error = db.buy(&b).unwrap_err();
    assert!(format!("{error:#}").contains("Invalid column type"));
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn unrelated_malformed_history_is_outside_lookup_and_exact_keys_are_not_normalized() -> Result<()> {
    let db = Db::new()?;
    let old = db.seed_claim(
        "old",
        "old-source",
        "buy",
        "mint",
        "execution-wallet",
        "unrelated-signature",
    )?;
    db.conn()?.execute(
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey=x'00' WHERE order_id=?1",
        [&old],
    )?;
    let a = db.seed_claim(
        "a",
        "source-a",
        "buy",
        "mint",
        "execution-wallet",
        "signature",
    )?;
    db.buy(&a)?;
    let b = db.seed_claim(
        "b",
        "source-b",
        "buy",
        "mint",
        "execution-wallet",
        " signature ",
    )?;
    let result = db.buy(&b)?;
    assert_eq!(result.position.qty, 14.0);
    assert_eq!(result.position.cost_lamports.unwrap().as_u64(), 2000);
    Ok(())
}
