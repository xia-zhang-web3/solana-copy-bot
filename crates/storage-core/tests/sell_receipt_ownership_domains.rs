#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;

fn check(db: &Db, reason: Option<&str>) -> Result<()> {
    let before = snapshot(&db.conn()?)?;
    let error = db
        .store
        .apply_execution_canary_sell_settlement(&db.facts(1, 0), as_of())
        .expect_err("foreign or unproven ownership cannot account");
    if let Some(expected) = reason {
        let typed = error
            .downcast_ref::<SellSettlementUnsupported>()
            .expect("local ownership refusal must be typed");
        assert_eq!(format!("{typed:?}"), expected);
    } else {
        assert!(
            error.downcast_ref::<rusqlite::Error>().is_some(),
            "schema/decode error must propagate: {error:#}"
        );
        assert!(!error.is::<SellSettlementUnsupported>());
    }
    assert_eq!(snapshot(&db.conn()?)?, before);
    assert!(!db.store.execution_canary_fill_exists(ORDER)?);
    assert!(db.store.execution_canary_accounting_pending()?);
    Ok(())
}

#[test]
fn pending_proof_facts_orphan_and_other_metadata_cannot_hide_a_claim() -> Result<()> {
    for variant in [
        "pending",
        "proof-only",
        "facts-only",
        "orphan",
        "metadata",
        "failed",
    ] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        let mut claim = db.facts(1, 0);
        claim.order_id = "foreign-claim".into();
        if variant == "metadata" {
            claim.token = "other-mint".into();
            claim.side = "buy".into();
            claim.token_delta.as_mut().unwrap().raw = 1;
            claim.block_time = Some((as_of() - Duration::days(20)).timestamp());
            claim.slot += 99;
        }
        prepare_identity(&db, claim, "other-route")?;
        let conn = db.conn()?;
        // Explicit corruption/history after valid fixture/API setup only.
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        match variant {
            "proof-only" => {
                conn.execute(
                    "DELETE FROM execution_canary_receipt_facts WHERE order_id='foreign-claim'",
                    [],
                )?;
            }
            "facts-only" => {
                conn.execute(
                    "DELETE FROM execution_canary_receipt_proofs WHERE order_id='foreign-claim'",
                    [],
                )?;
            }
            "orphan" => {
                conn.execute("DELETE FROM orders WHERE order_id='foreign-claim'", [])?;
            }
            "failed" => {
                conn.execute(
                    "UPDATE orders SET status='canary_failed' WHERE order_id='foreign-claim'",
                    [],
                )?;
            }
            "metadata" => {
                conn.execute("UPDATE copy_signals SET wallet_id='other-leader',ts='2000-01-01T00:00:00Z' WHERE signal_id='foreign-claim'",[])?;
            }
            _ => {}
        }
        check(&db, Some("ReceiptAlreadyClaimed"))?;
    }
    Ok(())
}

#[test]
fn independent_linked_order_signature_is_a_claim_in_each_receipt_table() -> Result<()> {
    for keep in ["proofs", "facts"] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        prepare(&db, "foreign-claim", 1, 0)?; // initially a distinct durable signature
        let conn = db.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        let drop_table = if keep == "proofs" { "facts" } else { "proofs" };
        conn.execute(
            &format!(
                "DELETE FROM execution_canary_receipt_{drop_table} WHERE order_id='foreign-claim'"
            ),
            [],
        )?;
        conn.execute(
            "UPDATE orders SET tx_signature='signature' WHERE order_id='foreign-claim'",
            [],
        )?;
        check(&db, Some("ReceiptAlreadyClaimed"))?;
    }
    Ok(())
}

#[test]
fn malformed_related_keys_are_typed_but_decode_errors_are_not() -> Result<()> {
    for mutation in [
        "order_id=''",
        "wallet_pubkey=' '",
        "tx_signature=''",
        "blob",
        "linked",
    ] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        let mut other = db.facts(1, 0);
        other.order_id = "foreign-claim".into();
        prepare_identity(&db, other, "tiny")?;
        let conn = db.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        conn.execute(
            "DELETE FROM execution_canary_receipt_proofs WHERE order_id='foreign-claim'",
            [],
        )?;
        if mutation == "linked" {
            conn.execute(
                "UPDATE orders SET tx_signature='' WHERE order_id='foreign-claim'",
                [],
            )?;
        } else {
            let set = if mutation == "blob" {
                "wallet_pubkey=CAST('wallet' AS BLOB)"
            } else {
                mutation
            };
            conn.execute(&format!("UPDATE execution_canary_receipt_facts SET {set} WHERE order_id='foreign-claim'"),[])?;
        }
        check(
            &db,
            if mutation == "blob" {
                None
            } else {
                Some("UnprovenReceiptOwnership")
            },
        )?;
    }
    Ok(())
}

#[test]
fn missing_modern_receipt_tables_are_sql_errors_without_repair() -> Result<()> {
    for table in ["proofs", "facts"] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        db.conn()?.execute_batch(&format!(
            "ALTER TABLE execution_canary_receipt_{table} RENAME TO unavailable_{table}"
        ))?;
        check(&db, None)?;
    }
    Ok(())
}

#[test]
fn unrelated_bad_history_and_distinct_exact_wallet_keys_do_not_block() -> Result<()> {
    for wallet in ["bad-unrelated", " wallet ", "Wallet"] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        let mut other = db.facts(1, 0);
        other.order_id = "foreign-claim".into();
        other.wallet_pubkey = wallet.into();
        if wallet == "bad-unrelated" {
            other.tx_signature = "unrelated".into();
        }
        prepare_identity(&db, other, "tiny")?;
        if wallet == "bad-unrelated" {
            db.conn()?.execute("UPDATE execution_canary_receipt_facts SET wallet_pubkey=CAST('bad' AS BLOB) WHERE order_id='foreign-claim'",[])?;
        }
        let result = db
            .store
            .apply_execution_canary_sell_settlement(&db.facts(1, 0), as_of())?;
        assert!(!result.already_accounted);
        assert_eq!(result.settlement.remaining_quantity.raw(), 2);
    }
    Ok(())
}
