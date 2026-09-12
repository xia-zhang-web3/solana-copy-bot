#[path = "common/legacy_sell_ownership_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn foreign_pending_proof_facts_orphan_metadata_and_linked_claims_are_not_filtered() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        for variant in [
            "pending",
            "proof-only",
            "facts-only",
            "orphan",
            "metadata",
            "linked-proof",
            "linked-facts",
        ] {
            let db = database(3)?;
            let current = seed(&db, "dup-current", api)?;
            let other = db.seed_claim(
                "foreign",
                "other-leader",
                if variant == "metadata" { "buy" } else { "sell" },
                "other-mint",
                "execution-wallet",
                if variant.starts_with("linked") {
                    "different"
                } else {
                    "shared-signature"
                },
            )?;
            let conn = db.conn()?;
            // Explicit corruption/history only after valid order/proof/facts APIs.
            conn.execute_batch("PRAGMA foreign_keys=OFF")?;
            if matches!(variant, "proof-only" | "linked-proof") {
                conn.execute(
                    "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
                    [&other],
                )?;
            }
            if matches!(variant, "facts-only" | "linked-facts") {
                conn.execute(
                    "DELETE FROM execution_canary_receipt_proofs WHERE order_id=?1",
                    [&other],
                )?;
            }
            if variant == "orphan" {
                conn.execute("DELETE FROM orders WHERE order_id=?1", [&other])?;
            }
            if variant.starts_with("linked") {
                conn.execute(
                    "UPDATE orders SET tx_signature='shared-signature' WHERE order_id=?1",
                    [&other],
                )?;
            }
            if variant == "metadata" {
                conn.execute("UPDATE orders SET route='other-route',status='failed',confirm_ts='2000-01-01T00:00:00Z' WHERE order_id=?1",[&other])?;
            }
            rejected(&db, api, &current)?;
        }
    }
    Ok(())
}

#[test]
fn current_receipts_must_bind_each_other_and_order_signal_caller_token() -> Result<()> {
    for (table, set) in [
        ("proofs", "wallet_pubkey='other'"),
        ("facts", "wallet_pubkey=' '"),
        ("proofs", "tx_signature='other'"),
        ("facts", "tx_signature=''"),
        ("proofs", "side='buy'"),
        ("facts", "token='other-mint'"),
        ("caller", ""),
    ] {
        let db = database(3)?;
        let id = seed(&db, "current", Api::Confirmed)?;
        if table != "caller" {
            db.conn()?.execute(
                &format!("UPDATE execution_canary_receipt_{table} SET {set} WHERE order_id=?1"),
                [&id],
            )?;
        }
        let before = snapshot(&db)?;
        let error = apply_token(
            &db,
            Api::Confirmed,
            &id,
            if table == "caller" {
                "other-mint"
            } else {
                "mint"
            },
        )
        .expect_err("unproven current binding must not write");
        assert_eq!(
            error.downcast_ref::<SellSettlementUnsupported>(),
            Some(&SellSettlementUnsupported::UnprovenReceiptOwnership)
        );
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn malformed_foreign_text_is_typed_and_sql_decode_remains_an_error() -> Result<()> {
    for bad in [
        "wallet_pubkey=' '",
        "order_id=''",
        "tx_signature=''",
        "wallet_pubkey=CAST('wallet' AS BLOB)",
    ] {
        let db = database(3)?;
        let id = seed(&db, "dup-current", Api::Confirmed)?;
        let other = seed(&db, "dup-foreign", Api::Confirm)?;
        let conn = db.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        conn.execute(
            "DELETE FROM execution_canary_receipt_proofs WHERE order_id=?1",
            [&other],
        )?;
        conn.execute(
            &format!("UPDATE execution_canary_receipt_facts SET {bad} WHERE order_id=?1"),
            [&other],
        )?;
        let before = snapshot(&db)?;
        let error = apply(&db, Api::Confirmed, &id)
            .expect_err("malformed claim must not become an empty lookup");
        if bad.contains("BLOB") {
            assert!(error.is::<rusqlite::Error>());
        } else {
            assert_eq!(
                error.downcast_ref::<SellSettlementUnsupported>(),
                Some(&SellSettlementUnsupported::UnprovenReceiptOwnership)
            );
        }
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn proof_only_current_and_recorded_fill_replay_preserve_legacy_contracts() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        let mut db = database(3)?;
        let a = seed(&db, "dup-first", api)?;
        db.conn()?.execute(
            "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
            [&a],
        )?;
        apply(&db, api, &a)?;
        let b = seed(&db, "independent", api)?;
        apply(&db, api, &b)?;
        // Restore a historical collision only AFTER two distinct actual settlements.
        let mut conn = db.conn()?;
        let tx = conn.transaction()?;
        for table in [
            "orders",
            "execution_canary_receipt_proofs",
            "execution_canary_receipt_facts",
        ] {
            tx.execute(
                &format!("UPDATE {table} SET tx_signature='shared-signature' WHERE order_id=?1"),
                [&b],
            )?;
        }
        tx.commit()?;
        db.reopen()?;
        let before = snapshot(&db)?;
        for id in [&a, &b] {
            assert_eq!(
                apply(&db, api, id)?.close_status,
                EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION
            );
        }
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}
