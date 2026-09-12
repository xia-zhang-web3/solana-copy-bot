#[path = "common/buy_attribution_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::*;
use fixture::Db;

#[test]
fn refused_attribution_rolls_back_insert_merge_and_confirmation() -> Result<()> {
    for merged in [false, true] {
        for refusal in ["ABORT", "IGNORE"] {
            let db = Db::new()?;
            if merged {
                let a = db.seed("a", "source-a", "buy")?;
                db.buy(&a)?;
            }
            let b = db.seed("b", "source-b", "buy")?;
            let raise = if refusal == "ABORT" {
                "RAISE(ABORT,'attribution refused')"
            } else {
                "RAISE(IGNORE)"
            };
            db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER refuse_link BEFORE INSERT ON fills
                WHEN NEW.position_id IS NOT NULL BEGIN SELECT {raise}; END;"
            ))?;
            let before = db.snapshot()?;
            assert!(db.buy(&b).is_err(), "{merged}/{refusal}");
            assert_eq!(db.snapshot()?, before);
            assert!(!db.store.execution_canary_fill_exists(&b)?);
            assert_eq!(
                db.store.load_execution_canary_order(&b)?.unwrap().status,
                EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
            );
        }
    }
    Ok(())
}

#[test]
fn inconsistent_links_are_explicit_and_never_rebound_on_replay() -> Result<()> {
    for (sql, reason) in [
        (
            "UPDATE fills SET position_id='absent'",
            BuyAttributionIssue::DanglingDestination,
        ),
        (
            "UPDATE positions SET token='wrong'",
            BuyAttributionIssue::PositionConflict,
        ),
        (
            "UPDATE fills SET token='wrong'",
            BuyAttributionIssue::IdentityConflict,
        ),
        (
            "UPDATE copy_signals SET token='wrong'",
            BuyAttributionIssue::IdentityConflict,
        ),
        (
            "UPDATE copy_signals SET side='sell'",
            BuyAttributionIssue::IdentityConflict,
        ),
        (
            "UPDATE copy_signals SET wallet_id=' '",
            BuyAttributionIssue::MissingSourceWallet,
        ),
        (
            "UPDATE orders SET signal_id='absent'",
            BuyAttributionIssue::MissingSignal,
        ),
        ("DELETE FROM orders", BuyAttributionIssue::MissingOrder),
        (
            "UPDATE execution_canary_receipt_proofs SET tx_signature='wrong'",
            BuyAttributionIssue::ReceiptIdentityConflict,
        ),
    ] {
        let db = Db::new()?;
        let a = db.seed("a", "source-a", "buy")?;
        db.buy(&a)?;
        db.conn()?
            .execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        let before = db.snapshot()?;
        // Exercise the lowest public writer too; the confirmed wrapper cannot mask bypasses.
        assert!(
            db.store
                .record_execution_canary_open_position(
                    &a,
                    "mint",
                    7.0,
                    Some(TokenQuantity::new(7000, 3)),
                    0.000001,
                    db.now
                )
                .is_err(),
            "{sql}"
        );
        let token = if reason == BuyAttributionIssue::PositionConflict {
            "wrong"
        } else {
            "mint"
        };
        let ExecutionCanaryBuyAttribution::Open(value) =
            db.store.load_execution_canary_buy_attribution(token)?
        else {
            panic!("{sql}")
        };
        assert_eq!(value.coverage, BuyAttributionCoverage::Unknown, "{sql}");
        assert!(value.proven_contributors.is_empty(), "{sql}");
        // Position-token corruption is also a fill-token conflict for the selected position.
        let expected = if reason == BuyAttributionIssue::PositionConflict {
            BuyAttributionIssue::IdentityConflict
        } else {
            reason
        };
        assert!(
            value.unproven_links.iter().any(|v| v.reason == expected),
            "{sql}: {value:?}"
        );
        assert_eq!(db.snapshot()?, before);
    }
    Ok(())
}

#[test]
fn conflicting_first_buy_chain_cannot_leave_money_or_link() -> Result<()> {
    for sql in [
        "UPDATE copy_signals SET token='wrong' WHERE signal_id='b'",
        "UPDATE copy_signals SET side='sell' WHERE signal_id='b'",
        "UPDATE copy_signals SET wallet_id='' WHERE signal_id='b'",
        "UPDATE orders SET signal_id='absent' WHERE order_id='exec-canary:b'",
        "UPDATE execution_canary_receipt_proofs SET tx_signature='wrong' WHERE order_id='exec-canary:b'",
    ] {
        let db = Db::new()?;
        let a = db.seed("a", "source-a", "buy")?;
        db.buy(&a)?;
        let b = db.seed("b", "source-b", "buy")?;
        db.conn()?.execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        let before = db.snapshot()?;
        assert!(db.store.record_execution_canary_open_position(&b, "mint", 7.0,
            Some(TokenQuantity::new(7000, 3)), 0.000001, db.now).is_err(), "{sql}");
        assert_eq!(db.snapshot()?, before);
    }
    Ok(())
}

#[test]
fn missing_fill_cannot_be_reconstructed_from_position_name() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    db.buy(&a)?;
    db.conn()?.execute("DELETE FROM fills", [])?;
    let before = db.snapshot()?;
    assert!(db.buy(&a).is_err());
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn later_completion_failure_rolls_back_the_new_link_too() -> Result<()> {
    for merged in [false, true] {
        let db = Db::new()?;
        if merged {
            let a = db.seed("a", "source-a", "buy")?;
            db.buy(&a)?;
        }
        let b = db.seed("b", "source-b", "buy")?;
        db.conn()?.execute_batch(
            "CREATE TRIGGER refuse_completion AFTER UPDATE ON orders
            WHEN NEW.order_id='exec-canary:b' AND NEW.status='execution_canary_confirmed'
            BEGIN SELECT RAISE(ABORT,'completion refused'); END;",
        )?;
        let before = db.snapshot()?;
        assert!(db.buy(&b).is_err());
        assert_eq!(db.snapshot()?, before);
        assert!(!db.store.execution_canary_fill_exists(&b)?);
    }
    Ok(())
}

#[test]
fn unproven_receipts_are_explicit_and_sql_failure_is_not_unknown_success() -> Result<()> {
    for (sql, reason) in [
        (
            "DELETE FROM execution_canary_receipt_facts",
            BuyAttributionIssue::MissingReceiptFacts,
        ),
        (
            "DELETE FROM execution_canary_receipt_proofs",
            BuyAttributionIssue::ReceiptIdentityConflict,
        ),
        (
            "UPDATE orders SET status='execution_canary_submitted'",
            BuyAttributionIssue::NotConfirmed,
        ),
        (
            "UPDATE fills SET qty_raw='7001'",
            BuyAttributionIssue::ReceiptOperandsConflict,
        ),
        (
            "UPDATE fills SET notional_lamports=1001",
            BuyAttributionIssue::ReceiptOperandsConflict,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET wallet_pubkey='source-a'",
            BuyAttributionIssue::ReceiptIdentityConflict,
        ),
    ] {
        let db = Db::new()?;
        let a = db.seed("a", "source-a", "buy")?;
        db.buy(&a)?;
        db.conn()?
            .execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        let before = db.snapshot()?;
        let ExecutionCanaryBuyAttribution::Open(value) =
            db.store.load_execution_canary_buy_attribution("mint")?
        else {
            panic!("expected position")
        };
        assert_eq!(value.coverage, BuyAttributionCoverage::Unknown, "{sql}");
        assert!(value.proven_contributors.is_empty(), "{sql}");
        assert!(
            value.unproven_links.iter().any(|v| v.reason == reason),
            "{sql}"
        );
        assert_eq!(db.snapshot()?, before);
    }
    let db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    db.buy(&a)?;
    db.conn()?
        .execute_batch("ALTER TABLE fills RENAME COLUMN accounting_basis TO unavailable")?;
    assert!(db
        .store
        .load_execution_canary_buy_attribution("mint")
        .is_err());
    Ok(())
}
