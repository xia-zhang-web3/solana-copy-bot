#[path = "common/legacy_sell_ownership_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn both_legacy_wrappers_block_second_partial_full_and_no_position_marker() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        for lots in [3, 2, 1] {
            let mut db = database(lots)?;
            let a = seed(&db, "dup-a", api)?;
            let first = apply(&db, api, &a)?;
            assert_eq!(first.closed_qty_exact.unwrap().raw(), 7000);
            let b = seed(&db, "dup-b", api)?;
            let before = snapshot(&db)?;
            db.reopen()?;
            rejected(&db, api, &b)?;
            assert_eq!(snapshot(&db)?, before);
            if matches!(api, Api::Confirm) {
                assert!(db.store.execution_canary_token_accounting_pending("mint")?);
            }
        }
    }
    Ok(())
}

#[test]
fn closed_reopen_and_new_generation_do_not_release_the_receipt() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        let mut db = database(1)?;
        let a = seed(&db, "dup-a", api)?;
        assert_eq!(
            apply(&db, api, &a)?.close_status,
            EXECUTION_CANARY_POSITION_CLOSE_CLOSED
        );
        db.reopen()?;
        let buy = db.seed("new-generation", "leader", "buy")?;
        db.buy(&buy)?;
        let b = seed(&db, "dup-b", api)?;
        rejected(&db, api, &b)?;
        let before = snapshot(&db)?;
        assert_eq!(
            apply(&db, api, &a)?.close_status,
            EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION
        );
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn prior_native_cash_or_buy_receipt_claim_blocks_legacy_sell() -> Result<()> {
    for kind in ["native-sell", "buy"] {
        for api in [Api::Confirm, Api::Confirmed] {
            let db = database(3)?;
            let a = db.seed(
                "dup-prior",
                "leader",
                if kind == "buy" { "buy" } else { "sell" },
            )?;
            if kind == "buy" {
                db.buy(&a)?;
            } else {
                let facts = db.store.load_execution_canary_receipt_facts(&a)?.unwrap();
                db.store
                    .apply_execution_canary_sell_settlement(&facts, db.now)?;
            }
            let b = seed(&db, "dup-legacy", api)?;
            rejected(&db, api, &b)?;
        }
    }
    Ok(())
}

#[test]
fn distinct_receipts_wallets_and_same_order_replay_remain_healthy() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        for other_wallet in [false, true] {
            let mut db = database(3)?;
            let a = seed(&db, "dup-a", api)?;
            apply(&db, api, &a)?;
            let b = db.seed_claim(
                "other",
                "other-leader",
                "sell",
                "mint",
                if other_wallet {
                    "other-wallet"
                } else {
                    "execution-wallet"
                },
                if other_wallet {
                    "shared-signature"
                } else {
                    "other-signature"
                },
            )?;
            if matches!(api, Api::Confirmed) {
                import_confirmed(&db, &b)?;
            }
            apply(&db, api, &b)?;
            assert_eq!(
                db.store
                    .load_execution_canary_open_position("mint")?
                    .unwrap()
                    .qty_exact
                    .unwrap()
                    .raw(),
                7000
            );
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
    }
    Ok(())
}

#[test]
fn no_receipt_imports_and_no_order_writeoff_ignore_foreign_receipt_claims() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        let db = database(3)?;
        seed(&db, "dup-foreign", Api::Confirm)?;
        let a = legacy_order(&db, "legacy-a", api)?;
        let b = legacy_order(&db, "legacy-b", api)?;
        apply(&db, api, &a)?;
        apply(&db, api, &b)?;
        let fills: i64 = db
            .conn()?
            .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))?;
        let out = db.store.close_execution_canary_open_position(
            "mint",
            7.0,
            Some(copybot_core_types::TokenQuantity::new(7000, 3)),
            0.0,
            0.0,
            db.now,
        )?;
        assert_eq!(out.close_status, EXECUTION_CANARY_POSITION_CLOSE_CLOSED);
        assert_eq!(
            db.conn()?
                .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get::<_, i64>(0))?,
            fills
        );
    }
    Ok(())
}

#[test]
fn confirmed_wrapper_duplicate_no_position_cannot_add_a_fill() -> Result<()> {
    let mut db = database(1)?;
    let a = seed(&db, "dup-a", Api::Confirmed)?;
    assert_eq!(
        apply(&db, Api::Confirmed, &a)?.close_status,
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED
    );
    let b = seed(&db, "dup-b", Api::Confirmed)?;
    db.reopen()?;
    rejected(&db, Api::Confirmed, &b)?;
    Ok(())
}
