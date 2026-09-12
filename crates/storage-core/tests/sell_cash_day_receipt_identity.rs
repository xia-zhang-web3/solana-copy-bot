#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
#[path = "common/sell_receipt_history.rs"]
mod history;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{SqliteStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET};
use fixture::*;
use rusqlite::params;

fn rejected_without_writes_after_reopen(db: &Db) -> Result<()> {
    let before = snapshot(&db.conn()?)?;
    for _ in 0..2 {
        let store = SqliteStore::open_read_only(&db.path)?;
        let error = store.execution_canary_sell_cash_day(as_of()).unwrap_err();
        assert!(
            error.to_string().contains("duplicate wallet cash receipt"),
            "unexpected rejection: {error:#}"
        );
        assert_eq!(snapshot(&db.conn()?)?, before);
    }
    Ok(())
}

fn assert_two_actual_cash_claims(db: &Db) -> Result<()> {
    assert_eq!(
        db.conn()?.query_row(
            "SELECT COUNT(*) FROM fills WHERE accounting_basis='receipt_native_cash'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        2
    );
    // Supported historical cash fills need not have native observation bundles.
    assert_eq!(
        db.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_receipt_native_observations",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    Ok(())
}

#[test]
fn mint_slot_position_and_route_cannot_split_one_wallet_receipt() -> Result<()> {
    for variant in ["mint", "slot", "position", "route", "all"] {
        // Close the original lot before creating another lot of the same mint.
        let db = if variant == "position" {
            Db::new(1, 3, 0, 1, 0)?
        } else {
            Db::new(3, 9, 0, 1, 0)?
        };
        let at = as_of() - Duration::seconds(2);
        let initial = first(&db, at)?;
        let mut fresh = db.facts(1, 0);
        fresh.order_id = "exec-canary:metadata-duplicate".into();
        if matches!(variant, "mint" | "all") {
            fresh.token = "other-mint".into();
        }
        if matches!(variant, "slot" | "all") {
            fresh.slot += 1;
        }
        if matches!(variant, "mint" | "position" | "all") {
            // Imported exact inventory only; receipt/fill/completion use the writer.
            db.conn()?.execute(
                "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
                 accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
                 VALUES('other-lot',?1,0.001,0.000000003,?2,'open',?3,'1',3,3,0)",
                params![
                    fresh.token,
                    db.now.to_rfc3339(),
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET
                ],
            )?;
        }
        let route = if matches!(variant, "route" | "all") {
            "other-route"
        } else {
            "tiny"
        };
        let second = history::settle(&db, fresh, route, at + Duration::seconds(1))?;
        assert!(!second.already_accounted, "{variant}");
        assert_eq!(initial.cash_result_delta.as_i128(), -3);
        assert_eq!(second.settlement.cash_result_delta.as_i128(), -3);
        if matches!(variant, "mint" | "position" | "all") {
            assert_ne!(initial.position_id, second.settlement.position_id);
        }
        assert_two_actual_cash_claims(&db)?;
        rejected_without_writes_after_reopen(&db)?;
    }
    Ok(())
}

#[test]
fn past_future_and_empty_day_windows_do_not_hide_conflicts() -> Result<()> {
    let since = time("2026-09-06T00:00:00Z");
    for (a, b) in [
        (since - Duration::nanoseconds(1), since),
        (as_of() - Duration::seconds(1), as_of()),
        (
            as_of() - Duration::seconds(1),
            as_of() + Duration::nanoseconds(1),
        ),
        (as_of() - Duration::seconds(1), as_of() + Duration::days(1)),
        (since - Duration::days(2), since - Duration::days(1)),
        (as_of() + Duration::days(1), as_of() + Duration::days(2)),
    ] {
        let db = Db::new(3, 9, 0, 1, 0)?;
        first(&db, a)?;
        let mut fresh = db.facts(1, 0);
        fresh.order_id = "exec-canary:outside-window".into();
        history::settle(&db, fresh, "tiny", b)?;
        assert_two_actual_cash_claims(&db)?;
        rejected_without_writes_after_reopen(&db)?;
    }
    Ok(())
}

#[test]
fn same_signature_for_different_wallet_balances_remains_two_events() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    let at = as_of() - Duration::seconds(1);
    first(&db, at)?;
    let mut fresh = db.facts(1, 0);
    fresh.order_id = "exec-canary:other-wallet".into();
    fresh.wallet_pubkey = "other-wallet".into();
    let fresh = prepare_identity(&db, fresh, "tiny")?;
    db.store
        .apply_execution_canary_sell_settlement(&fresh, at)?;
    assert_two_actual_cash_claims(&db)?;
    sums(&db, as_of(), 2, -6, 6)?;
    Ok(())
}

#[test]
fn same_order_replay_keeps_one_event_and_original_accounting_timestamp() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    let at = as_of() - Duration::seconds(1);
    first(&db, at)?;
    let before = snapshot(&db.conn()?)?;
    let fresh = db
        .store
        .load_execution_canary_receipt_facts(ORDER)?
        .unwrap();
    let store = SqliteStore::open(&db.path)?;
    let replay =
        store.apply_execution_canary_sell_settlement(&fresh, as_of() + Duration::days(1))?;
    assert!(replay.already_accounted);
    assert_eq!(snapshot(&db.conn()?)?, before);
    let timestamp: String = db.conn()?.query_row(
        "SELECT settlement_ts FROM fills WHERE order_id=?1",
        [ORDER],
        |r| r.get(0),
    )?;
    assert_eq!(time(&timestamp), at);
    sums(&db, as_of(), 1, -3, 3)?;
    Ok(())
}

#[test]
fn noncanary_claim_of_same_receipt_stays_outside_selected_ledger() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    let at = as_of() - Duration::seconds(1);
    first(&db, at)?;
    let mut fresh = db.facts(1, 0);
    fresh.order_id = "other:duplicate".into();
    history::settle(&db, fresh, "tiny", at)?;
    assert_two_actual_cash_claims(&db)?;
    sums(&db, as_of(), 1, -3, 3)?;
    Ok(())
}

#[test]
fn pending_and_legacy_receipts_are_not_native_cash_claims() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    first(&db, as_of() - Duration::seconds(1))?;
    let mut fresh = db.facts(1, 0);
    fresh.order_id = "exec-canary:undated-duplicate".into();
    let fresh = prepare_identity(&db, fresh, "tiny")?;
    let pending = sums(&db, as_of(), 1, -3, 3)?;
    assert_eq!(
        pending
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        1
    );
    // Imported legacy fixture, not a fabricated successful native-cash settlement.
    legacy_fill(&db, &fresh.order_id)?;
    let legacy = sums(&db, as_of(), 1, -3, 3)?;
    assert_eq!(legacy.undated_obligations.legacy_fill_orders, 1);
    assert_eq!(
        legacy
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        0
    );
    Ok(())
}
