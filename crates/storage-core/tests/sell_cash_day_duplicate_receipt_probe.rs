#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
#[path = "common/sell_receipt_history.rs"]
mod history;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

fn two_claims(duplicate: bool, cross_day: bool) -> Result<Db> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    let at = as_of() - Duration::seconds(2);
    first(
        &db,
        if cross_day {
            at - Duration::days(1)
        } else {
            at
        },
    )?;
    let mut fresh = db.facts(1, 0);
    fresh.order_id = "exec-canary:second-claim".into();
    if !duplicate {
        fresh.tx_signature = "distinct-onchain-transaction".into();
    }
    if duplicate {
        // Historical conflict is introduced only after independent API settlements.
        history::settle(&db, fresh, "tiny", at + Duration::seconds(1))?;
    } else {
        let fresh = prepare_identity(&db, fresh, "tiny")?;
        db.store
            .apply_execution_canary_sell_settlement(&fresh, at + Duration::seconds(1))?;
    }
    let position = db
        .store
        .load_execution_canary_open_position("mint")?
        .unwrap();
    assert_eq!(position.qty_exact.unwrap().raw(), 1);
    Ok(db)
}

#[test]
fn root_distinct_receipts_remain_two_exact_events() -> Result<()> {
    let db = two_claims(false, false)?;
    let v = db.store.execution_canary_sell_cash_day(as_of())?;
    assert_eq!(v.known_events.events, 2);
    assert_eq!(v.known_events.signed_net_cash_result_lamports, "-6");
    assert_eq!(v.known_events.gross_negative_cash_result_lamports, "6");
    Ok(())
}

#[test]
fn root_same_receipt_cannot_be_two_validated_cash_events() -> Result<()> {
    let db = two_claims(true, false)?;
    let before = snapshot(&db.conn()?)?;
    let result = db.store.execution_canary_sell_cash_day(as_of());
    assert_eq!(snapshot(&db.conn()?)?, before);
    assert!(
        result.is_err(),
        "same wallet/signature/token receipt claimed twice: {result:?}"
    );
    Ok(())
}

#[test]
fn root_prior_day_duplicate_cannot_validate_todays_second_claim() -> Result<()> {
    let db = two_claims(true, true)?;
    let result = db.store.execution_canary_sell_cash_day(as_of());
    assert!(
        result.is_err(),
        "prior-day duplicate identity silently accepted: {result:?}"
    );
    Ok(())
}
