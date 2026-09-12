#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

// Batch 16 intentionally replaces the historical Batch 15 gap witness with a cap regression.
#[test]
fn actual_partial_cash_loss_reaches_entry_cap_while_original_closed_floor_stays_zero() -> Result<()>
{
    let db = Db::new(3, 9, 0, 1, -4)?;
    let cash = db
        .store
        .apply_execution_canary_sell_settlement(&db.facts(1, -4), db.now)?
        .settlement;
    assert_eq!(cash.allocated_entry_basis.as_u64(), 3);
    assert_eq!(cash.cash_result_delta.as_i128(), -7);
    assert_eq!(cash.remaining_quantity.raw(), 2);
    let position = db
        .store
        .load_execution_canary_open_position("mint")?
        .unwrap();
    assert_eq!(position.qty_exact.unwrap().raw(), 2);
    let guard = db
        .store
        .execution_canary_entry_cost(db.now + Duration::seconds(1))?;
    assert_eq!(guard.closed_loss.positions, 0);
    assert_eq!(guard.closed_loss.loss_lamports, "0");
    assert_eq!(guard.known_total_lamports.as_deref(), Some("7"));
    assert!(guard.check_cap(0.000000007)?.exhausted);
    eprintln!("B16: atomic partial SELL delta=-7 basis=3 remaining_raw=2 position=OPEN; original CLOSED floor=0, additional cash=7, cap_7_exhausted=true");
    Ok(())
}
