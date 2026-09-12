#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{association_sell_preparation::*, EXECUTION_STATUS_CANARY_CONFIRMED};
#[test]
fn b95_pending_to_applied_changes_current_without_rebinding_first() -> Result<()> {
    let mut f = f::F::new()?;
    f.anchors()?;
    f.sell()?;
    assert!(f.read()?.current.pending_buys.is_empty());
    let first = f.read()?.first;
    let order = f.db.seed("shadow:pending:other:buy:mint", "other", "buy")?;
    assert_eq!(f.read()?.current.pending_buys.len(), 1);
    f.db.buy(&order)?;
    let p = f.read()?;
    assert!(p.current.pending_buys.is_empty());
    assert_eq!(p.first, first);
    assert_eq!(
        p.current.selected_chain,
        Check::Blocked(Reason::FinancialSetChanged)
    );
    Ok(())
}
#[test]
fn b95_confirmed_without_fill_nonconfirmed_and_orphan_signal_stay_pending() -> Result<()> {
    for case in 0..4 {
        let mut f = f::F::new()?;
        f.anchors()?;
        f.sell()?;
        let c = f.db.conn()?;
        match case {
            0 => {
                c.execute("DELETE FROM fills", [])?;
            }
            1 => {
                c.execute("UPDATE orders SET status='execution_canary_submitted'", [])?;
            }
            2 => {
                c.execute_batch("PRAGMA foreign_keys=OFF; DELETE FROM copy_signals;")?;
            }
            _ => {
                c.execute_batch(
                    "PRAGMA foreign_keys=OFF; DELETE FROM copy_signals; DELETE FROM fills;",
                )?;
            }
        }
        if case != 1 {
            assert_eq!(
                c.query_row("SELECT status FROM orders", [], |r| r.get::<_, String>(0))?,
                EXECUTION_STATUS_CANARY_CONFIRMED
            );
        }
        assert_eq!(f.read()?.current.pending_buys.len(), 1, "case={case}");
    }
    Ok(())
}
