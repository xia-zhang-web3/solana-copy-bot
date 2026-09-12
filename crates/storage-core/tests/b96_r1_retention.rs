#[path = "common/b96_r1.rs"]
mod f;
use anyhow::Result;
use f::*;

#[test]
fn r1_post0072_orphan_normal_retention_keeps_first_fingerprint_and_owner() -> Result<()> {
    let mut f = within()?;
    let before = f.read()?;
    assert!(f.db.store.insert_copy_signal(&signal(&f, SIGNAL, "sell"))?);
    assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
    blocked(&mut f)?;
    assert_eq!(retain(&f)?.copy_signals_deleted, 1);
    reopen(&mut f)?;
    assert_eq!(f.read()?.first, before.first);
    assert_eq!(f.read()?.current, before.current);
    blocked(&mut f)?;
    assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
    Ok(())
}
#[test]
#[ignore = "requires hash-bound actual accepted95 0071 database"]
fn r1_pre0072_orphan_normal_retention_claims_without_history_backfill() -> Result<()> {
    let mut f = pre0072()?;
    assert!(f.db.store.insert_copy_signal(&signal(&f, SIGNAL, "sell"))?);
    upgrade(&mut f)?;
    let before = f.read()?;
    blocked(&mut f)?;
    assert_eq!(retain(&f)?.copy_signals_deleted, 1);
    reopen(&mut f)?;
    assert_eq!(f.read()?.first, before.first);
    assert_eq!(f.read()?.current, before.current);
    blocked(&mut f)?;
    assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
    Ok(())
}
#[test]
#[ignore = "requires hash-bound actual accepted95 0071 database"]
fn r1_pre0072_orphan_order_retention_and_promotion_delete_keep_claim() -> Result<()> {
    for promotion in [false, true] {
        let mut f = pre0072()?;
        let c = f.db.conn()?;
        c.pragma_update(None, "foreign_keys", false)?;
        if promotion {
            c.execute("INSERT INTO execution_source_sell_promotions VALUES(?1,'source-sell:sell','2026-09-05T12:00:00Z')",[SIGNAL])?;
        } else {
            c.execute("INSERT INTO orders(order_id,signal_id,client_order_id,route,submit_ts,status) VALUES('orphan',?1,'orphan','tiny','2026-09-05T12:00:00Z','execution_failed')",[SIGNAL])?;
        }
        upgrade(&mut f)?;
        if promotion {
            blocked(&mut f)?;
        } else {
            // Orphan orders are conservatively pending in the accepted financial reader.
            use copybot_storage_core::association_sell_preparation::{Check, Reason};
            assert_eq!(
                stage(&mut f)?,
                OrderedSellStage::Blocked(OrderedSellReason::SelectedChain(Check::Blocked(
                    Reason::FinancialSetChanged
                )))
            );
        }
        if promotion {
            // No public promotion retention API; test its actual SQL deletion boundary.
            assert_eq!(
                c.execute(
                    "DELETE FROM execution_source_sell_promotions WHERE signal_id=?1",
                    [SIGNAL]
                )?,
                1
            );
        } else {
            assert_eq!(retain(&f)?.orders_deleted, 1);
        }
        reopen(&mut f)?;
        assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
        blocked(&mut f)?;
    }
    Ok(())
}
#[test]
#[ignore = "requires hash-bound actual accepted95 0071 database"]
fn r1_retention_claim_ignore_abort_roll_back_delete_and_preserve_evidence() -> Result<()> {
    for action in ["IGNORE", "ABORT,'claim fault'"] {
        let mut f = pre0072()?;
        f.db.store.insert_copy_signal(&signal(&f, SIGNAL, "sell"))?;
        upgrade(&mut f)?;
        let before = f.db.snapshot()?;
        f.db.conn()?.execute_batch(&format!("CREATE TRIGGER fault BEFORE INSERT ON source_sell_signature_claims BEGIN SELECT RAISE({action}); END;"))?;
        assert!(retain(&f).is_err());
        assert_eq!(f.db.snapshot()?, before);
        assert_eq!(owner(&f, "sell")?, None);
        reopen(&mut f)?;
        blocked(&mut f)?;
        f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
        assert_eq!(retain(&f)?.copy_signals_deleted, 1);
        blocked(&mut f)?;
        assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
    }
    Ok(())
}
