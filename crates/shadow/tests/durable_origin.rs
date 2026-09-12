#[path = "event_time_support/mod.rs"]
mod f;
use anyhow::Result;
use copybot_shadow::{ShadowDropReason, ShadowProcessOutcome};
use copybot_storage_core::SqliteStore;
use f::*;

#[test]
fn b95_real_shadow_producer_persists_source_raw_separately_from_scaled_lot() -> Result<()> {
    for exact in [true, false] {
        let (dir, mut store, service) = setup(false)?;
        let swap = buy(now(), exact, "durable-source");
        let (outcome, receipt) =
            service.process_swap_with_buy_receipt(&store, &swap, &follow(), now())?;
        recorded(&outcome);
        assert_eq!(receipt.is_some(), exact);
        let lot = store.list_shadow_lots(WALLET, TOKEN)?.remove(0);
        assert_eq!(lot.qty, 50.0);
        assert_eq!(swap.amount_out, 100.0);
        if exact {
            assert_eq!(lot.qty_exact.unwrap().raw(), 50000);
        }
        let origin = store.shadow_lot_origin(lot.id)?.unwrap();
        assert_eq!(origin.signature, swap.signature);
        assert_eq!(origin.slot, 100);
        assert_eq!(origin.amount_out_bits, 100.0f64.to_bits());
        assert_eq!(origin.exact_amounts, swap.exact_amounts);
        store = SqliteStore::open(dir.path().join("event-time.db"))?;
        assert_eq!(store.shadow_lot_origin(lot.id)?.unwrap(), origin);
        if let Some(r) = receipt {
            r.verify(&store, &swap)?;
        }
        let (replay, receipt) =
            service.process_swap_with_buy_receipt(&store, &swap, &follow(), now())?;
        assert!(matches!(
            replay,
            ShadowProcessOutcome::Dropped(ShadowDropReason::DuplicateSignal)
        ));
        assert!(receipt.is_none());
        counts(&store, 1, 1)?;
    }
    Ok(())
}
#[test]
fn b95_real_producer_origin_insert_fault_rolls_back_lot_but_signal_stays_deduped() -> Result<()> {
    for trigger in [
        "CREATE TRIGGER b95_fault BEFORE INSERT ON shadow_lot_origins BEGIN SELECT RAISE(ABORT,'injected'); END;",
        "CREATE TRIGGER b95_fault BEFORE INSERT ON shadow_lot_origins BEGIN SELECT RAISE(IGNORE); END;",
        "CREATE TRIGGER b95_fault AFTER INSERT ON shadow_lot_origins BEGIN DELETE FROM shadow_lots WHERE id=NEW.lot_id; END;",
    ] {
        let (dir,store,service)=setup(false)?;
        // Test-only SQLite fault injection. No new Cargo dependency or runtime hook.
        let status=std::process::Command::new("python3").arg("-c")
            .arg("import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.executescript(sys.argv[2]); c.close()")
            .arg(dir.path().join("event-time.db")).arg(trigger).status()?;
        assert!(status.success());
        let swap=buy(now(),true,"failed-origin");
        let result=service.process_swap_with_buy_receipt(&store,&swap,&follow(),now());
        assert!(result.is_err(),"no successful RecordedBuyLot for rolled-back insert");
        counts(&store,1,0)?;
        assert!(store.shadow_lot_origin(1)?.is_none());
        let reopened=SqliteStore::open(dir.path().join("event-time.db"))?;
        let (outcome,receipt)=service.process_swap_with_buy_receipt(&reopened,&swap,&follow(),now())?;
        reason(&outcome,ShadowDropReason::DuplicateSignal); assert!(receipt.is_none());
        counts(&reopened,1,0)?;
    }
    Ok(())
}
#[test]
fn b95_invalid_source_identity_in_real_producer_has_no_half_lot_or_receipt() -> Result<()> {
    let (_dir, store, service) = setup(false)?;
    let swap = buy(now(), true, "substituted:source");
    assert!(service
        .process_swap_with_buy_receipt(&store, &swap, &follow(), now())
        .is_err());
    counts(&store, 1, 0)?;
    assert!(store.shadow_lot_origin(1)?.is_none());
    Ok(())
}
