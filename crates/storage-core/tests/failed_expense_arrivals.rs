#[path = "common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::{Db, ORDER, ROUTE};

#[test]
fn failed_expense_ready_old_task_recovers_during_continuous_new_arrivals() -> Result<()> {
    let mut db = Db::new()?;
    db.detect(ORDER, "signature_status")?;
    db.store
        .defer_failed_expense(ORDER, "failed_receipt_unavailable")?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        ORDER
    );
    db.store
        .defer_failed_expense(ORDER, "failed_receipt_unavailable")?;
    // A becomes ready now. Every B is attempted at detection, unavailable then,
    // and ready on its next attempt. Arrivals never stop to let A catch up.
    let mut selected = Vec::new();
    for index in 0..4 {
        let id = format!("exec-canary:arrival-{index}");
        db.add(&id, &format!("arrival-signature-{index}"), "sell", db.now)?;
        db.detect(&id, "signature_status")?;
        db.store
            .defer_failed_expense(&id, "failed_receipt_unavailable")?;
        db.reopen()?;
        let tasks = db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?;
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        selected.push(task.order_id.clone());
        let fee = if task.order_id == ORDER { 5000 } else { 7 };
        db.store
            .apply_failed_expense(&task.order_id, &db.facts(&task.order_id, fee)?, db.now)?;
    }
    assert_eq!(
        db.store.load_failed_expense_task(ORDER)?.unwrap().status,
        "complete",
        "ready A must complete while arrivals continue; selected={selected:?}"
    );
    assert_eq!(selected.iter().filter(|id| id.as_str() == ORDER).count(), 1);
    assert_eq!(db.count("execution_failed_expense_ledger")?, 4);
    assert_eq!(db.count("fills")?, 0);
    assert_eq!(db.count("positions")?, 0);
    Ok(())
}
