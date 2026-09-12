#![allow(dead_code)]
#[path = "source_sell_fixture.rs"]
mod staged;
use anyhow::Result;
use copybot_core_types::CopySignalRow;
pub use copybot_storage_core::{
    ExecutionSourceSellPromotion as Promotion, ExecutionSourceSellPromotionOutcome as Outcome,
    ExecutionSourceSellPromotionReject as Reject,
};
pub use staged::*;

pub const MARKER: &str = "execution_source_sell_promotions";
pub fn prepare(
    db: &Db,
    signature: &str,
    source: &str,
) -> Result<copybot_storage_core::ExecutionSourceSellIntent> {
    let event = db.observed(signature, source)?;
    Ok(staged::inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &db.position()?)?,
    ))
}
pub fn promoted(outcome: Outcome) -> Promotion {
    let Outcome::Inserted(row) = outcome else {
        panic!("expected promotion: {outcome:?}")
    };
    row
}
pub fn signal(db: &Db, row: &Promotion) -> Result<CopySignalRow> {
    Ok(db
        .store
        .load_copy_signal_by_signal_id(&row.signal_id)?
        .unwrap())
}
pub fn rejected(db: &Db, id: &str, reason: Reject) -> Result<()> {
    let before = snapshot(&db.conn()?, &[])?;
    let outcome = db.store.promote_execution_source_sell_intent(id)?;
    assert!(
        matches!(outcome, Outcome::Rejected(r) if r == reason),
        "{outcome:?}"
    );
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}
