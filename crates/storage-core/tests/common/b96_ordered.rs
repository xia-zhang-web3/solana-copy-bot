#![allow(dead_code)]
#[path = "b95_shadow.rs"]
mod old;
use anyhow::Result;
use copybot_core_types::SwapEvent;
pub use copybot_storage_core::ordered_source_sell::*;
use copybot_storage_core::ExecutionSourceSellOutcome;
pub use old::*;

pub fn stage(f: &mut F) -> Result<OrderedSellStage> {
    f.inbox
        .stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)
}
pub fn inserted(f: &mut F) -> Result<OrderedSourceSellIntent> {
    let actual = stage(f)?;
    record("insert", &actual)?;
    let OrderedSellStage::Inserted(i) = actual else {
        panic!("expected actual Inserted: {actual:?}")
    };
    Ok(*i)
}
pub fn record(label: &str, actual: &impl serde::Serialize) -> Result<()> {
    if let Ok(dir) = std::env::var("B96_OBSERVATIONS") {
        let t = std::thread::current();
        let path = std::path::Path::new(&dir)
            .join(format!("{}-{label}.json", t.name().unwrap_or("unnamed")));
        std::fs::write(path, serde_json::to_vec_pretty(actual)?)?;
    }
    Ok(())
}
pub fn refused(f: &mut F) -> Result<OrderedSellStage> {
    let actual = stage(f)?;
    record("refused", &actual)?;
    assert!(
        matches!(
            actual,
            OrderedSellStage::Blocked(_) | OrderedSellStage::Unknown(_)
        ),
        "{actual:?}"
    );
    Ok(actual)
}
pub fn fresh(f: &F) -> Result<OrderedSellDecision> {
    f.inbox
        .revalidate_ordered_source_sell_intent("source-sell:sell")
}
pub fn count(f: &F, table: &str) -> Result<i64> {
    Ok(f.db
        .conn()?
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}
pub fn no_intent(f: &F) -> Result<()> {
    assert_eq!(count(f, "ordered_source_sell_intents")?, 0);
    assert_eq!(count(f, "source_sell_signature_claims")?, 0);
    Ok(())
}
pub fn observed(f: &F) -> Result<SwapEvent> {
    let mut a = facts("sell", "leader", false);
    a.facts.amount_in_bits = 7.0f64.to_bits();
    a.facts.amount_out_bits = 0.00000095f64.to_bits();
    let s = swap(f, &a);
    assert!(f.db.store.insert_observed_swap(&s)?);
    Ok(s)
}
pub fn legacy(f: &F, s: &SwapEvent) -> Result<ExecutionSourceSellOutcome> {
    let id =
        f.db.store
            .load_execution_canary_open_position("mint")?
            .unwrap()
            .position_id;
    f.db.store.stage_execution_source_sell_intent(s, &id)
}

/// Deliberately corrupt data while restoring the exact production schema before
/// invoking the public API. This is a fixture attack, never a production repair.
pub fn corrupt_first(f: &F, sql: &str) -> Result<()> {
    let c = f.db.conn()?;
    let ddl: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE name='association_sell_first_immutable'",
        [],
        |r| r.get(0),
    )?;
    c.execute_batch("DROP TRIGGER association_sell_first_immutable")?;
    c.execute_batch(sql)?;
    c.execute_batch(&ddl)?;
    Ok(())
}
