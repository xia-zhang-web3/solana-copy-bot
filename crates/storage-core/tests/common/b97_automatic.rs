#![allow(dead_code)]
#[path = "b96_ordered.rs"]
mod old;
use anyhow::Result;
pub use copybot_core_types::association_delivery::*;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
pub use old::*;

pub fn automatic(f: &mut F) -> Result<()> {
    f.inbox = AssociationInbox::open_ordered_sell_consumer(&f.db.path, limits())?;
    Ok(())
}
pub fn new() -> Result<F> {
    let mut f = F::new()?;
    automatic(&mut f)?;
    Ok(f)
}
pub fn intent(f: &F, sig: &str) -> Result<Option<OrderedSourceSellIntent>> {
    f.inbox
        .load_ordered_source_sell_intent_history(&format!("source-sell:{sig}"))
}
pub fn pair(f: &F, n: i64) -> Result<()> {
    assert_eq!(count(f, "ordered_source_sell_intents")?, n);
    assert_eq!(count(f, "source_sell_signature_claims")?, n);
    Ok(())
}
pub fn snapshot(c: &rusqlite::Connection, tables: &[&str]) -> Result<Vec<String>> {
    let mut result = vec![];
    for table in tables {
        let mut q = c.prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
        let n = q.column_count();
        for row in q.query_map([], |r| {
            (0..n)
                .map(|i| r.get::<_, rusqlite::types::Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })? {
            result.push(format!("{table}:{:?}", row?));
        }
    }
    Ok(result)
}
pub fn protocol(f: &F) -> Result<Vec<String>> {
    snapshot(
        &f.db.conn()?,
        &[
            "association_inbox_identities",
            "association_inbox_events",
            "association_sell_preparations",
            "association_sell_dependencies",
            "association_sell_work",
            "association_sell_bootstrap",
            "association_parent_work",
            "association_parent_dependencies",
            "ordered_source_sell_intents",
            "source_sell_signature_claims",
        ],
    )
}
pub fn money(f: &F) -> Result<Vec<String>> {
    snapshot(
        &f.db.conn()?,
        &[
            "observed_swaps",
            "copy_signals",
            "orders",
            "fills",
            "positions",
            "execution_canary_receipt_proofs",
            "execution_canary_receipt_facts",
            "shadow_lots",
            "shadow_lot_origins",
            "execution_source_sell_intents",
            "execution_source_sell_promotions",
        ],
    )
}
pub fn pulse(seq: u64) -> Delivery {
    Delivery {
        session: "budget".into(),
        sequence: seq,
        arrival_offset_ns: seq,
        event: DeliveryEvent::Session(SessionGap::Reset),
    }
}
pub fn exhaust(i: &mut AssociationInbox) -> Result<usize> {
    let mut n = 0;
    while i.has_sell_preparation_work()? {
        i.recover_sell_preparation()?;
        n += 1;
        assert!(n < 1000, "unstaged rows must not create busy loops");
    }
    Ok(n)
}
pub fn open(f: &F, l: InboxLimits) -> Result<AssociationInbox> {
    AssociationInbox::open_ordered_sell_consumer(&f.db.path, l)
}
