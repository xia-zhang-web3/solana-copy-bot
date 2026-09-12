#![allow(dead_code)]
#[path = "b96_ordered.rs"]
mod old;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::{CopySignalRow, Lamports, SwapEvent};
use copybot_storage_core::{HistoryRetentionCutoffs, HistoryRetentionSummary};
pub use old::*;
use rusqlite::OptionalExtension;

pub const SIGNAL: &str = "shadow:sell:leader:sell:mint";
pub fn ready() -> Result<(F, SwapEvent)> {
    let f = within()?;
    let swap = observed(&f)?;
    f.db.store
        .activate_follow_wallet("leader", f.db.now - Duration::seconds(20), "r1")?;
    Ok((f, swap))
}
pub fn signal(f: &F, id: &str, side: &str) -> CopySignalRow {
    CopySignalRow {
        signal_id: id.into(),
        wallet_id: "leader".into(),
        token: "mint".into(),
        side: side.into(),
        notional_sol: 0.00000095,
        notional_lamports: Some(Lamports::new(950)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: f.db.now,
        status: "execution_failed".into(),
    }
}
pub fn owner(f: &F, sig: &str) -> Result<Option<String>> {
    Ok(f.db
        .conn()?
        .query_row(
            "SELECT owner FROM source_sell_signature_claims WHERE signature=?1",
            [sig],
            |r| r.get(0),
        )
        .optional()?)
}
pub fn retain(f: &F) -> Result<HistoryRetentionSummary> {
    let old = f.db.now - Duration::days(1);
    Ok(f.db.store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: old,
            copy_signals_before: f.db.now + Duration::seconds(1),
            orders_before: old,
            shadow_closed_trades_before: old,
            execution_quote_canary_before: old,
        },
        true,
    )?)
}
pub fn blocked(f: &mut F) -> Result<()> {
    assert_eq!(
        stage(f)?,
        OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed)
    );
    assert_eq!(count(f, "ordered_source_sell_intents")?, 0);
    Ok(())
}
/// Work on a new copy of the hash-bound database produced by accepted95 at 0071.
/// Its no-shadow historical JSON arm is synthetic compatibility data.
pub fn pre0072() -> Result<F> {
    let mut f = within()?;
    f.db.path = f.db.dir.path().join("copied-0071.sqlite");
    std::fs::copy(std::env::var("B96_0071_DB")?, &f.db.path)?;
    reopen(&mut f)?;
    Ok(f)
}
pub fn upgrade(f: &mut F) -> Result<()> {
    let before = f.db.snapshot()?;
    let first = f.read()?.first;
    let migrations = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    assert_eq!(f.db.store.run_migrations(migrations)?, 1);
    reopen(f)?;
    assert_eq!(f.db.store.run_migrations(migrations)?, 0);
    assert_eq!(f.db.snapshot()?, before);
    assert_eq!(f.read()?.first, first);
    assert_eq!(
        count(f, "source_sell_signature_claims")?,
        0,
        "no history backfill"
    );
    Ok(())
}
