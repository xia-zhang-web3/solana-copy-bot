#![allow(dead_code)]
#[path = "b97_automatic.rs"]
mod old;
use anyhow::Result;
pub use copybot_core_types::TokenQuantity;
pub use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
pub use old::*;
pub const WORK: &str = "association_shadow_sell_work";
pub fn missing_lot(f: &F, wallet: &str, mint: &str) -> Result<i64> {
    f.db.store.insert_shadow_lot_exact(
        wallet,
        mint,
        1.0,
        Some(TokenQuantity::new(1000, 3)),
        0.000001,
        f.db.now,
    )
}
pub fn parked() -> Result<F> {
    let mut f = new()?;
    missing_lot(&f, "leader", "mint")?;
    f.anchors()?;
    f.sell()?;
    f.drain()?;
    pair(&f, 0)?;
    assert!(!f.inbox.has_sell_preparation_work()?);
    Ok(f)
}
pub fn close(
    f: &F,
    id: &str,
    wallet: &str,
    mint: &str,
    qty: f64,
    l: InboxLimits,
) -> Result<copybot_storage_core::ShadowCloseOutcome> {
    f.db.store
        .close_shadow_lots_fifo_atomic_exact_with_recovery(
            id,
            wallet,
            mint,
            qty,
            Some(TokenQuantity::new((qty * 1000.0) as u64, 3)),
            0.000001,
            copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
            f.db.now,
            Some(l),
        )
}
pub fn state(f: &F) -> Result<Vec<String>> {
    let mut s = protocol(f)?;
    s.extend(money(f)?);
    s.extend(snapshot(&f.db.conn()?, &["shadow_closed_trades", WORK])?);
    Ok(s)
}
pub fn cursor(f: &F) -> Result<String> {
    Ok(f.db.conn()?.query_row("SELECT after_signature FROM association_shadow_sell_work WHERE wallet='leader' AND token='mint'", [], |r| r.get(0))?)
}
pub fn unrelated(f: &mut F) -> Result<()> {
    for (sig, wallet, mint) in [
        ("other-wallet", "other", "mint"),
        ("other-mint", "leader", "other"),
    ] {
        let mut a = facts(sig, wallet, false);
        a.facts.token_in = mint.into();
        f.event(
            DeliveryEvent::Admission(a.clone()),
            CandidateGeneration::Unknown,
        )?;
        f.terminal(&a, 3, 42, "block")?;
    }
    f.drain()?;
    Ok(())
}
