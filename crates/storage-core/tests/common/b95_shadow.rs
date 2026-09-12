#![allow(dead_code)]
#[path = "association_parent_fixture.rs"]
pub mod base;
use anyhow::Result;
pub use base::*;
use copybot_core_types::{
    association_delivery::AdmissionFacts, CopySignalRow, Lamports, SwapEvent, TokenQuantity,
};
use copybot_storage_core::association_sell_shadow_types::*;

pub fn swap(f: &F, a: &AdmissionFacts) -> SwapEvent {
    let x = &a.facts;
    SwapEvent {
        wallet: x.wallet.clone(),
        dex: x.dex.clone(),
        token_in: x.token_in.clone(),
        token_out: x.token_out.clone(),
        amount_in: f64::from_bits(x.amount_in_bits),
        amount_out: f64::from_bits(x.amount_out_bits),
        signature: x.signature.clone(),
        slot: x.slot,
        ts_utc: f.db.now,
        exact_amounts: x.exact_amounts.clone(),
    }
}
pub fn insert(f: &F, a: &AdmissionFacts) -> Result<i64> {
    let s = swap(f, a);
    let id = format!("shadow:{}:{}:buy:{}", s.signature, s.wallet, s.token_out);
    f.db.store.insert_copy_signal(&CopySignalRow {
        signal_id: id.clone(),
        wallet_id: s.wallet.clone(),
        token: s.token_out.clone(),
        side: "buy".into(),
        notional_sol: 0.000001,
        notional_lamports: Some(Lamports::new(1000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: s.ts_utc,
        status: "shadow_recorded".into(),
    })?;
    f.db.store
        .insert_shadow_buy_lot(&s, &id, 3.5, Some(TokenQuantity::new(3500, 3)), 0.000001)
}
pub fn lot(f: &mut F, signature: &str, slot: u64, hash: &str, index: u64) -> Result<i64> {
    let mut a = facts(signature, "leader", true);
    a.facts.slot = slot;
    f.admit(a.clone())?;
    f.terminal(&a, index, slot, hash)?;
    insert(f, &a)
}
pub fn evidence(f: &F) -> Result<ShadowEvidence> {
    let p = f.read()?;
    if let Ok(dir) = std::env::var("B95_OBSERVATIONS") {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("unnamed");
        let output = std::path::Path::new(&dir).join(format!("{name}-{n}.json"));
        std::fs::write(
            output,
            serde_json::to_vec_pretty(&serde_json::json!({
            "actual_first":p.first,"actual_current":p.current,"historical_latest":p.historical_latest,
            "fixture_only":true,"trade_authority":"trade_authority_none"}))?,
        )?;
    }
    Ok(p.current.shadow.unwrap())
}
pub fn within() -> Result<F> {
    let mut f = F::new()?;
    f.anchors()?;
    f.sell()?;
    f.drain()?;
    Ok(f)
}
pub fn reopen(f: &mut F) -> Result<()> {
    f.db.reopen()?;
    f.inbox =
        copybot_storage_core::association_inbox::AssociationInbox::open(&f.db.path, limits())?;
    Ok(())
}
