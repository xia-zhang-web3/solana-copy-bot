//! Production storage/native admission, quote is explicit mock-only and bounded.
use super::corpus::Corpus;
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{
        NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY, SPL_TOKEN_PROGRAM,
    },
    SqliteStore,
};
use std::path::Path;
pub fn authority(db: &mut AssociationInbox, corpus: &Corpus) -> Result<()> {
    db.register_technical_cohort_authority(&TechnicalCohortAuthority {
        run_id: "MOCK-OFFLINE-INGRESS-RELAY".into(),
        wallet_ids: vec![corpus.wallet.clone()],
        mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
        activated_at: corpus.started,
        deadline: corpus.started + chrono::Duration::hours(4),
        max_buy_count: 1,
        policy_identity: "MOCK-offline-unchanged-tiny-limits".into(),
    })
}
pub fn fence(db: &mut AssociationInbox, corpus: &Corpus, session: &str) -> Result<u64> {
    // Independent finite producer clock; never derived from drained ACK/cursor.
    let slot = corpus.latest().saturating_add(4);
    db.record_native_buy_fence_epoch(&NativeBuyFence {
        session: session.into(),
        processed_slot: slot,
        sampled_at: Utc::now(),
        genesis_hash: "MOCK-offline-genesis".into(),
        policy_identity: "MOCK-offline-unchanged-tiny-limits".into(),
    })?;
    Ok(slot)
}
pub fn mock_quote(
    path: &Path,
    corpus: &Corpus,
    signature: &str,
    slot: u64,
) -> Result<serde_json::Value> {
    let store = SqliteStore::open(path)?;
    ensure!(
        store.native_buy_record_finalized(signature, slot, SPL_TOKEN_PROGRAM, Utc::now())?,
        "synthetic source finality binding"
    );
    let candidate = store
        .native_buy_ready(signature, Utc::now(), 120)?
        .ok_or_else(|| anyhow::anyhow!("native ready refused"))?;
    ensure!(
        store.native_buy_recheck(
            &candidate.signal_id,
            &candidate.decision_id,
            Utc::now(),
            120
        )?,
        "native prequote recheck"
    );
    let created = corpus.created(slot);
    let age_ms = (Utc::now() - created).num_milliseconds();
    ensure!((0..120_000).contains(&age_ms), "producer-to-quote age");
    // No production execution route, signer, simulation or submit object exists here.
    let amount = 10_000_000u64.min(candidate.amount_lamports);
    ensure!(amount <= 10_000_000, "BUY cap");
    Ok(
        serde_json::json!({"outcome":"MOCK_ONLY_NO_SUBMIT","signature":signature,"source_amount_lamports":candidate.amount_lamports,"mock_copy_amount_lamports":amount,"producer_to_quote_age_ms":age_ms,"quote_calls":1,"source_finality":"explicit synthetic fixture proof","real_financial_calls":0,"signatures_created":0}),
    )
}
