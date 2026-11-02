#![cfg(feature = "local-bounded-session")]
#[path = "common/association.rs"]
mod association;
#[path = "common/local_session_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox,native_buy::*};
#[test]
fn local_native_admission_uses_two_local_fk_bindings_and_keeps_once_empty() -> Result<()> {
    let f=fixture::F::blank()?;
    let mut inbox=AssociationInbox::open(&f.path,association::limits())?;
    inbox.register_local_bounded_session(&f.authority)?;
    inbox.register_technical_cohort_authority(&f.authority.cohort)?;
    for i in 0..2 {
        let at=f.now+Duration::seconds(i as i64*3);
        let slot=5+i as u64*3;
        inbox.record_native_buy_fence_epoch(&NativeBuyFence {session:"session-A".into(),processed_slot:slot,sampled_at:at,genesis_hash:f.authority.genesis.clone(),policy_identity:f.authority.cohort.policy_identity.clone()})?;
        let mut a=association::facts();
        a.facts.signature=f.authority.model.cycles[i].buy_signature.clone();
        a.facts.wallet=fixture::SOURCE.into();a.facts.slot=slot+1;
        a.facts.token_in="So11111111111111111111111111111111111111112".into();
        a.facts.token_out=copybot_storage_core::OWNER_EXIT_MINT.into();
        let x=a.facts.exact_amounts.as_mut().unwrap();
        x.amount_in_raw="10000000".into();x.amount_in_decimals=9;x.amount_out_raw="7000".into();x.amount_out_decimals=6;
        let observed=at+Duration::seconds(1);
        inbox.persist_at(&association::event(i as u64*2+1,DeliveryEvent::Admission(a.clone())),&CandidateGeneration::Unknown,observed)?;
        inbox.persist_at(&association::event(i as u64*2+2,DeliveryEvent::Terminal {signature:a.facts.signature.clone(),expected:a.clone(),result:Terminal::ProviderAsserted(ProviderAssertion {slot:a.facts.slot,blockhash:"local-block".into(),signature:a.facts.signature.clone(),transaction_index:0,block_time:BlockTime::Missing})}),&CandidateGeneration::Unknown,observed)?;
        assert!(f.store.native_buy_record_finalized(&a.facts.signature,a.facts.slot,SPL_TOKEN_PROGRAM,observed)?);
        assert!(f.store.native_buy_ready(&a.facts.signature,observed,120)?.is_some());
        assert_eq!(f.count("local_bounded_native_decisions")?,i as u64+1);
        assert_eq!(f.count("native_buy_cohort_decisions")?,0);
        assert_eq!(f.count("native_buy_technical_cohort")?,0);
        if i==0 {
            // Admission-only fixture transition. Actual native accounting closure is
            // checked by LOCAL P01; no decision/checkpoint is seeded with SQL here.
            f.conn()?.execute("UPDATE local_bounded_cycles SET state='closed' WHERE cycle_no=1",[])?;
        }
    }
    drop(inbox);
    let mut reopened=AssociationInbox::open(&f.path,association::limits())?;
    reopened.register_local_bounded_session(&f.authority)?;
    assert_eq!(f.count("local_bounded_native_decisions")?,2);
    assert!(f.conn()?.execute("DELETE FROM local_bounded_native_decisions",[]).is_err());
    Ok(())
}
