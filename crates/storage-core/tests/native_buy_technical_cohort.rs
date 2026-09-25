#[path = "common/association.rs"]
mod fixture;
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::{
    CandidateGeneration, DeliveryEvent, ProviderAssertion, Terminal, BlockTime,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{NativeBuyActivationBinding, NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY, SPL_TOKEN_PROGRAM},
    SqliteStore,
};

fn authority(now: chrono::DateTime<Utc>) -> TechnicalCohortAuthority {
    TechnicalCohortAuthority {
        run_id:"technical-cohort-once".into(),
        wallet_ids:vec!["leader".into(),"other-preselected".into()],
        mint_policy:CLASSIC_SPL_MINT_POLICY.into(),
        activated_at:now-Duration::seconds(240),
        deadline:now+Duration::seconds(30),
        max_buy_count:1,
        policy_identity:"pinned-execution-policy".into(),
    }
}
fn fence(at: chrono::DateTime<Utc>, slot: u64) -> NativeBuyFence {
    NativeBuyFence { session:"session-A".into(), processed_slot:slot,
        sampled_at:at, genesis_hash:"pinned-genesis".into(),
        policy_identity:"pinned-execution-policy".into() }
}
fn admission(signature: &str, wallet: &str, mint: &str, slot: u64) -> copybot_core_types::association_delivery::AdmissionFacts {
    let mut a = fixture::facts();
    a.facts.signature=signature.into();
    a.facts.wallet=wallet.into();
    a.facts.token_in="So11111111111111111111111111111111111111112".into();
    a.facts.token_out=mint.into();
    a.facts.slot=slot;
    a.facts.exact_amounts.as_mut().unwrap().amount_in_raw="10000000".into();
    a.facts.exact_amounts.as_mut().unwrap().amount_out_raw="12345".into();
    a
}
fn terminal(a: &copybot_core_types::association_delivery::AdmissionFacts) -> Terminal {
    Terminal::ProviderAsserted(ProviderAssertion {
        slot:a.facts.slot, blockhash:"pinned-block".into(),
        signature:a.facts.signature.clone(), transaction_index:1,
        block_time:BlockTime::Missing,
    })
}

#[test]
fn fresh_buy_after_old_start_fence_uses_pinned_epoch_without_discovery() -> Result<()> {
    let (_dir,path)=fixture::db();
    let now=Utc::now();
    let mut inbox=AssociationInbox::open(&path,fixture::limits())?;
    let auth=authority(now);
    inbox.register_technical_cohort_authority(&auth)?;
    inbox.record_native_buy_fence_epoch(&fence(now-Duration::seconds(180),5))?;
    inbox.record_native_buy_fence_epoch(&fence(now-Duration::seconds(1),6))?;
    let a=admission("source-buy-one","leader","unknown-classic-mint",7);
    inbox.persist_at(&fixture::event(1,DeliveryEvent::Admission(a.clone())),&CandidateGeneration::Unknown,now)?;
    inbox.persist_at(&fixture::event(2,DeliveryEvent::Terminal {
        signature:a.facts.signature.clone(), expected:a.clone(), result:terminal(&a),
    }),&CandidateGeneration::Unknown,now)?;
    let store=SqliteStore::open(&path)?;
    assert_eq!(store.list_native_buy_pending(2)?.len(),1);
    assert!(store.native_buy_record_finalized("source-buy-one",7,SPL_TOKEN_PROGRAM,now)?);
    let candidate=store.native_buy_ready("source-buy-one",now,120)?.unwrap();
    assert_eq!(candidate.wallet,"leader");
    assert_eq!(candidate.mint,"unknown-classic-mint");
    assert_eq!(store.native_buy_policy_identity(&candidate.signal_id)?.as_deref(),Some("pinned-execution-policy"));
    drop(inbox);
    let mut reopened=AssociationInbox::open(&path,fixture::limits())?;
    reopened.register_technical_cohort_authority(&auth)?;
    assert!(store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now,120)?);
    reopened.record_native_buy_fence_epoch(&fence(now+Duration::seconds(1),8))?;
    assert!(store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now+Duration::seconds(1),120)?);
    assert!(reopened.register_technical_cohort_authority(&TechnicalCohortAuthority {
        deadline:auth.deadline+Duration::seconds(1),..auth.clone()
    }).is_err());
    let another=admission("source-buy-two","other-preselected","another-mint",9);
    reopened.persist_at(&fixture::event(3,DeliveryEvent::Admission(another.clone())),&CandidateGeneration::Unknown,now+Duration::seconds(2))?;
    reopened.persist_at(&fixture::event(4,DeliveryEvent::Terminal {
        signature:another.facts.signature.clone(),expected:another.clone(),result:terminal(&another),
    }),&CandidateGeneration::Unknown,now+Duration::seconds(2))?;
    assert!(store.native_buy_ready("source-buy-two",now,120)?.is_none());
    let count: i64=rusqlite::Connection::open(&path)?.query_row(
        "SELECT count(*) FROM native_buy_cohort_decisions",[],|r|r.get(0))?;
    assert_eq!(count,1);
    assert!(store.native_buy_ready("source-buy-one",auth.deadline,120)?.is_none());
    Ok(())
}

#[test]
fn stale_epoch_foreign_wallet_and_replayed_slot_never_gain_authority() -> Result<()> {
    for (wallet,slot,age) in [("leader",7,121),("foreign",7,1),("leader",6,1)] {
        let (_dir,path)=fixture::db();
        let now=Utc::now();
        let mut inbox=AssociationInbox::open(&path,fixture::limits())?;
        inbox.register_technical_cohort_authority(&authority(now))?;
        inbox.record_native_buy_fence_epoch(&fence(now-Duration::seconds(age),6))?;
        let a=admission("untrusted-source",wallet,"mint",slot);
        inbox.persist_at(&fixture::event(1,DeliveryEvent::Admission(a.clone())),&CandidateGeneration::Unknown,now)?;
        inbox.persist_at(&fixture::event(2,DeliveryEvent::Terminal {
            signature:a.facts.signature.clone(),expected:a.clone(),result:terminal(&a),
        }),&CandidateGeneration::Unknown,now)?;
        let store=SqliteStore::open(&path)?;
        assert!(store.list_native_buy_pending(2)?.is_empty());
        assert!(store.native_buy_ready("untrusted-source",now,120)?.is_none());
    }
    Ok(())
}

#[test]
fn cohort_protected_tiny_budget_keeps_sell_authority_past_one_hour_only_until_cohort_deadline() -> Result<()> {
    let (_dir,path)=fixture::db();
    let now=Utc::now();
    let mut auth=authority(now);
    auth.activated_at=now-Duration::seconds(10);
    auth.deadline=auth.activated_at+Duration::hours(4);
    let mut inbox=AssociationInbox::open(&path,fixture::limits())?;
    inbox.register_technical_cohort_authority(&auth)?;
    inbox.record_native_buy_fence_epoch(&fence(now-Duration::seconds(5),5))?;
    let a=admission("source-buy-four-hours","leader","classic-mint",7);
    inbox.persist_at(&fixture::event(1,DeliveryEvent::Admission(a.clone())),
        &CandidateGeneration::Unknown,now)?;
    inbox.persist_at(&fixture::event(2,DeliveryEvent::Terminal {
        signature:a.facts.signature.clone(),expected:a.clone(),result:terminal(&a),
    }),&CandidateGeneration::Unknown,now)?;
    let store=SqliteStore::open(&path)?;
    assert!(store.native_buy_record_finalized(&a.facts.signature,7,SPL_TOKEN_PROGRAM,now)?);
    let candidate=store.native_buy_ready(&a.facts.signature,now,120)?.unwrap();
    let order=store.reserve_execution_canary_order(&candidate.signal_id,
        "jupiter_swap_instructions",now)?.order;
    let binding=NativeBuyActivationBinding {
        signal_id:candidate.signal_id,decision_id:candidate.decision_id,
        policy_identity:auth.policy_identity.clone(),max_age_seconds:120,
        order_id:order.order_id,client_order_id:order.client_order_id,
        attempt:order.attempt,route:order.route,
    };
    let policy=store.prepare_tiny_native_policy_for_native_buy(
        &auth.run_id,"bot-wallet",175_200_031,160_200_031,8,now,
        &binding,||Ok(now))?;
    assert_eq!(policy.deadline,auth.deadline);
    drop(store);
    let reopened=SqliteStore::open(&path)?;
    assert_eq!(reopened.load_tiny_experiment(now+Duration::hours(1)+Duration::seconds(1))?
        .unwrap().state,"active");
    assert_eq!(reopened.load_tiny_experiment(auth.deadline)?.unwrap().state,"stopped");
    Ok(())
}
