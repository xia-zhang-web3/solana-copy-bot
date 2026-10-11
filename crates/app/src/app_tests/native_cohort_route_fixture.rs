//! Synthetic fresh native admission; real quote transport and accepted Raydium wire operands.
#[path = "native_buy_route_fixture.rs"]
mod base;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{association_delivery::*, CopySignalRow};
use copybot_storage_core::{association_inbox::{AssociationInbox, InboxLimits},
    native_buy::{NativeBuyFence, SPL_TOKEN_PROGRAM}, SqliteStore};
use crate::execution_canary_route::NativeBuyGuard;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub(super) struct Case {
    pub config: ExecutionConfig,
    pub store: SqliteStore,
    pub signal: CopySignalRow,
    pub guard: NativeBuyGuard,
    pub now: chrono::DateTime<Utc>,
}

pub(super) fn setup() -> Result<Case> {
    let now = Utc::now();
    let mut config = base::config(&crate::app_tests::owner_exit_test_fixture::wallet());
    config.tiny_experiment.activate = true;
    config.tiny_experiment.policy_mode = copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    config.tiny_experiment.id = Some("route28-local".into());
    config.canary_entry_submit_enabled = true;
    config.canary_route = "jupiter_swap_instructions".into();
    config.quote_canary_slippage_bps = 50;
    config.quote_canary_buy_slippage_bps = 50;
    config.quote_canary_sell_slippage_bps = 50;
    config.canary_max_signal_age_seconds = 120;
    config.pretrade_min_sol_reserve = 0.160_200_031;
    config.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(), activate: true,
        run_id: "route28-local".into(), wallet_ids: vec![base::LEADER.into()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: config.canary_route.clone(), activated_at: (now-Duration::seconds(2)).to_rfc3339(),
        deadline: (now+Duration::seconds(598)).to_rfc3339(), max_wait_seconds:600,
        max_buy_count:1, max_source_sell_count:1,
    });
    let path=format!("file:route28-{}-{}?mode=memory&cache=shared",
        std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed));
    let mut store=SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"),"/../../migrations")))?;
    let mut inbox=AssociationInbox::open(&path,InboxLimits{count:1000,bytes:8<<20,busy_ms:100})?;
    let auth=crate::execution_technical_cohort::authority(&config)?.context("authority")?;
    inbox.register_technical_cohort_authority(&auth)?;
    inbox.record_native_buy_fence_epoch(&NativeBuyFence{session:"native-session-A".into(),
        processed_slot:99,sampled_at:now-Duration::seconds(1),genesis_hash:base::GENESIS.into(),
        policy_identity:auth.policy_identity})?;
    let mut admission=base::admission();
    admission.facts.token_out=crate::app_tests::run15_rpc_proof_fixture::MINT.into();
    let exact=copybot_core_types::ExactSwapAmounts { amount_in_raw:"10000000".into(),
        amount_out_raw:"100000000".into(), amount_in_decimals:9, amount_out_decimals:9 };
    admission.facts.amount_in_bits=(exact.amount_in_raw.parse::<u64>()? as f64/1e9).to_bits();
    admission.facts.amount_out_bits=(exact.amount_out_raw.parse::<u64>()? as f64/1e9).to_bits();
    admission.facts.exact_amounts=Some(exact);
    inbox.persist_at(&base::delivery(1,DeliveryEvent::Admission(admission.clone())),&CandidateGeneration::Unknown,now)?;
    let result=Terminal::ProviderAsserted(ProviderAssertion{slot:100,blockhash:"local-block".into(),
        signature:base::SOURCE_SIGNATURE.into(),transaction_index:1,block_time:BlockTime::Missing});
    inbox.persist_at(&base::delivery(2,DeliveryEvent::Terminal{signature:base::SOURCE_SIGNATURE.into(),
        expected:admission,result}),&CandidateGeneration::Unknown,now)?;
    assert!(store.native_buy_record_finalized(base::SOURCE_SIGNATURE,100,SPL_TOKEN_PROGRAM,now)?);
    let candidate=store.native_buy_ready(base::SOURCE_SIGNATURE,now,120)?.context("candidate")?;
    let signal=store.load_copy_signal_by_signal_id(&candidate.signal_id)?.context("signal")?;
    let guard=NativeBuyGuard::new(&config,&signal.signal_id,&candidate.decision_id,now)?;
    assert!(guard.check(&store)?);
    Ok(Case{config,store,signal,guard,now})
}
