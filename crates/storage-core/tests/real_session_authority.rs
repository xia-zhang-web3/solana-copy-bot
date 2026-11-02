//! Normal authority identity/cap refusals. No financial decisions are seeded here.
#[path="common/local_session_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::{real_session::Authority,SqliteStore};
fn grant(f:&fixture::F)->Authority{
    let mut scope=f.authority.scope.clone();scope.source_buy_intent_id=scope.bot_run_id.clone();
    Authority{policy:"real_bounded_usdc_session_v1".into(),cohort:f.authority.cohort.clone(),scope,
        genesis:"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),mint:copybot_storage_core::OWNER_EXIT_MINT.into(),
        route:"jupiter_swap_instructions".into(),max_cycles:2,buy_lamports:10_000_000,gross_buy_cap:20_000_000,
        fee_cap:400_000,loss_cap:20_000_000,total_fee_cap:100_000,priority_fee_cap:50_000,native_floor:0,personal_usdc_raw:100,slippage_bps:50}
}
#[test]
fn normal_grant_is_explicit_immutable_and_does_not_seed_future_cycles()->Result<()>{
    let f=fixture::F::blank()?;let a=grant(&f);f.store.register_real_session(&a,f.now)?;
    assert_eq!(f.count("real_session_cycles")?,0);assert_eq!(f.count("native_buy_decisions")?,0);
    let s=SqliteStore::open(&f.path)?;s.register_real_session(&a,f.now)?;
    s.register_real_session(&a,a.cohort.deadline+chrono::Duration::seconds(1))?; // identity reuse for reconciliation only
    for kind in ["deadline","wallet","genesis","mode","cap"] {
        let mut other=a.clone();match kind {
            "deadline"=>other.cohort.deadline+=chrono::Duration::seconds(1),
            "wallet"=>other.scope.bot_wallet=fixture::SOURCE.into(),
            "genesis"=>other.genesis="11111111111111111111111111111111".into(),
            "mode"=>other.policy="local_bounded_session_v1".into(),_=>other.priority_fee_cap=22_000,
        }
        assert!(s.register_real_session(&other,f.now).is_err(),"{kind}");
    }
    assert_eq!(f.count("execution_tiny_experiment")?,0);assert_eq!(f.count("real_session_sessions")?,1);
    assert!(s.activate_tiny_experiment("model-session",fixture::BOT,f.now).is_err());
    Ok(())
}
#[test]
fn prior_grant_expiry_foreign_genesis_and_unsupported_lower_sell_hold_refuse()->Result<()>{
    for kind in ["prior","expired","early","genesis","lower-total"] {
        let f=fixture::F::blank()?;let mut a=grant(&f);let mut now=f.now;
        match kind {
            "prior"=>{f.store.activate_tiny_experiment("old-consumed",fixture::BOT,f.now)?;},
            "expired"=>now=a.cohort.deadline,"early"=>now-=chrono::Duration::seconds(1),
            "genesis"=>a.genesis="11111111111111111111111111111111".into(),
            _=>{a.total_fee_cap=50_000;a.fee_cap=120_000;}
        }
        assert!(f.store.register_real_session(&a,now).is_err(),"{kind}");
        assert_eq!(f.count("real_session_sessions")?,0);
    }
    Ok(())
}
