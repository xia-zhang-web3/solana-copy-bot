//! Changed recovery boundaries only; synthetic history/RPC/signer, not live trades.
use super::owner_native_recovery_window_fixture::{fresh_case, intent};
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_storage_core::owner_exit_order_id;

#[tokio::test]
async fn native_recovery_window_expired_original_fresh_exit_preserves100_once() -> Result<()> {
    let c=Box::pin(fresh_case()).await?;
    let sql=c.gate.sql()?;
    let before:(String,String,String)=sql.query_row(
        "SELECT activated_at,deadline,policy_identity FROM native_buy_technical_cohort",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let budget=c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    let buy=c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?;
    let result=c.tick().await?;
    assert_eq!(c.provider.calls("sendTransaction"),1,"{result:?}");
    assert_eq!(c.gate.bot.execution_canary_open_position_count()?,0,"{result:?}");
    let sell=c.gate.bot.load_execution_canary_receipt_facts(&owner_exit_order_id(&c.exit))?.unwrap();
    assert_eq!(sell.token_delta.unwrap().raw,-i128::from(intent(&c)?.amount_raw));
    let after:(String,String,String)=sql.query_row(
        "SELECT activated_at,deadline,policy_identity FROM native_buy_technical_cohort",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert_eq!(before,after);
    assert_eq!(c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?,buy);
    let b=c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    assert_eq!((b.activated_at,b.deadline,b.buy_order_id),(budget.activated_at,budget.deadline,budget.buy_order_id));
    c.tick().await?;
    Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
        .process_tick(&c.reopen()?,Utc::now())).await?;
    assert_eq!(c.provider.calls("sendTransaction"),1);
    assert_eq!(c.provider.calls("quote"),1);
    assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
    Ok(())
}

#[tokio::test]
async fn native_recovery_window_original_contract_and_fresh_deadlines_refuse() -> Result<()> {
    for fault in ["policy","original_start","original_end","without_contract","long_window","expired_exit","qty","receipt","cost"] {
        let mut c=Box::pin(fresh_case()).await?;
        let p=c.config.owner_exit.as_mut().unwrap();
        match fault {
            "policy"=>p.native_recovery.as_mut().unwrap().original_policy_identity="e".repeat(64),
            "original_start"=>p.native_recovery.as_mut().unwrap().original_activated_at=(Utc::now()-Duration::days(2)).to_rfc3339(),
            "original_end"=>p.native_recovery.as_mut().unwrap().original_expires_at=(Utc::now()-Duration::minutes(5)).to_rfc3339(),
            "without_contract"=>p.native_recovery=None,
            "long_window"=>p.expires_at=(Utc::now()+Duration::hours(5)).to_rfc3339(),
            "expired_exit"=>{p.activated_at=(Utc::now()-Duration::minutes(20)).to_rfc3339();p.expires_at=(Utc::now()-Duration::seconds(1)).to_rfc3339();},
            "qty"=>p.amount_raw+=1,
            "receipt"=>p.buy_receipt_signature="MOCK_WRONG".into(),
            "cost"=>{c.gate.sql()?.execute("UPDATE positions SET cost_lamports=cost_lamports+1",[])?;},
            _=>unreachable!(),
        }
        let result=c.tick().await;
        assert!(result.is_err() || result.as_ref().is_ok_and(|s|s.state_machine_skipped_reason.is_some()),"{fault}: {result:?}");
        assert_eq!(c.provider.calls("quote"),0,"{fault}");
        assert_eq!(c.provider.calls("sendTransaction"),0,"{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_window_exact_baseline_inventory_and_wire_refuse() -> Result<()> {
    for fault in ["less100","more100","bad_ata","baseline99","baseline101","stale","oversell"] {
        let mut c=Box::pin(fresh_case()).await?;
        let raw=intent(&c)?.amount_raw;
        match fault {
            "less100"=>c.provider.state.lock().unwrap().wallet_held_raw=Some(raw+99),
            "more100"=>c.provider.state.lock().unwrap().wallet_held_raw=Some(raw+101),
            "bad_ata"=>c.config.owner_exit.as_mut().unwrap().native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.token_account=bs58::encode([4;32]).into_string(),
            "baseline99"=>c.config.owner_exit.as_mut().unwrap().native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.amount_raw=99,
            "baseline101"=>c.config.owner_exit.as_mut().unwrap().native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.amount_raw=101,
            "stale"=>c.provider.state.lock().unwrap().protected_balance_slot=Some(1),
            "oversell"=>c.provider.state.lock().unwrap().mode="sell_oversell_one",
            _=>unreachable!(),
        }
        let result=c.tick().await;
        assert_eq!(c.provider.calls("sendTransaction"),0,"{fault}: {result:?}");
        assert_eq!(c.gate.bot.execution_canary_open_position_count()?,1);
        if fault!="oversell" {assert_eq!(c.provider.calls("quote"),0,"{fault}");}
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_window_short_balance_after_simulation_before_key_and_before_send() -> Result<()> {
    for mode in ["short_after_simulation","short_before_send"] {
        let c=Box::pin(fresh_case()).await?;
        c.provider.state.lock().unwrap().mode=mode;
        if mode=="short_after_simulation" {std::fs::remove_file(&c.config.execution_signer_keypair_path)?;}
        let result=c.tick().await;
        assert_eq!(c.provider.calls("sendTransaction"),0,"{mode}: {result:?}");
        assert_eq!(c.provider.calls("simulateTransaction"),1,"{mode}: {result:?}");
        if mode=="short_after_simulation" {assert!(result.as_ref().is_err_and(|e|e.to_string().contains("owner_exit_wallet_amount_changed")),"{result:?}");}
        assert_eq!(c.gate.bot.execution_canary_open_position_count()?,1);
        let _=c.tick().await;
        assert_eq!(c.provider.calls("sendTransaction"),0);
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_window_unknown_partial_stop_restart_no_repeat() -> Result<()> {
    for mode in ["unknown_sell","partial_sell","stop"] {
        let c=Box::pin(fresh_case()).await?;
        c.provider.state.lock().unwrap().mode=mode;
        if mode=="stop" {std::fs::write(&c.config.canary_kill_switch_path,"STOP")?;}
        let _=c.tick().await;
        let sends=c.provider.calls("sendTransaction");
        let _=c.tick().await;
        let _=Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
            .process_tick(&c.reopen()?,Utc::now())).await;
        assert_eq!(c.provider.calls("sendTransaction"),sends,"{mode}");
        assert_eq!(sends,if mode=="stop" {0}else{1},"{mode}");
        assert_eq!(c.gate.bot.execution_canary_open_position_count()?,1);
        assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
    }
    Ok(())
}

#[test]
#[ignore="exact STOPPED config and saved actual readonly native parent graph"]
fn native_recovery_window_actual38_config_and_saved_parent_readonly() -> Result<()> {
    let path=std::env::var("COPYBOT_OWNER_NATIVE_RECOVERY_CONFIG")?;
    let loaded=copybot_config::load_from_path(path)?;
    crate::config_contract::validate_execution_runtime_contract(&loaded.execution,&loaded.system.env)?;
    crate::risk_contract::validate_execution_risk_contract(&loaded.risk)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(&loaded.execution,&loaded.risk,&loaded.system.env)?;
    let mut c=loaded.execution;
    assert!(c.owner_exit.as_ref().unwrap().native_recovery.is_some());
    assert!(!c.owner_exit.as_ref().unwrap().activate);
    c.owner_exit.as_mut().unwrap().activate=true;
    copybot_config::validate_owner_exit(&c)?;
    let i=crate::execution_owner_exit_authority::configured(&c)?.unwrap();
    assert_eq!(i.amount_raw,1069140);
    assert_eq!(i.min_reserve_lamports,251346415);
    let store=copybot_storage_core::SqliteStore::open_read_only(std::env::var("COPYBOT_NATIVE_RECOVERY_PARENT_DB")?)?;
    store.validate_owner_native_recovery_exit(&i,i.activated_at)?;
    let original=store.load_execution_canary_receipt_facts(&i.buy_order_id)?.unwrap();
    assert_eq!(original.transaction_fee.unwrap().as_u64(),55000);
    assert_eq!(original.wallet_native_delta.as_i128(),-10055000);
    Ok(())
}

#[tokio::test]
async fn native_recovery_window_expiry_stop_only_and_overflow_fail_closed() -> Result<()> {
    let c=Box::pin(fresh_case()).await?;
    let original=intent(&c)?;
    let mut overflow=original.clone();overflow.amount_raw=u64::MAX;
    assert!(crate::execution_owner_native_recovery::wallet_amount(&c.config,&overflow,u64::MAX).is_err());
    c.gate.sql()?.execute("UPDATE execution_tiny_experiment SET state='stopped',stop_reason='tiny_budget_fee_exhausted'",[])?;
    assert!(c.gate.bot.register_owner_exit_intent(&original).is_err());
    assert_eq!(c.provider.calls("sendTransaction"),0);
    c.gate.sql()?.execute("UPDATE execution_tiny_experiment SET stop_reason='tiny_budget_deadline'",[])?;
    c.gate.bot.register_owner_exit_intent(&original)?;
    let mut conflicting=original.clone();conflicting.expires_at+=Duration::seconds(1);
    assert!(c.gate.bot.register_owner_exit_intent(&conflicting).is_err());
    assert_eq!(c.gate.bot.load_owner_exit_intent(&original.intent_id)?,Some(original));
    Ok(())
}
