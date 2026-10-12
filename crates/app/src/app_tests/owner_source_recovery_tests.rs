//! Changed recovery authority only. All provider/quote/receipt facts are mocks.
use super::owner_source_recovery_fixture::{prepared, tick};
use super::owner_source_gate_fixture::EXIT;
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{owner_exit_order_id, SqliteStore};

#[tokio::test]
async fn owner_source_recovery_sells_confirmed_quantity_once_without_buy_or_bot_authority() -> Result<()> {
    let (c, e) = prepared().await?;
    let original_parent = c.source.store.load_owner_technical_buy_intent("owner-test-intent")?;
    let original_buy = c.source.store.load_execution_canary_receipt_facts(&c.source.order_id())?;
    let original_budget = c.source.store.load_tiny_experiment(Utc::now())?;
    let result = tick(&c, &e).await?;
    assert_eq!(c.source.calls("sendTransaction"), 2, "{result:?}");
    let exit = c.source.store.load_owner_exit_intent(EXIT)?.unwrap();
    assert_eq!(exit.amount_raw, original_buy.as_ref().unwrap().token_delta.unwrap().raw as u64);
    assert_eq!(exit.buy_receipt_signature, original_buy.as_ref().unwrap().tx_signature);
    assert_eq!(c.source.store.execution_canary_open_position_count()?, 0);
    assert!(c.source.store.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
    assert_eq!(c.source.store.load_owner_technical_buy_intent("owner-test-intent")?, original_parent);
    assert_eq!(c.source.store.load_execution_canary_receipt_facts(&c.source.order_id())?, original_buy);
    let after_budget = c.source.store.load_tiny_experiment(Utc::now())?.unwrap();
    let original_budget = original_budget.unwrap();
    assert_eq!(after_budget.id, original_budget.id);
    assert_eq!(after_budget.wallet, original_budget.wallet);
    assert_eq!(after_budget.activated_at, original_budget.activated_at);
    assert_eq!(after_budget.deadline, original_budget.deadline);
    assert_eq!(after_budget.last_decision_at, original_budget.last_decision_at);
    assert_eq!(after_budget.buy_order_id, original_budget.buy_order_id);
    assert_eq!(after_budget.state, "completed", "ordinary settlement closes, never resets, the inherited budget");
    assert_eq!(c.bot.execution_canary_open_position_count()?, 0);
    tick(&c, &e).await?;
    let reopened = SqliteStore::open(c.source.root.path().join("state.db"))?;
    crate::execution_canary::ExecutionCanaryRunner::new(e).process_tick(&reopened, Utc::now()).await?;
    assert_eq!(c.source.calls("sendTransaction"), 2);
    assert_eq!(c.source.calls("quote"), 2);
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_config_rejects_buy_wallet_parent_window_and_limits() -> Result<()> {
    let (c, e) = prepared().await?;
    for fault in ["buy", "entry", "wallet", "parent", "gate", "window", "floor", "fee", "submit"] {
        let mut bad = e.clone();
        match fault {
            "buy" => bad.owner_technical_buy.as_mut().unwrap().activate = true,
            "entry" => bad.canary_entry_submit_enabled = true,
            "wallet" => bad.owner_exit.as_mut().unwrap().wallet_pubkey = "other".into(),
            "parent" => bad.owner_technical_buy = None,
            "gate" => bad.owner_technical_buy.as_mut().unwrap().source_copy_exit_gate = None,
            "window" => bad.owner_exit.as_mut().unwrap().expires_at = (Utc::now()+chrono::Duration::hours(4)).to_rfc3339(),
            "floor" => bad.owner_exit.as_mut().unwrap().min_reserve_lamports -= 1,
            "fee" => bad.owner_exit.as_mut().unwrap().max_priority_fee_lamports = 22_001,
            "submit" => bad.max_submit_attempts = 2,
            _ => unreachable!(),
        }
        assert!(copybot_config::validate_owner_exit(&bad).is_err(), "{fault}");
    }
    assert_eq!(c.source.calls("sendTransaction"), 1);
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_wrong_quantity_parent_cost_or_proof_refuses_before_quote() -> Result<()> {
    for fault in ["qty", "parent", "cost", "fill_cost", "proof"] {
        let (c, mut e) = prepared().await?;
        match fault {
            "qty" => e.owner_exit.as_mut().unwrap().amount_raw -= 1,
            "parent" => e.owner_technical_buy.as_mut().unwrap().source_copy_exit_gate.as_mut().unwrap().bot_policy_identity = "c".repeat(64),
            "cost" => { c.source.sql()?.execute("UPDATE positions SET cost_lamports=cost_lamports+1 WHERE state='open'", [])?; },
            "fill_cost" => { c.source.sql()?.execute("UPDATE fills SET notional_lamports=notional_lamports+1 WHERE order_id=?1", [c.source.order_id()])?; },
            "proof" => { c.source.sql()?.execute("UPDATE execution_canary_receipt_proofs SET reason='accounting_pending'", [])?; },
            _ => unreachable!(),
        }
        let result = tick(&c, &e).await;
        assert!(result.is_err() || result.as_ref().is_ok_and(|s| s.state_machine_skipped_reason.is_some()), "{fault}: {result:?}");
        assert_eq!(c.source.calls("sendTransaction"), 1, "{fault}");
        assert_eq!(c.source.calls("quote"), 1, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_stop_expiry_and_conflicting_exit_do_not_send() -> Result<()> {
    for fault in ["stop", "expired", "conflict"] {
        let (c, e) = prepared().await?;
        let intent = crate::execution_owner_exit_authority::configured(&e)?.unwrap();
        match fault {
            "stop" => { std::fs::write(&e.canary_kill_switch_path, "STOP")?; },
            "expired" => {
                c.source.store.register_owner_exit_intent(&intent)?;
                assert!(crate::execution_owner_exit_authority::current(&e, &c.source.store, &intent, intent.expires_at).is_err());
                let expired = crate::execution_canary::ExecutionCanaryRunner::new(e.clone())
                    .process_tick(&c.source.store, intent.expires_at).await?;
                assert!(expired.state_machine_skipped_reason.is_some());
                assert_eq!(c.source.calls("sendTransaction"), 1);
                assert_eq!(c.source.calls("quote"), 1);
                continue;
            },
            "conflict" => {
                let mut normal = e.clone();
                normal.owner_exit.as_mut().unwrap().policy = copybot_config::OWNER_ROUNDTRIP_EXIT_V1.into();
                let old = crate::execution_owner_exit_authority::configured(&normal)?.unwrap();
                c.source.store.register_owner_exit_intent(&old)?;
            },
            _ => unreachable!(),
        }
        let result = tick(&c, &e).await;
        assert!(result.is_err() || result.as_ref().is_ok_and(|s| s.state_machine_skipped_reason.is_some()), "{fault}: {result:?}");
        assert_eq!(c.source.calls("sendTransaction"), 1);
        assert_eq!(c.source.calls("quote"), 1);
    }
    Ok(())
}

#[test]
#[ignore = "requires the exact stopped recovery package config"]
fn owner_source_recovery_exact_stopped_package_config_startup_without_io() -> Result<()> {
    let path = std::env::var("COPYBOT_OWNER_SOURCE_RECOVERY_CONFIG")?;
    let c = copybot_config::load_from_path(path)?;
    assert!(copybot_config::owner_source_recovery_exit(&c.execution));
    assert!(!c.execution.owner_exit.as_ref().unwrap().activate);
    assert!(!c.execution.owner_technical_buy.as_ref().unwrap().activate);
    crate::config_contract::validate_execution_runtime_contract(&c.execution, &c.system.env)?;
    crate::config_contract::validate_live_ingestion_source_contract(&c.ingestion.source, &c.system.env)?;
    crate::risk_contract::validate_execution_risk_contract(&c.risk)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(&c.execution, &c.risk, &c.system.env)?;
    crate::execution_owner_exit_authority::configured(&c.execution)?;
    let mut active = c.execution.clone();
    active.owner_exit.as_mut().unwrap().activate = true;
    copybot_config::validate_owner_technical_buy(&active)?;
    copybot_config::validate_owner_exit(&active)?;
    crate::config_contract::validate_execution_runtime_contract(&active, &c.system.env)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(&active, &c.risk, &c.system.env)?;
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_unknown_outcome_and_partial_sell_never_resend() -> Result<()> {
    for mode in ["unknown_sell", "partial_sell", "sell_quote_failure"] {
        let (c, mut e) = prepared().await?;
        c.source.state.lock().unwrap().mode = mode;
        tick(&c, &e).await?;
        let sends = c.source.calls("sendTransaction");
        let quotes = c.source.calls("quote");
        assert_eq!(sends, if mode == "sell_quote_failure" {1} else {2});
        assert_eq!(c.source.store.execution_canary_open_position_count()?, 1);
        e.owner_exit.as_mut().unwrap().activate = false;
        tick(&c, &e).await?;
        let reopened = SqliteStore::open(c.source.root.path().join("state.db"))?;
        crate::execution_canary::ExecutionCanaryRunner::new(e.clone()).process_tick(&reopened, Utc::now()).await?;
        assert_eq!(c.source.calls("sendTransaction"), sends);
        assert_eq!(c.source.calls("quote"), quotes);
        assert!(!reopened.rearm_owner_exit_undispatched(EXIT)?);
        if mode == "unknown_sell" {
            assert!(reopened.load_execution_canary_dispatch(&owner_exit_order_id(EXIT))?.is_some());
            assert!(!reopened.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
            c.source.state.lock().unwrap().mode = "";
            tick(&c, &e).await?;
            assert!(reopened.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
            assert_eq!(c.source.calls("sendTransaction"), sends);
        }
    }
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_concurrent_ticks_share_one_exit_slot_even_if_unknown() -> Result<()> {
    for mode in ["", "unknown_sell"] {
        let (c, e) = prepared().await?;
        c.source.state.lock().unwrap().mode = mode;
        let first = Box::pin(tick(&c, &e));
        let second = Box::pin(tick(&c, &e));
        let (first, second) = tokio::join!(first, second);
        first?; second?;
        assert_eq!(c.source.calls("sendTransaction"), 2, "one inherited BUY + one recovery SELL");
        assert_eq!(c.source.calls("quote"), 2);
        assert_eq!(c.source.sql()?.query_row("SELECT count(*) FROM owner_exit_fee_reservations", [], |r| r.get::<_,u32>(0))?, 1);
        tick(&c, &e).await?;
        assert_eq!(c.source.calls("sendTransaction"), 2);
        assert!(!c.source.store.rearm_owner_exit_undispatched(EXIT)?);
    }
    Ok(())
}

#[tokio::test]
async fn owner_source_recovery_normal_gated_path_still_requires_confirmed_bot_buy() -> Result<()> {
    let (mut c, _) = prepared().await?;
    assert!(crate::execution_owner_roundtrip::exit_config(&c.source.config, &c.source.store)?.is_none());
    c.seed_bot()?;
    let normal = crate::execution_owner_roundtrip::exit_config(&c.source.config, &c.source.store)?.unwrap();
    assert_eq!(normal.owner_exit.as_ref().unwrap().policy, copybot_config::OWNER_ROUNDTRIP_EXIT_V1);
    assert!(!copybot_config::owner_source_recovery_exit(&normal));
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 2);
    assert_eq!(c.source.store.execution_canary_open_position_count()?, 0);
    Ok(())
}
