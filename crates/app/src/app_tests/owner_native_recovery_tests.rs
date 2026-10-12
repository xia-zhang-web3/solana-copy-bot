//! Only the new native-origin recovery; provider/receipts are explicitly mocked.
use super::owner_native_recovery_fixture::NativeCase;
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::owner_exit_order_id;

#[tokio::test]
async fn owner_native_recovery_exact_buy_sells_once_without_buy_or_source_actions() -> Result<()> {
    let c = Box::pin(NativeCase::new()).await?;
    let buy = c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?;
    let source_calls = c.gate.source.calls("sendTransaction");
    let budget = c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    let result = c.tick().await?;
    let order = c
        .gate
        .bot
        .load_execution_canary_order(&owner_exit_order_id(&c.exit))?;
    assert_eq!(
        c.provider.calls("sendTransaction"),
        1,
        "{result:?}; {order:?}"
    );
    assert_eq!(c.provider.calls("quote"), 1);
    assert_eq!(c.gate.source.calls("sendTransaction"), source_calls);
    assert!(
        c.gate
            .bot
            .execution_canary_fill_exists(&owner_exit_order_id(&c.exit))?,
        "{result:?}; {:?}",
        c.gate
            .bot
            .load_execution_canary_receipt_proof(&owner_exit_order_id(&c.exit))?
    );
    assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 0);
    assert_eq!(c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?, buy);
    assert!(c.gate.bot.list_owner_technical_buy_intents(10)?.is_empty());
    let after = c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    assert_eq!(after.id, budget.id);
    assert_eq!(after.deadline, budget.deadline);
    assert_eq!(after.activated_at, budget.activated_at);
    assert_eq!(after.buy_order_id, budget.buy_order_id);
    c.tick().await?;
    Box::pin(
        crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
            .process_tick(&c.reopen()?, Utc::now()),
    )
    .await?;
    assert_eq!(c.provider.calls("sendTransaction"), 1);
    assert_eq!(c.provider.calls("quote"), 1);
    assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
    Ok(())
}

#[tokio::test]
async fn owner_native_recovery_wrong_parent_quantity_receipt_and_cost_refuse_before_quote(
) -> Result<()> {
    for fault in [
        "quantity",
        "signature",
        "run",
        "window",
        "cost",
        "proof",
        "origin",
        "source_finality",
        "fence",
    ] {
        let mut c = Box::pin(NativeCase::new()).await?;
        match fault {
            "quantity" => c.config.owner_exit.as_mut().unwrap().amount_raw -= 1,
            "signature" => {
                c.config.owner_exit.as_mut().unwrap().buy_receipt_signature = "MOCK_WRONG".into()
            }
            "run" => c.config.owner_exit.as_mut().unwrap().run_id = "wrong-old-run".into(),
            "window" => {
                c.config.owner_exit.as_mut().unwrap().expires_at =
                    (Utc::now() + chrono::Duration::hours(5)).to_rfc3339()
            }
            "cost" => {
                c.gate
                    .sql()?
                    .execute("UPDATE positions SET cost_lamports=cost_lamports+1", [])?;
            }
            "proof" => {
                c.gate.sql()?.execute("UPDATE execution_canary_receipt_proofs SET reason='receipt_accounting_pending'",[])?;
            }
            "origin" => {
                let sql = c.gate.sql()?;
                sql.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON;")?;
                sql.execute(
                    "UPDATE execution_order_sources SET copy_signal_id='MOCK_WRONG_ORIGIN'",
                    [],
                )?;
            }
            "source_finality" => {
                let sql = c.gate.sql()?;
                let names=sql.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='native_buy_decisions'")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
                for name in names {
                    sql.execute_batch(&format!("DROP TRIGGER {name}"))?;
                }
                sql.execute(
                    "UPDATE native_buy_decisions SET finalized_slot=NULL,finalized_at=NULL",
                    [],
                )?;
            }
            "fence" => {
                c.gate.sql()?.execute(
                    "DROP TRIGGER IF EXISTS native_buy_fence_epoch_immutable",
                    [],
                )?;
                c.gate.sql()?.execute(
                    "UPDATE native_buy_fence_epochs SET genesis_hash='wrong-genesis'",
                    [],
                )?;
            }
            _ => unreachable!(),
        }
        let result = c.tick().await;
        assert!(
            result.is_err()
                || result
                    .as_ref()
                    .is_ok_and(|s| s.state_machine_skipped_reason.is_some()),
            "{fault}: {result:?}"
        );
        assert_eq!(c.provider.calls("quote"), 0, "{fault}");
        assert_eq!(c.provider.calls("sendTransaction"), 0, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn owner_native_recovery_config_disallows_buy_scope_and_limit_widening() -> Result<()> {
    let c = Box::pin(NativeCase::new()).await?;
    for fault in [
        "buy",
        "entry",
        "enabled",
        "attempts",
        "priority",
        "wallet",
        "slip",
        "positions",
        "policy_alias",
    ] {
        let mut e = c.config.clone();
        match fault {
            "policy_alias" => {
                e.owner_exit.as_mut().unwrap().policy = copybot_config::OWNER_EXIT_V1.into();
                e.owner_exit.as_mut().unwrap().amount_raw = 1167085;
            }
            "buy" => e.owner_technical_buy = c.gate.source.config.owner_technical_buy.clone(),
            "entry" => e.canary_entry_submit_enabled = true,
            "enabled" => e.enabled = true,
            "attempts" => e.max_submit_attempts = 2,
            "priority" => e.owner_exit.as_mut().unwrap().max_priority_fee_lamports = 22001,
            "wallet" => e.owner_exit.as_mut().unwrap().wallet_pubkey = "wrong-wallet".into(),
            "slip" => e.owner_exit.as_mut().unwrap().max_slippage_bps = 51,
            "positions" => e.canary_max_open_positions = 2,
            _ => unreachable!(),
        }
        assert!(copybot_config::validate_owner_exit(&e).is_err(), "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn owner_native_recovery_stop_expiry_failure_unknown_partial_never_resend() -> Result<()> {
    for mode in [
        "stop",
        "expired",
        "sell_quote_failure",
        "unknown_sell",
        "partial_sell",
    ] {
        let c = Box::pin(NativeCase::new()).await?;
        if mode == "stop" {
            std::fs::write(&c.config.canary_kill_switch_path, "STOP")?;
        }
        if mode == "expired" {
            let end = chrono::DateTime::parse_from_rfc3339(
                &c.config.owner_exit.as_ref().unwrap().expires_at,
            )?
            .with_timezone(&Utc);
            c.at(end).await?;
            assert_eq!(c.provider.calls("sendTransaction"), 0);
            continue;
        }
        c.provider.state.lock().unwrap().mode = mode;
        let _ = c.tick().await;
        let sends = c.provider.calls("sendTransaction");
        let quotes = c.provider.calls("quote");
        let _ = c.tick().await;
        let _ = Box::pin(
            crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
                .process_tick(&c.reopen()?, Utc::now()),
        )
        .await;
        assert_eq!(c.provider.calls("sendTransaction"), sends, "{mode}");
        assert_eq!(c.provider.calls("quote"), quotes, "{mode}");
        assert!(sends <= 1);
        assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
        if mode == "unknown_sell" || mode == "partial_sell" {
            assert_eq!(sends, 1);
            assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn owner_native_recovery_concurrent_unknown_ticks_share_one_sell_slot() -> Result<()> {
    let c = Box::pin(NativeCase::new()).await?;
    c.provider.state.lock().unwrap().mode = "unknown_sell";
    let a = crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone());
    let b = crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone());
    let (x, y) = tokio::join!(
        Box::pin(a.process_tick(&c.gate.bot, Utc::now())),
        Box::pin(b.process_tick(&c.gate.bot, Utc::now()))
    );
    x?;
    y?;
    assert_eq!(c.provider.calls("sendTransaction"), 1);
    assert_eq!(c.provider.calls("quote"), 1);
    let _ = c.tick().await;
    assert_eq!(c.provider.calls("sendTransaction"), 1);
    assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 1);
    Ok(())
}

#[test]
#[ignore = "requires exact inactive recovery config and optional canonical read-only parent DB"]
fn owner_native_recovery_exact_stopped_config_and_optional_actual_parent_without_io() -> Result<()>
{
    let path = std::env::var("COPYBOT_OWNER_NATIVE_RECOVERY_CONFIG")?;
    let c = copybot_config::load_from_path(path)?;
    assert!(copybot_config::owner_native_recovery_exit(&c.execution));
    assert!(!c.execution.owner_exit.as_ref().unwrap().activate);
    assert!(c.execution.owner_technical_buy.is_none());
    crate::config_contract::validate_execution_runtime_contract(&c.execution, &c.system.env)?;
    crate::config_contract::validate_live_ingestion_source_contract(
        &c.ingestion.source,
        &c.system.env,
    )?;
    crate::risk_contract::validate_execution_risk_contract(&c.risk)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(
        &c.execution,
        &c.risk,
        &c.system.env,
    )?;
    let mut active = c.execution.clone();
    active.owner_exit.as_mut().unwrap().activate = true;
    copybot_config::validate_owner_exit(&active)?;
    crate::config_contract::validate_execution_runtime_contract(&active, &c.system.env)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(
        &active,
        &c.risk,
        &c.system.env,
    )?;
    let intent = crate::execution_owner_exit_authority::configured(&active)?.unwrap();
    if let Ok(path) = std::env::var("COPYBOT_NATIVE_RECOVERY_PARENT_DB") {
        let store = copybot_storage_core::SqliteStore::open_read_only(&path)?;
        store.validate_owner_native_recovery_exit(&intent, Utc::now())?;
    }
    Ok(())
}
