//! New same-original-window protected100 boundaries only. Providers are mocked.
use super::owner_native_recovery_early_fixture::{early_case, SavedCase};
use super::owner_native_recovery_window_fixture::intent;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::owner_exit_order_id;

#[tokio::test]
async fn native_recovery_early100_invalid_original_and_unknown_fail_before_registration() -> Result<()> {
    for fault in ["policy", "original_start", "unknown"] {
        let mut c = Box::pin(early_case()).await?;
        match fault {
            "policy" => c.config.owner_exit.as_mut().unwrap().native_recovery.as_mut().unwrap().original_policy_identity = "e".repeat(64),
            "original_start" => c.config.owner_exit.as_mut().unwrap().native_recovery.as_mut().unwrap().original_activated_at = (Utc::now()-Duration::hours(2)).to_rfc3339(),
            "unknown" => {
                c.gate.sql()?.execute("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status,notional_lamports)
                    SELECT 'unknown-signal',wallet_id,'sell',token,notional_sol,ts,status,notional_lamports FROM copy_signals WHERE signal_id=(SELECT signal_id FROM orders WHERE order_id=?1)", [&c.buy])?;
                c.gate.sql()?.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt,tx_signature,simulation_status)
                    SELECT 'exec-canary:explicit-mock-unknown','unknown-signal',route,submit_ts,'execution_canary_submitted','unknown-client',1,'unknown-signature',simulation_status FROM orders WHERE order_id=?1", [&c.buy])?;
                c.gate.sql()?.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
                    SELECT 'exec-canary:explicit-mock-unknown','unknown-signal','unknown-client',route,1,wallet,token,'sell','unknown-signature',transaction_sha256,message_sha256,claimed_at FROM execution_canary_dispatch WHERE order_id=?1", [&c.buy])?;
            }
            _ => unreachable!(),
        }
        let configured = intent(&c);
        if fault == "original_start" { assert!(configured.is_err()); continue; }
        let i = configured?;
        let error = format!("{:#}", c.gate.bot.register_owner_exit_intent(&i).unwrap_err());
        assert!(error.contains(if fault == "unknown" {"native_recovery_unknown_dispatch"}
            else {"native_recovery_original_authority_changed"}), "{fault}: {error}");
        assert!(c.gate.bot.load_owner_exit_intent(&i.intent_id)?.is_none());
        assert!(c.gate.bot.load_execution_canary_order(&owner_exit_order_id(&i.intent_id))?.is_none());
        assert_eq!(c.provider.calls("quote"), 0);
        assert_eq!(c.provider.calls("sendTransaction"), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_early100_closes_lot_preserves_original_window_and_restarts_once() -> Result<()> {
    let c = Box::pin(early_case()).await?;
    let before = c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    let buy = c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?;
    let source_sends = c.gate.source.calls("sendTransaction");
    let result = c.tick().await?;
    assert_eq!(c.provider.calls("sendTransaction"), 1, "{result:?}");
    assert_eq!(c.gate.source.calls("sendTransaction"), source_sends);
    assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 0);
    let sell = c.gate.bot.load_execution_canary_receipt_facts(&owner_exit_order_id(&c.exit))?.unwrap();
    assert_eq!(sell.token_delta.unwrap().raw, -i128::from(intent(&c)?.amount_raw));
    let observations = c.gate.bot.load_receipt_native_observations(&owner_exit_order_id(&c.exit))?.unwrap();
    assert!(observations.accounts.iter().any(|a| a.pre_token.raw.value.as_deref()
        == Some((intent(&c).unwrap().amount_raw + 100).to_string().as_str())
        && a.post_token.raw.value.as_deref() == Some("100")));
    let after = c.gate.bot.load_tiny_experiment(Utc::now())?.unwrap();
    assert_eq!((before.activated_at,before.deadline,before.buy_order_id),
        (after.activated_at,after.deadline,after.buy_order_id));
    assert_eq!(c.gate.bot.load_execution_canary_receipt_facts(&c.buy)?, buy);
    c.tick().await?;
    Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
        .process_tick(&c.reopen()?, Utc::now())).await?;
    assert_eq!(c.provider.calls("sendTransaction"), 1);
    assert_eq!(c.provider.calls("quote"), 1);
    assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
    Ok(())
}

#[tokio::test]
async fn native_recovery_early100_parent_baseline_inventory_and_oversell_refuse() -> Result<()> {
    for fault in ["policy","mixed_start","extended_end","lot","baseline99","baseline101", "bad_ata", "short", "extra", "oversell"] {
        let mut c = Box::pin(early_case()).await?;
        let raw = c.config.owner_exit.as_ref().unwrap().amount_raw;
        let p = c.config.owner_exit.as_mut().unwrap();
        match fault {
            "policy" => p.native_recovery.as_mut().unwrap().original_policy_identity = "e".repeat(64),
            "mixed_start" => p.activated_at = (Utc::now()-Duration::seconds(1)).to_rfc3339(),
            "extended_end" => p.expires_at = (DateTime::parse_from_rfc3339(&p.expires_at)?+Duration::seconds(1)).to_rfc3339(),
            "lot" => p.amount_raw += 100,
            "baseline99" => p.native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.amount_raw = 99,
            "baseline101" => p.native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.amount_raw = 101,
            "bad_ata" => p.native_recovery.as_mut().unwrap().protected_bot_usdc_baseline.token_account = bs58::encode([4;32]).into_string(),
            "short" => c.provider.state.lock().unwrap().wallet_held_raw = Some(raw+99),
            "extra" => c.provider.state.lock().unwrap().wallet_held_raw = Some(raw+101),
            "oversell" => c.provider.state.lock().unwrap().mode = "sell_oversell_one",
            _ => unreachable!(),
        }
        let result = c.tick().await;
        assert_eq!(c.provider.calls("sendTransaction"), 0, "{fault}: {result:?}");
        assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 1);
        if fault != "oversell" { assert_eq!(c.provider.calls("quote"), 0, "{fault}: {result:?}"); }
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_early100_unknown_partial_stop_and_expiry_do_not_repeat() -> Result<()> {
    for mode in ["unknown_sell","partial_sell","stop","expired"] {
        let c = Box::pin(early_case()).await?;
        c.provider.state.lock().unwrap().mode = mode;
        if mode == "stop" { std::fs::write(&c.config.canary_kill_switch_path, "STOP")?; }
        if mode == "expired" {
            c.at(intent(&c)?.expires_at).await?;
            assert_eq!(c.provider.calls("quote"), 0);
            assert_eq!(c.provider.calls("sendTransaction"), 0);
            continue;
        }
        let _ = c.tick().await;
        let sends = c.provider.calls("sendTransaction");
        let _ = c.tick().await;
        let _ = Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone())
            .process_tick(&c.reopen()?, Utc::now())).await;
        assert_eq!(c.provider.calls("sendTransaction"), sends);
        assert_eq!(sends, if mode == "stop" {0} else {1});
        assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 1);
        assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
        if mode == "unknown_sell" {
            let pending: (String, Option<u64>) = c.gate.sql()?.query_row(
                "SELECT state,actual_fee FROM owner_exit_fee_reservations", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
            assert_eq!(pending, ("pending".into(),None));
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_early100_short_after_simulation_blocks_key_or_send() -> Result<()> {
    for mode in ["short_after_simulation","short_before_send"] {
        let c = Box::pin(early_case()).await?;
        c.provider.state.lock().unwrap().mode = mode;
        if mode == "short_after_simulation" { std::fs::remove_file(&c.config.execution_signer_keypair_path)?; }
        let result = c.tick().await;
        assert_eq!(c.provider.calls("simulateTransaction"), 1, "{mode}: {result:?}");
        assert_eq!(c.provider.calls("sendTransaction"), 0, "{mode}: {result:?}");
        if mode == "short_after_simulation" {
            assert!(result.as_ref().is_err_and(|e| e.to_string().contains("owner_exit_wallet_amount_changed")), "{result:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_recovery_early100_atomic_dispatch_failure_retains_lot_without_rearm() -> Result<()> {
    let c = Box::pin(early_case()).await?;
    c.gate.sql()?.execute_batch("CREATE TRIGGER reject_early_dispatch BEFORE INSERT ON execution_canary_dispatch
        WHEN NEW.side='sell' BEGIN SELECT RAISE(ABORT,'explicit_mock_dispatch_failure'); END;")?;
    assert!(c.tick().await.is_err());
    assert_eq!(c.provider.calls("sendTransaction"), 0);
    let sql = c.gate.sql()?;
    assert_eq!(sql.query_row("SELECT count(*) FROM owner_exit_fee_reservations", [], |r| r.get::<_,u64>(0))?, 0);
    assert_eq!(sql.query_row("SELECT count(*) FROM execution_canary_dispatch WHERE side='sell'", [], |r| r.get::<_,u64>(0))?, 0);
    assert_eq!(c.gate.bot.execution_canary_open_position_count()?, 1);
    sql.execute_batch("DROP TRIGGER reject_early_dispatch")?;
    c.tick().await?;
    assert_eq!(c.provider.calls("sendTransaction"), 0);
    assert!(!c.gate.bot.rearm_owner_exit_undispatched(&c.exit)?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires preserved COPY40 DB and inactive recovery40 config; loopback-only mock execution"]
async fn native_recovery_early100_saved40_sqlite_lot_closes_with100_before_original_expiry() -> Result<()> {
    let c = Box::pin(SavedCase::new()).await?;
    let i = crate::execution_owner_exit_authority::configured(&c.config)?.unwrap();
    assert_eq!(i.amount_raw, 1041505);
    assert_eq!(i.min_reserve_lamports, 251447608);
    assert_eq!(i.buy_receipt_signature, "241p7RZpPqMuq6MU1hhqGzvQw4bXPkhVcyXK84Ujgx9dnR1XW15b9mrwkgfHWz3yMwbMaX8fGjCWnAFSoh4rZkTZ");
    c.store.validate_owner_native_recovery_original(&i, &crate::execution_owner_native_recovery::original(&c.config)?.unwrap())?;
    c.store.validate_owner_native_recovery_exit(&i, i.activated_at+Duration::minutes(30))?;
    let before = c.store.load_tiny_experiment(i.activated_at)?.unwrap();
    let buy = c.store.load_execution_canary_receipt_facts(&i.buy_order_id)?;
    c.mock_atomic_transport_and_reconcile().await?;
    assert_eq!(c.calls("sendTransaction"), 1);
    assert_eq!(c.store.execution_canary_open_position_count()?, 0);
    let order = owner_exit_order_id(&i.intent_id);
    let sell = c.store.load_execution_canary_receipt_facts(&order)?.unwrap();
    assert_eq!(sell.token_delta.unwrap().raw, -1041505);
    let observations = c.store.load_receipt_native_observations(&order)?.unwrap();
    assert!(observations.accounts.iter().any(|a| a.pre_token.raw.value.as_deref() == Some("1041605")
        && a.post_token.raw.value.as_deref() == Some("100")));
    let after = c.store.load_tiny_experiment(i.activated_at)?.unwrap();
    assert_eq!((before.activated_at,before.deadline,before.buy_order_id), (after.activated_at,after.deadline,after.buy_order_id));
    assert_eq!(c.store.load_execution_canary_receipt_facts(&i.buy_order_id)?, buy);
    c.tick().await?;
    let reopened = SqliteStore::open(c.root.path().join("state.db"))?;
    let mut runner = crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone());
    runner.owner_exit_adapter = Some(c.adapter.clone());
    Box::pin(runner.process_tick(&reopened, Utc::now())).await?;
    assert_eq!(c.calls("sendTransaction"), 1);
    assert!(!c.store.rearm_owner_exit_undispatched(&i.intent_id)?);
    Ok(())
}

use copybot_storage_core::SqliteStore;
