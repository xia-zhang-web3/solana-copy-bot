//! Financial boundary checks with explicit loopback SOURCE and synthetic BOT facts.
use super::owner_source_gate_fixture::{GateCase, EXIT};
use anyhow::Result;
use chrono::Utc;

#[tokio::test]
async fn owner_source_gate_optional_destination_sell_fails_before_sign_send_without_retry(
) -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    // Valid accepted OWNER optional account, but outside the closed SOURCE grammar.
    c.source.state.lock().unwrap().mode = "sell_optional_destination";
    c.source.tick().await?;
    let order = c
        .source
        .store
        .load_execution_canary_order(&owner_exit_order_id(EXIT))?
        .unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert!(
        order
            .simulation_error
            .as_deref()
            .unwrap_or("")
            .contains("source_copy_sell_profile_unsupported"),
        "{order:?}"
    );
    assert!(c
        .source
        .store
        .load_execution_canary_dispatch(&order.order_id)?
        .is_none());
    assert_eq!(c.source.calls("sendTransaction"), 1);
    assert_eq!(c.source.calls("simulateTransaction"), 1);
    assert_eq!(c.source.store.execution_canary_open_position_count()?, 1);
    c.source.state.lock().unwrap().mode = "";
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 1);
    assert_eq!(c.source.calls("quote"), 2);
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_unsupported_classic_sell_fails_before_sign_send_without_retry(
) -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    c.source.state.lock().unwrap().mode = "sell_classic_variant";
    c.source.tick().await?;
    let order = c
        .source
        .store
        .load_execution_canary_order(&owner_exit_order_id(EXIT))?
        .unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert!(
        order
            .simulation_error
            .as_deref()
            .unwrap_or("")
            .contains("source_copy_sell_profile_unsupported"),
        "{order:?}"
    );
    assert!(c
        .source
        .store
        .load_execution_canary_dispatch(&order.order_id)?
        .is_none());
    assert_eq!(c.source.calls("sendTransaction"), 1);
    assert_eq!(c.source.store.execution_canary_open_position_count()?, 1);
    c.source.state.lock().unwrap().mode = "";
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 1);
    assert_eq!(c.source.calls("quote"), 2);
    Ok(())
}
use copybot_storage_core::{owner_exit_order_id, SqliteStore};

#[tokio::test]
async fn owner_source_gate_waits_for_canonical_bot_buy_then_exits_own_quantity_once() -> Result<()>
{
    let mut c = GateCase::new().await?;
    assert!(
        crate::execution_owner_roundtrip::exit_config(&c.source.config, &c.source.store)?.is_none()
    );
    c.source.tick().await?;
    c.assert_held()?;
    c.source.tick().await?;
    c.assert_held()?;
    c.seed_bot()?;
    assert!(c.ready()?);
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 2);
    assert_eq!(c.source.calls("quote"), 2);
    let exit = c.source.store.load_owner_exit_intent(EXIT)?.unwrap();
    assert_eq!(exit.amount_raw, super::owner_roundtrip_fixture::RECEIVED);
    assert_eq!(c.source.store.execution_canary_open_position_count()?, 0);
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 2);
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_unknown_unaccounted_wrong_wallet_and_quantity_do_not_release(
) -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    assert!(c.ready()?);
    let id = c.bot_order.as_ref().unwrap();
    let sql = c.sql()?;
    for status in [
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
    ] {
        sql.execute(
            "UPDATE orders SET status=?1 WHERE order_id=?2",
            rusqlite::params![status, id],
        )?;
        c.assert_held()?;
    }
    sql.execute(
        "UPDATE orders SET status=?1 WHERE order_id=?2",
        rusqlite::params![copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED, id],
    )?;
    sql.execute("UPDATE execution_canary_receipt_proofs SET reason='confirmed_accounting_pending' WHERE order_id=?1",[id])?;
    c.assert_held()?;
    sql.execute(
        "UPDATE execution_canary_receipt_proofs SET reason='accounting_complete' WHERE order_id=?1",
        [id],
    )?;
    sql.execute(
        "UPDATE execution_canary_receipt_facts SET wallet_pubkey='foreign-bot' WHERE order_id=?1",
        [id],
    )?;
    c.assert_held()?;
    let bot = &c
        .source
        .config
        .owner_technical_buy
        .as_ref()
        .unwrap()
        .source_copy_exit_gate
        .as_ref()
        .unwrap()
        .bot_wallet_pubkey;
    sql.execute(
        "UPDATE execution_canary_receipt_facts SET wallet_pubkey=?1 WHERE order_id=?2",
        rusqlite::params![bot, id],
    )?;
    sql.execute("UPDATE positions SET qty_raw='1' WHERE state='open'", [])?;
    c.assert_held()?;
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_stop_and_expired_run_hold_even_with_confirmed_bot_buy() -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    assert!(c.ready()?);
    let g = c
        .source
        .config
        .owner_technical_buy
        .as_ref()
        .unwrap()
        .source_copy_exit_gate
        .as_ref()
        .unwrap();
    std::fs::write(&g.bot_kill_switch_path, b"STOP")?;
    c.assert_held()?;
    std::fs::remove_file(&g.bot_kill_switch_path)?;
    std::fs::write(&c.source.config.canary_kill_switch_path, b"STOP")?;
    c.assert_held()?;
    std::fs::remove_file(&c.source.config.canary_kill_switch_path)?;
    let expires = chrono::DateTime::parse_from_rfc3339(
        &c.source
            .config
            .owner_technical_buy
            .as_ref()
            .unwrap()
            .expires_at,
    )?
    .with_timezone(&Utc);
    assert!(!crate::execution_owner_source_gate::ready(
        &c.source.config,
        &c.source.store,
        expires
    )?);
    c.sql()?.execute(
        "UPDATE execution_tiny_experiment SET state='stopped',stop_reason='mock_stop_open'",
        [],
    )?;
    c.assert_held()?;
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_binding_removal_or_rebind_cannot_release_after_buy() -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    assert!(c.ready()?);
    let accepted = c.source.config.clone();
    c.source
        .config
        .owner_technical_buy
        .as_mut()
        .unwrap()
        .source_copy_exit_gate = None;
    assert!(c
        .ready()
        .unwrap_err()
        .to_string()
        .contains("parent_changed"));
    c.source.config = accepted.clone();
    c.source
        .config
        .owner_technical_buy
        .as_mut()
        .unwrap()
        .source_copy_exit_gate
        .as_mut()
        .unwrap()
        .bot_run_id = "old-run".into();
    assert!(c.ready().is_err());
    c.source.config = accepted;
    // A foreign SOURCE signature cannot match the current SOURCE receipt.
    let mut foreign = GateCase::new().await?;
    foreign.source.tick().await?;
    foreign.seed_bot_with_signature(Some(&bs58::encode([17; 64]).into_string()))?;
    foreign.assert_held()?;
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_unknown_exit_reconciles_after_bot_stop_without_resend() -> Result<()> {
    let mut c = GateCase::new().await?;
    c.source.tick().await?;
    c.seed_bot()?;
    c.source.state.lock().unwrap().mode = "unknown_sell";
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 2);
    let g = c
        .source
        .config
        .owner_technical_buy
        .as_ref()
        .unwrap()
        .source_copy_exit_gate
        .as_ref()
        .unwrap();
    std::fs::write(&g.bot_kill_switch_path, b"STOP")?;
    let reopened = SqliteStore::open(c.source.root.path().join("state.db"))?;
    let mut stopped = c.source.config.clone();
    stopped.owner_technical_buy.as_mut().unwrap().activate = false;
    crate::execution_canary::ExecutionCanaryRunner::new(stopped.clone())
        .process_tick(&reopened, Utc::now())
        .await?;
    assert!(!reopened.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
    c.source.state.lock().unwrap().mode = "";
    crate::execution_canary::ExecutionCanaryRunner::new(stopped)
        .process_tick(&reopened, Utc::now())
        .await?;
    assert!(reopened.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
    assert_eq!(c.source.calls("sendTransaction"), 2);
    assert_eq!(c.source.calls("quote"), 2);
    Ok(())
}

#[tokio::test]
async fn owner_source_gate_config_rejects_same_wallet_and_reader_rejects_writable_db() -> Result<()>
{
    let mut c = GateCase::new().await?;
    let p = c.source.config.owner_technical_buy.as_mut().unwrap();
    p.source_copy_exit_gate.as_mut().unwrap().bot_wallet_pubkey = p.wallet_pubkey.clone();
    assert!(copybot_config::validate_owner_technical_buy(&c.source.config).is_err());
    // Reader's financial predicate is unusable through the writable BOT handle.
    let b = copybot_storage_core::owner_source_copy_gate::SourceCopyGateBinding {
        source_wallet: "source",
        source_signature: "signature",
        source_slot: 1,
        source_mint: "mint",
        source_input_lamports: 1,
        source_output_raw: 1,
        source_decimals: 6,
        genesis_hash: "genesis",
        bot_wallet: "bot",
        bot_run_id: "run",
        bot_policy_identity: "identity",
        activated_at: Utc::now(),
        expires_at: Utc::now(),
    };
    assert!(c
        .bot
        .confirmed_native_bot_buy_for_source(&b, Utc::now())
        .unwrap_err()
        .to_string()
        .contains("requires_read_only"));
    Ok(())
}
