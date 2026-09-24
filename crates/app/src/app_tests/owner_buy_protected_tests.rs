//! Causal daemon checks: IDL-shaped Jupiter BUY goes through the real signer path.
use super::{owner_buy_protected_fixture::Case, owner_buy_wire_fixture as wire};
use anyhow::Result;
use chrono::Utc;

#[tokio::test]
async fn owner_protected_real_adapter_preserves_wire_amount_floor_and_receipt() -> Result<()> {
    let c = Case::new().await?;
    let result = c.tick().await?;
    assert_eq!(
        c.calls("sendTransaction"),
        1,
        "{result:?} order={:?} calls={:?}",
        c.store.load_execution_canary_order(&c.order_id())?,
        c.state.lock().unwrap().calls
    );
    assert_eq!(
        super::b135_hooks::count(&c.config.execution_signer_keypair_path),
        1
    );
    let state = c.state.lock().unwrap();
    let simulated = crate::execution_transaction_wire::decode_message(
        state.simulated.as_ref().unwrap(),
        |_| Ok(()),
    )?;
    let sent = crate::execution_transaction_wire::decode_message(
        state.sent.as_ref().unwrap(),
        |_| Ok(()),
    )?;
    assert_eq!(simulated.binding.message_bytes, sent.binding.message_bytes);
    drop(state);
    let expected_floor =
        crate::execution_native_floor_policy::reserve_lamports(c.config.pretrade_min_sol_reserve)?
            .max(985_000_000);
    let policy =
        c.store
            .tiny_native_policy("owner-test-run", &c.config.canary_wallet_pubkey, Utc::now())?;
    assert_eq!(policy.floor_lamports, expected_floor);
    let row: (u64,u64,String,Option<u64>,String) = c.sql()?.query_row(
        "SELECT r.buy_lamports,e.requested_lamports,e.floor_lamports,r.actual_fee,r.outcome FROM execution_tiny_reservations r JOIN execution_tiny_capital_evidence e USING(order_id) WHERE r.order_id=?1",
        [c.order_id()],|r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    assert_eq!(
        row,
        (
            10_000_000,
            10_000_000,
            expected_floor.to_string(),
            Some(19_000),
            "successful".into()
        )
    );
    let facts = c
        .store
        .load_execution_canary_receipt_facts(&c.order_id())?
        .expect("receipt");
    assert_eq!(facts.token, wire::USDC);
    assert_eq!(facts.token_delta.unwrap().raw, i128::from(wire::OUT));
    assert_eq!(facts.transaction_fee.unwrap().as_u64(), 19_000);
    let position = c
        .store
        .load_execution_canary_open_position(wire::USDC)?
        .expect("position");
    assert_eq!(position.cost_lamports.unwrap().as_u64(), 12_058_280);
    c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1);
    assert_eq!(
        c.sql()?
            .query_row("SELECT count(*) FROM copy_signals", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn owner_protected_wire_mutations_never_reach_signer_or_dispatch() -> Result<()> {
    for mode in [
        "wrong_input",
        "wrong_destination",
        "wrong_mint",
        "wrong_slippage",
        "trailing_bytes",
    ] {
        let c = Case::new().await?;
        c.state.lock().unwrap().mode = mode;
        let result = c.tick().await;
        assert!(c.calls("instructions") > 0, "{mode}: {result:?}");
        assert_eq!(
            super::b135_hooks::count(&c.config.execution_signer_keypair_path),
            0,
            "{mode}: {result:?}"
        );
        assert_eq!(c.calls("sendTransaction"), 0, "{mode}: {result:?}");
        assert!(c
            .store
            .load_execution_canary_dispatch(&c.order_id())?
            .is_none());
    }
    Ok(())
}

#[tokio::test]
async fn owner_protected_authority_revoked_during_anchor_does_not_activate() -> Result<()> {
    let c = Case::new().await?;
    c.state.lock().unwrap().mode = "kill_on_anchor";
    assert!(c.tick().await.is_err());
    assert_eq!(c.calls("getMultipleAccounts"), 1);
    assert!(c.store.load_tiny_experiment(Utc::now())?.is_none());
    assert_eq!(
        super::b135_hooks::count(&c.config.execution_signer_keypair_path),
        0
    );
    assert_eq!(c.calls("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn owner_protected_unknown_restart_only_reconciles_original_dispatch() -> Result<()> {
    let mut c = Case::new().await?;
    c.state.lock().unwrap().mode = "unknown";
    let result = c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{result:?}");
    assert!(c
        .store
        .load_execution_canary_dispatch(&c.order_id())?
        .is_some());
    assert!(!c.store.execution_canary_fill_exists(&c.order_id())?);
    assert!(c.store.execution_canary_unresolved_buy()?);
    c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1);
    c.state.lock().unwrap().mode = "ok";
    c.config.owner_technical_buy.as_mut().unwrap().activate = false;
    c.config.owner_technical_buy.as_mut().unwrap().expires_at =
        (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    std::fs::write(&c.config.canary_kill_switch_path, "stop")?;
    c.store = copybot_storage_core::SqliteStore::open(c.root.path().join("state.db"))?;
    let recovered = c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{recovered:?}");
    assert_eq!(
        super::b135_hooks::count(&c.config.execution_signer_keypair_path),
        1
    );
    assert!(c.store.execution_canary_fill_exists(&c.order_id())?);
    assert_eq!(
        c.sql()?.query_row(
            "SELECT count(*) FROM execution_tiny_reservations",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    Ok(())
}
