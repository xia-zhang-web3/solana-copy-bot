use super::source_sell_producer_fixture::Fixture;
use anyhow::{ensure, Context, Result};
use copybot_storage_core::*;

#[tokio::test]
async fn source_sell_producer_ingress_commit_restart_reaches_existing_execution() -> Result<()> {
    let mut f = Fixture::new().await?;
    let staged = f.stage("durable-exit").await?;
    let position = f.f.position()?;
    assert_eq!(staged.position_id, position);
    assert!(f.f.store.list_active_follow_wallets()?.is_empty());
    f.f.reopen()?;
    let outcome = f.tick().await;
    f.finish().await?;
    let summary = outcome?;
    let signal_id = "shadow:durable-exit:source-a:sell:mint";
    let signal = f.f.store.load_copy_signal_by_signal_id(signal_id)?;
    assert!(
        signal.is_some(),
        "durable staging must produce a runnable owned SELL: {summary:?}"
    );
    let signal = signal.unwrap();
    assert_eq!(signal.ts, staged.event.ts_utc);
    assert_eq!(signal.wallet_id, staged.event.wallet);
    assert_eq!(summary.quote_close_inserted, 1, "{summary:?}");
    assert_eq!(f.rpc.count("simulateTransaction"), 1, "{summary:?}");
    assert_eq!(f.rpc.count("sendTransaction"), 1, "{summary:?}");
    let order =
        f.f.store
            .load_execution_canary_order_by_signal(signal_id)?
            .unwrap();
    assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(
        order.tx_signature.as_deref(),
        Some(sent_signature(&f)?.as_str())
    );
    assert_eq!(f.f.position()?, position);
    assert_eq!(f.f.staged("durable-exit")?.unwrap().position_id, position);
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_existing_keeps_quote_order_retry_and_original_generation(
) -> Result<()> {
    let mut f = Fixture::new().await?;
    let staged = f.stage("retry-existing").await?;
    f.rpc.state.lock().unwrap().error_at = Some("sendTransaction");
    let first = f.tick().await?;
    assert_eq!(first.source_sell_production.inserted, 1);
    let id = "shadow:retry-existing:source-a:sell:mint";
    let order =
        f.f.store
            .load_execution_canary_order_by_signal(id)?
            .unwrap();
    assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(
        order.tx_signature.as_deref(),
        Some(sent_signature(&f)?.as_str())
    );
    assert_eq!(
        order.simulation_error.as_deref(),
        Some("dispatch_outcome_unknown")
    );
    let dispatch =
        f.f.store
            .load_execution_canary_dispatch(&order.order_id)?
            .unwrap();
    assert_eq!(
        dispatch.tx_signature,
        order.tx_signature.as_deref().unwrap()
    );
    let transport_note: String = f.f.conn()?.query_row(
        "SELECT transport_note FROM execution_canary_dispatch WHERE order_id=?1",
        [&order.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(transport_note, "dispatch_rpc_rejection_unknown");
    let before = f.f.money()?;
    f.config.canary_tiny_submit_enabled = false; // Isolate producer replay; unknown dispatch cannot authorize resend.
    f.f.reopen()?;
    let second = f.tick().await?;
    f.finish().await?;
    assert_eq!(
        (
            second.source_sell_production.inserted,
            second.source_sell_production.existing
        ),
        (0, 1)
    );
    assert_eq!(
        f.f.store.load_execution_canary_order_by_signal(id)?,
        Some(order)
    );
    assert_eq!(
        f.rpc.count("sendTransaction"),
        1,
        "unknown outcome must not resend"
    );
    assert_eq!(f.f.money()?, before);
    assert_eq!(
        f.f.staged("retry-existing")?.unwrap().position_id,
        staged.position_id
    );
    assert_eq!(
        f.f.staged("retry-existing")?.unwrap().event.ts_utc,
        staged.event.ts_utc
    );
    assert_eq!(
        f.f.conn()?.query_row(
            "SELECT count(*) FROM execution_source_sell_promotions",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_pending_receipt_preserves_reconciliation_and_blocks_second_submit(
) -> Result<()> {
    let mut f = Fixture::new().await?;
    f.stage("pending-a").await?;
    let first = f.tick().await?;
    assert_eq!(first.source_sell_production.inserted, 1);
    let id = "shadow:pending-a:source-a:sell:mint";
    let order =
        f.f.store
            .load_execution_canary_order_by_signal(id)?
            .unwrap();
    assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    let signatures_before = f.rpc.count("getSignatureStatuses");
    let submitted_before = f.rpc.count("sendTransaction");
    let simulations_before = f.rpc.count("simulateTransaction");
    let position = f.f.store.load_execution_canary_open_position("mint")?;
    f.stage("pending-b").await?;
    f.f.reopen()?;
    let second = f.tick().await?;
    f.finish().await?;
    assert_eq!(
        (
            second.source_sell_production.inserted,
            second.source_sell_production.existing
        ),
        (1, 1)
    );
    assert!(
        f.rpc.count("getSignatureStatuses") > signatures_before,
        "reconciliation still runs"
    );
    assert_eq!(f.rpc.count("sendTransaction"), submitted_before);
    assert_eq!(f.rpc.count("simulateTransaction"), simulations_before);
    assert_eq!(
        f.f.store
            .load_execution_canary_order_by_signal(id)?
            .unwrap()
            .tx_signature,
        order.tx_signature
    );
    assert!(f
        .f
        .store
        .load_execution_canary_order_by_signal("shadow:pending-b:source-a:sell:mint")?
        .is_none());
    assert_eq!(
        f.f.store.load_execution_canary_open_position("mint")?,
        position
    );
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_quote_only_needs_no_tiny_submit_flag() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.config.canary_tiny_submit_enabled = false;
    f.stage("quote-only").await?;
    let tick = f.tick().await?;
    f.finish().await?;
    assert_eq!(tick.source_sell_production.inserted, 1);
    assert_eq!(tick.quote_close_inserted, 1);
    assert_eq!(f.rpc.count("simulateTransaction"), 0);
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    assert!(f
        .f
        .store
        .load_execution_canary_order_by_signal("shadow:quote-only:source-a:sell:mint")?
        .is_none());
    Ok(())
}

// Read and verify the synthetic transaction actually sent by the production path.
// The server's response label is not the signature committed by dispatch53.
fn sent_signature(f: &Fixture) -> Result<String> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let state = f.rpc.state.lock().unwrap();
    let sends: Vec<_> = state
        .calls
        .iter()
        .filter(|(m, _)| m == "sendTransaction")
        .collect();
    ensure!(sends.len() == 1, "exactly one send in this fixture");
    let wire = sends[0].1["params"][0]
        .as_str()
        .context("signed transaction payload")?;
    let bytes = STANDARD.decode(wire)?;
    ensure!(
        bytes.len() > 65 && bytes[0] == 1,
        "single-signature fixture framing"
    );
    let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65])?;
    let payer: [u8; 32] = bs58::decode(&f.config.canary_wallet_pubkey)
        .into_vec()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("fixture payer length"))?;
    ed25519_dalek::VerifyingKey::from_bytes(&payer)?.verify_strict(&bytes[65..], &signature)?;
    Ok(bs58::encode(signature.to_bytes()).into_string())
}
