//! Counter-controls for the repaired historical/owned-SELL fixture boundaries.
use super::{
    b53_fixture::Fixture, buy_retry_queue_fixture as q, buy_retry_queue_http_fixture::QueueRpc,
};
use anyhow::Result;

#[tokio::test]
async fn reserved_buy_cannot_be_relabelled_legacy_or_release_a_mismatched_receipt() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("reserved-a", 0)?;
    f.tick(0).await?;
    let a = f.order("reserved-a")?;
    let dispatch = f
        .store
        .load_execution_canary_dispatch(&a.order_id)?
        .unwrap();
    let err = f
        .conn()?
        .execute(
            "DELETE FROM execution_canary_dispatch WHERE order_id=?1",
            [&a.order_id],
        )
        .unwrap_err();
    assert_eq!(
        err.sqlite_error_code(),
        Some(rusqlite::ErrorCode::ConstraintViolation)
    );
    assert_eq!(
        f.store.load_execution_canary_dispatch(&a.order_id)?,
        Some(dispatch.clone())
    );
    // Model corruption without deleting the reservation or disabling foreign keys.
    f.conn()?.execute(
        "UPDATE execution_canary_dispatch SET message_sha256=?2 WHERE order_id=?1",
        rusqlite::params![a.order_id, "c".repeat(64)],
    )?;
    f.rpc.state.lock().unwrap().chain.insert(
        dispatch.tx_signature.clone(),
        super::b53_http::Chain::Settled,
    );
    f.reopen()?;
    f.tick(1).await?;
    use copybot_core_types::{Lamports, SignedLamports};
    use copybot_storage_core::*;
    let at = f.now + chrono::Duration::seconds(2);
    let wallet = f.config.canary_wallet_pubkey.clone();
    let token = super::priority_fee_route_fixture::TOKEN.to_owned();
    f.store.mark_execution_canary_confirmed_unreconciled(
        &a.order_id,
        &ExecutionCanaryReceiptProof {
            tx_signature: dispatch.tx_signature.clone(),
            wallet_pubkey: wallet.clone(),
            token: token.clone(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: at,
            reason: "synthetic_canonical_receipt".into(),
        },
        at,
    )?;
    let facts = ExecutionCanaryReceiptFacts {
        order_id: a.order_id.clone(),
        tx_signature: dispatch.tx_signature,
        wallet_pubkey: wallet.clone(),
        token,
        side: "buy".into(),
        slot: 42,
        wallet_native_pre: Lamports::new(80_000_000),
        wallet_native_post: Lamports::new(69_993_000),
        wallet_native_delta: SignedLamports::new(-10_007_000),
        transaction_fee: Some(Lamports::new(7000)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(wallet),
        token_delta: Some(ReceiptTokenDelta {
            raw: 123456,
            decimals: 0,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    };
    let error = f
        .store
        .record_execution_canary_receipt_facts(&facts, at)
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("tiny_budget_receipt_dispatch_conflict"),
        "{error:#}"
    );
    assert!(!f.store.execution_canary_fill_exists(&a.order_id)?);
    assert!(f.store.execution_canary_unresolved_buy()?);
    let held: (u64, Option<u64>) = f.conn()?.query_row(
        "SELECT fee_bound,actual_fee FROM execution_tiny_reservations WHERE order_id=?1",
        [&a.order_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(held, (100_000, None));
    assert_eq!(f.sends().len(), 1);
    f.finish().await
}

#[tokio::test]
async fn historical_unsigned_sell_unknown_keeps_inventory_and_never_resends() -> Result<()> {
    let mut f = q::queue_fixture("unknown-sell-hold", false).await?;
    f.config.canary_entry_submit_enabled = false;
    let id = q::add_sell(&f, true)?;
    let mut order = f.store.load_execution_canary_order(&id)?.unwrap();
    order.status = copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED.into();
    let metadata = f.store.load_execution_canary_build_plan_metadata(&id)?;
    let position = f.store.load_execution_canary_open_position(q::SELL_TOKEN)?;
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    for _ in 0..3 {
        super::buy_retry_safety_fixture::reopen(&mut f)?;
        f.sweep().await?;
        assert_eq!(
            f.store.load_execution_canary_order(&id)?,
            Some(order.clone())
        );
        assert_eq!(
            f.store.load_execution_canary_build_plan_metadata(&id)?,
            metadata
        );
        assert_eq!(
            f.store.load_execution_canary_open_position(q::SELL_TOKEN)?,
            position
        );
    }
    rpc.finish().await?;
    assert!(rpc.trace().is_empty());
    assert!(f.store.load_execution_canary_dispatch(&id)?.is_none());
    assert!(!f.store.execution_canary_fill_exists(&id)?);
    Ok(())
}
