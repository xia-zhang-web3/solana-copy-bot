use super::priority_fee_route_fixture::{Fixture, Route, TOKEN};
use anyhow::Result;
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;

pub(super) async fn fixture() -> Result<Fixture> {
    let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
    f.config = super::b126_config_fixture::activated(&f.config)?;
    Ok(f)
}
pub(super) fn finish_buy(f: &Fixture, fee: u64) -> Result<()> {
    let order = f
        .store
        .load_execution_canary_order(&f.request.order_id)?
        .unwrap();
    let sig = order.tx_signature.unwrap();
    f.store.mark_execution_canary_confirmed_unreconciled(
        &order.order_id,
        &ExecutionCanaryReceiptProof {
            tx_signature: sig.clone(),
            wallet_pubkey: f.config.canary_wallet_pubkey.clone(),
            token: TOKEN.into(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: f.now,
            reason: "pending".into(),
        },
        f.now,
    )?;
    let facts = ExecutionCanaryReceiptFacts {
        order_id: order.order_id.clone(),
        tx_signature: sig,
        wallet_pubkey: f.config.canary_wallet_pubkey.clone(),
        token: TOKEN.into(),
        side: "buy".into(),
        slot: 42,
        wallet_native_pre: Lamports::new(100_000_000),
        wallet_native_post: Lamports::new(90_000_000 - fee),
        wallet_native_delta: SignedLamports::new(-10_000_000 - i128::from(fee)),
        transaction_fee: Some(Lamports::new(fee)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(f.config.canary_wallet_pubkey.clone()),
        token_delta: Some(ReceiptTokenDelta {
            raw: 123456,
            decimals: 0,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: Some(f.now.timestamp()),
        decomposition: ReceiptDecomposition::Unresolved,
    };
    f.store
        .record_execution_canary_receipt_facts(&facts, f.now)?;
    f.store.confirm_execution_canary_buy_fill(
        &order.order_id,
        TOKEN,
        123456.0,
        Some(TokenQuantity::new(123456, 0)),
        0.010_005,
        f.now,
        f.now,
        Some(Lamports::new(10_000_000 + fee)),
    )?;
    Ok(())
}
pub(super) fn sell(f: &mut Fixture, id: &str) -> Result<()> {
    f.now += chrono::Duration::seconds(1);
    let signal = CopySignalRow {
        signal_id: id.into(),
        wallet_id: "leader".into(),
        side: "sell".into(),
        token: TOKEN.into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: f.now,
        status: "shadow_recorded".into(),
    };
    f.store.insert_copy_signal(&signal)?;
    let order = f
        .store
        .reserve_execution_canary_order(id, &f.config.canary_route, f.now)?
        .order;
    f.request.order_id = order.order_id;
    f.request.signal_id = id.into();
    f.request.client_order_id = order.client_order_id;
    f.request.attempt = order.attempt;
    f.request.side = "sell".into();
    f.request.metadata.quote_in_amount_raw = Some("123456".into());
    f.request.metadata.quote_out_amount_raw = Some("10000000".into());
    let mut q: serde_json::Value =
        serde_json::from_str(f.request.metadata.quote_response_json.as_deref().unwrap())?;
    q["inputMint"] = serde_json::json!(TOKEN);
    q["outputMint"] = serde_json::json!("So11111111111111111111111111111111111111112");
    q["inAmount"] = serde_json::json!("123456");
    q["outAmount"] = serde_json::json!("10000000");
    q["otherAmountThreshold"] = serde_json::json!("9500000");
    f.request.metadata.quote_response_json = Some(q.to_string());
    f.wire.lock().unwrap().blockhash += 1;
    super::owned_sell_fixture::bind(&f.store, &mut f.request, 123456, 0)?;
    Ok(())
}
pub(super) fn failed(f: &Fixture, fee: u64) -> Result<()> {
    let id = &f.request.order_id;
    let wallet = &f.config.canary_wallet_pubkey;
    let error = serde_json::json!({"InstructionError":[0,{"Custom":7}]});
    let task = f.store.detect_failed_expense(
        id,
        wallet,
        "signature_status",
        "confirmed",
        Some(42),
        &error,
        f.now,
    )?;
    f.store.apply_failed_expense(
        id,
        &FailedTransactionFacts {
            tx_signature: task.tx_signature,
            wallet: wallet.clone(),
            slot: 42,
            commitment: "confirmed".into(),
            transaction_error: error,
            transaction_fee_lamports: Some(fee.to_string()),
            fee_coverage: FailedExpenseCoverage::Known,
            payer: Some(wallet.clone()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: Some("100000000".into()),
            wallet_native_post_lamports: Some((100000000 - fee).to_string()),
            native_coverage: FailedExpenseCoverage::Known,
        },
        f.now,
    )
}
