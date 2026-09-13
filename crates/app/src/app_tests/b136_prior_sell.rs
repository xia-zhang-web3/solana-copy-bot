use super::b136_fixture::Fixture;
use anyhow::Result;
use chrono::Utc;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
pub(super) fn failed_sell(f: &Fixture, n: usize) -> Result<()> {
    let now = Utc::now();
    let id = format!("b136-prior-{n}");
    let wallet = f.meta["our"]["signer"].as_str().unwrap();
    let token = f.meta["our"]["token_out"].as_str().unwrap();
    let signal = CopySignalRow {
        signal_id: id.clone(),
        wallet_id: f.meta["source"]["signer"].as_str().unwrap().into(),
        side: "sell".into(),
        token: token.into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    };
    f.db.store.insert_copy_signal(&signal)?;
    let o =
        f.db.store
            .reserve_execution_canary_order(&id, "tiny", now)?
            .order;
    f.db.store.mark_execution_canary_built(&o.order_id, now)?;
    let o = f.db.store.mark_execution_canary_simulated(
        &o.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let d = ExecutionCanaryDispatch {
        order_id: o.order_id.clone(),
        signal_id: id,
        client_order_id: o.client_order_id.clone(),
        route: o.route.clone(),
        attempt: o.attempt,
        wallet: wallet.into(),
        token: token.into(),
        side: "sell".into(),
        tx_signature: format!("synthetic-failed-{n}"),
        message_sha256: "c".repeat(64),
        transaction_sha256: "d".repeat(64),
    };
    let p = TinyBudgetClaim {
        experiment_id: "b136".into(),
        wallet: wallet.into(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: 10000,
        priority_fee: 5000,
        fee_slot: 151,
    };
    f.db.store
        .claim_tiny_experiment_dispatch(&o, &signal, &d, &p, now)?;
    let err = serde_json::json!({"InstructionError":[0,{"Custom":7}]});
    f.db.store.detect_failed_expense(
        &o.order_id,
        wallet,
        "signature_status",
        "confirmed",
        Some(151),
        &err,
        now,
    )?;
    f.db.store.apply_failed_expense(
        &o.order_id,
        &FailedTransactionFacts {
            tx_signature: d.tx_signature,
            wallet: wallet.into(),
            slot: 151,
            commitment: "confirmed".into(),
            transaction_error: err,
            transaction_fee_lamports: Some("10000".into()),
            fee_coverage: FailedExpenseCoverage::Known,
            payer: Some(wallet.into()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: Some("100000000".into()),
            wallet_native_post_lamports: Some("99990000".into()),
            native_coverage: FailedExpenseCoverage::Known,
        },
        now,
    )?;
    Ok(())
}
