//! Regression from the auditor probe; child of the existing signer-contract fixture.
//! Reuses its existing synthetic signed-envelope fixture; no RPC or real signing.
use super::*;
use crate::execution_signing_envelope::{
    ExecutionSerializedTransactionPayload, ExecutionSignedTransactionPayload,
};
use crate::execution_submit_adapter::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct MutationAdapter {
    path: PathBuf,
    mutate: bool,
    simulated: Arc<AtomicUsize>,
    signed: Arc<AtomicUsize>,
    submitted: Arc<AtomicUsize>,
}
impl ExecutionSubmitAdapter for MutationAdapter {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> {
        SignedEnvelopeAdapter.native_floor_config()
    }
    fn build_transaction_plan(
        &self,
        r: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        SignedEnvelopeAdapter.build_transaction_plan(r)
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        p: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            self.simulated.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            if self.mutate {
                // Explicit AFTER-setup corruption of the durable BUY identity.
                // The actual consumer still holds its original valid BUY request.
                let changed = rusqlite::Connection::open(&self.path)?.execute(
                    "UPDATE copy_signals SET side='sell' WHERE signal_id=?1 AND side='buy'",
                    [&p.signal_id],
                )?;
                assert_eq!(changed, 1);
            }
            SignedEnvelopeAdapter.simulate_transaction_plan(p).await
        })
    }
    fn sign_serialized_transaction(
        &self,
        r: &ExecutionSubmitRequest,
        p: &ExecutionTransactionPlan,
        bytes: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        self.signed.fetch_add(1, Ordering::SeqCst);
        SignedEnvelopeAdapter.sign_serialized_transaction(r, p, bytes)
    }
    fn plan_submit(&self, r: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        SignedEnvelopeAdapter.plan_submit(r)
    }
}

fn run_buy_consumer(mutate: bool) -> Result<()> {
    let path = unique_signer_contract_test_path(if mutate {
        "root40-mutated"
    } else {
        "root40-control"
    });
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = signer_contract_signal("buy", now);
    assert!(store.insert_copy_signal(&signal)?);
    record_signer_contract_quote(&store, &signal, now)?;
    // A nonempty legacy/import position, created through its supported writer API.
    // It is only a money-preservation control, not evidence of a source leader.
    store.record_execution_canary_open_position(
        "existing-import",
        &signal.token,
        2.0,
        Some(TokenQuantity::new(2_000, 3)),
        0.02,
        now,
    )?;
    let money_before = money_rows(&path)?;
    let position_before = store.load_execution_canary_open_position(&signal.token)?;
    assert!(position_before.is_some());
    let simulated = Arc::new(AtomicUsize::new(0));
    let signed = Arc::new(AtomicUsize::new(0));
    let submitted = Arc::new(AtomicUsize::new(0));
    let mut config = signer_contract_config();
    config.canary_max_open_positions = 2;
    config.canary_kill_switch_path = path.with_extension("absent-stop").to_string_lossy().into();
    let machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config,
        MutationAdapter {
            path: path.clone(),
            mutate,
            simulated: simulated.clone(),
            signed: signed.clone(),
            submitted: submitted.clone(),
        },
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(machine.process_buy_candidate(&store, &signal, now))
    }));
    assert_eq!(
        simulated.load(Ordering::SeqCst),
        1,
        "actual simulation must be reached"
    );
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    assert!(order.tx_signature.is_none());
    assert_eq!(
        store
            .load_copy_signal_by_signal_id(&signal.signal_id)?
            .unwrap()
            .side,
        if mutate { "sell" } else { "buy" }
    );
    let calls = (
        signed.load(Ordering::SeqCst),
        submitted.load(Ordering::SeqCst),
    );
    assert_eq!(calls, if mutate { (0, 0) } else { (1, 1) });
    assert!(
        outcome.is_ok(),
        "new source_refusal outcome must not panic its BUY consumer"
    );
    let summary = outcome.unwrap()?;
    assert_eq!(
        (
            summary.candidates,
            summary.reserved,
            summary.built,
            summary.simulated
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(
        (
            summary.failed,
            summary.submit_ready_rejected,
            summary.expired
        ),
        (0, 0, 0)
    );
    assert_eq!(
        (
            summary.sell_closed,
            summary.sell_partial,
            summary.sell_dust_closed
        ),
        (0, 0, 0)
    );
    assert_eq!(summary.source_sell_write_off_refusals.count(), 0);
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .unwrap();
    assert_eq!(metadata.signal_id, signal.signal_id);
    assert_eq!(metadata.quote_in_amount_raw.as_deref(), Some("10000000"));
    if mutate {
        // The immutable canonical ID still encodes BUY: the durable side change
        // is rejected by canonical association validation before request matching.
        let reason = "source_sell_state_unavailable";
        assert!(order.confirm_ts.is_none());
        assert_eq!(summary.source_sell_refusals.count(), 1);
        assert_eq!(summary.source_sell_refusals.reason(), reason);
        assert!(summary.source_sell_refusals.reason().len() <= 64);
        assert_eq!(summary.source_sell_refusals.order_id(), order.order_id);
        assert_eq!(
            summary.last_order_id.as_deref(),
            Some(order.order_id.as_str())
        );
        assert_eq!(summary.skipped_reason, Some(reason));
        assert_eq!(
            summary.last_error,
            Some(format!("{reason}: id={}", order.order_id))
        );
        assert_eq!(
            (summary.signing_envelope_built, summary.submit_disabled),
            (0, 0)
        );
        assert!(summary.last_signing_envelope_id.is_none());
        assert!(summary.last_signing_envelope_mode.is_none());
        assert!(summary.last_submit_idempotency_key.is_none());
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
        );
        assert!(order.err_code.is_none());
        assert_eq!(
            order.simulation_status.as_deref(),
            Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
        );
        assert_eq!(
            order.simulation_error.as_deref(),
            Some("serialized_transaction_base64_ready=true")
        );
        // Envelope/submit are in-memory outcomes in this consumer. No priority-fee
        // signing proof or submit terminal state may be persisted after refusal.
        assert_eq!(
            metadata.priority_fee_json,
            Some(crate::app_tests::priority_fee_fixture::total_json(12_345))
        );
    } else {
        assert_eq!(order.confirm_ts, Some(now));
        assert_eq!(summary.source_sell_refusals.count(), 0);
        assert!(summary.last_signing_envelope_id.is_some());
        assert!(summary.last_submit_idempotency_key.is_some());
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
        );
        assert_eq!(
            (summary.signing_envelope_built, summary.submit_disabled),
            (1, 1)
        );
        assert_eq!(summary.last_signing_envelope_mode.as_deref(),
            Some(crate::execution_signing_envelope::EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN));
    }
    assert_eq!(money_rows(&path)?, money_before);
    assert_eq!(
        store.load_execution_canary_open_position(&signal.token)?,
        position_before
    );
    drop(store);
    let reopened = SqliteStore::open(&path)?;
    assert_eq!(
        reopened.load_execution_canary_order(&order.order_id)?,
        Some(order)
    );
    assert_eq!(
        reopened.load_execution_canary_open_position(&signal.token)?,
        position_before
    );
    assert_eq!(money_rows(&path)?, money_before);
    drop(reopened);
    std::fs::remove_file(path)?;
    Ok(())
}

fn money_rows(path: &Path) -> Result<Vec<Vec<Vec<rusqlite::types::Value>>>> {
    let conn = rusqlite::Connection::open(path)?;
    [
        "positions",
        "fills",
        "execution_failed_expense_tasks",
        "execution_failed_expense_facts",
        "execution_failed_expense_ledger",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
        "execution_receipt_native_observations",
    ]
    .into_iter()
    .map(|table| {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))?;
        let columns = stmt.column_count();
        let rows = stmt
            .query_map([], |r| (0..columns).map(|i| r.get(i)).collect())?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .collect()
}

#[test]
fn source_guard_buy_consumer_healthy_control() -> Result<()> {
    run_buy_consumer(false)
}

#[test]
fn source_guard_buy_consumer_refusal_must_not_panic() -> Result<()> {
    run_buy_consumer(true)
}
