use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;

#[tokio::test]
async fn execution_canary_signer_contract_builds_signed_envelope() -> Result<()> {
    let db_path = unique_signer_contract_test_path("signed-envelope");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = signer_contract_signal("signed-envelope", now);
    store.insert_copy_signal(&signal)?;
    record_signer_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        signer_contract_config(),
        SignedEnvelopeAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        summary.last_signing_envelope_mode.as_deref(),
        Some(
            crate::execution_signing_envelope::EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN
        )
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_signer_contract_rejects_bad_signed_payload() -> Result<()> {
    let db_path = unique_signer_contract_test_path("bad-signed-payload");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = signer_contract_signal("bad-signed-payload", now);
    store.insert_copy_signal(&signal)?;
    record_signer_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        signer_contract_config(),
        BadSignedPayloadAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 0);
    assert_eq!(summary.failed, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("signed transaction base64"));
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[derive(Debug, Clone)]
struct SignedEnvelopeAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for SignedEnvelopeAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        signer_contract_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            store_serialized_payload(plan)?;
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: Some("serialized_transaction_base64_ready=true".to_string()),
            })
        })
    }

    fn sign_serialized_transaction(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        _plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
        payload: &crate::execution_signing_envelope::ExecutionSerializedTransactionPayload,
    ) -> Result<Option<crate::execution_signing_envelope::ExecutionSignedTransactionPayload>> {
        assert_eq!(payload.source, "test_metis_swap_transaction");
        assert_eq!(payload.serialized_transaction_base64, serialized_payload());
        Ok(Some(
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: signed_payload(),
                tx_signature_hint: Some("canary_signed_tx_hint".to_string()),
            },
        ))
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.plan_submit(request)
    }
}

#[derive(Debug, Clone)]
struct BadSignedPayloadAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for BadSignedPayloadAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        signer_contract_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            store_serialized_payload(plan)?;
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: Some("serialized_transaction_base64_ready=true".to_string()),
            })
        })
    }

    fn sign_serialized_transaction(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        _plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
        _payload: &crate::execution_signing_envelope::ExecutionSerializedTransactionPayload,
    ) -> Result<Option<crate::execution_signing_envelope::ExecutionSignedTransactionPayload>> {
        Ok(Some(
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: "not base64".to_string(),
                tx_signature_hint: Some("canary_signed_tx_hint".to_string()),
            },
        ))
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.plan_submit(request)
    }
}

fn signer_contract_plan(
    request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
    let mut plan = crate::execution_submit_adapter::NoSubmitExecutionAdapter
        .build_transaction_plan(request)?;
    plan.serialized_transaction_payload_slot = Some(
        crate::execution_serialized_transaction_slot::ExecutionSerializedTransactionPayloadSlot::new(
        ),
    );
    Ok(plan)
}

fn store_serialized_payload(
    plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
) -> Result<()> {
    let slot = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("test plan should carry serialized payload slot");
    slot.store(
        crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
            source: "test_metis_swap_transaction".to_string(),
            serialized_transaction_base64: serialized_payload(),
        },
    )
}

fn signer_contract_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn serialized_payload() -> String {
    "AQIDBA==".to_string()
}

fn signed_payload() -> String {
    "BQYHCA==".to_string()
}

fn signer_contract_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-signer-contract:leader-wallet:{signal_id}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn record_signer_contract_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(7),
            quote_latency_ms: Some(11),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(12_345),
            priority_fee_json: Some("{\"recommended\":12345}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_signer_contract_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-signer-contract-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
