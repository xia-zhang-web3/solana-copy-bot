use super::*;

#[tokio::test]
async fn execution_canary_rejects_submit_ready_intent_in_dry_run() -> Result<()> {
    let db_path = unique_submit_contract_test_path("ready-rejected");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = submit_contract_signal("ready-rejected", now);
    store.insert_copy_signal(&signal)?;
    record_submit_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        submit_contract_config(),
        SubmitReadyAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(summary.submit_ready_rejected, 1);
    assert_eq!(
        summary.skipped_reason,
        Some("submit_ready_rejected_in_dry_run")
    );
    let expected_idempotency_key = expected_submit_key(&order.client_order_id);
    let expected_envelope_id = format!("copybot:sign:{}:attempt:1", order.client_order_id);
    assert_eq!(
        summary.last_submit_idempotency_key.as_deref(),
        Some(expected_idempotency_key.as_str())
    );
    assert_eq!(
        summary.last_signing_envelope_id.as_deref(),
        Some(expected_envelope_id.as_str())
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SUBMIT_DISABLED)
    );
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_marks_bad_signing_envelope_as_failed() -> Result<()> {
    let db_path = unique_submit_contract_test_path("bad-signing-envelope");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = submit_contract_signal("bad-signing-envelope", now);
    store.insert_copy_signal(&signal)?;
    record_submit_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        submit_contract_config(),
        BadSigningEnvelopeAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.signing_envelope_built, 0);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
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
        .contains("signing envelope idempotency key mismatch"));
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_marks_mismatched_submit_idempotency_as_failed() -> Result<()> {
    let db_path = unique_submit_contract_test_path("bad-idempotency");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = submit_contract_signal("bad-idempotency", now);
    store.insert_copy_signal(&signal)?;
    record_submit_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        submit_contract_config(),
        BadIdempotencyAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.failed, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SUBMIT_PLAN_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("submit idempotency key mismatch"));
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_marks_malformed_submit_ready_payload_as_failed() -> Result<()> {
    let db_path = unique_submit_contract_test_path("malformed-submit-payload");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = submit_contract_signal("malformed-submit-payload", now);
    store.insert_copy_signal(&signal)?;
    record_submit_contract_quote(&store, &signal, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        submit_contract_config(),
        MalformedSubmitReadyPayloadAdapter,
    );

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.failed, 1);
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(summary.submit_ready_rejected, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SUBMIT_PLAN_FAILED)
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

fn submit_contract_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn expected_submit_key(client_order_id: &str) -> String {
    format!("copybot:submit:{client_order_id}:attempt:1")
}

fn signed_transaction_payload() -> String {
    "AQIDBA==".to_string()
}

fn tx_signature_hint() -> String {
    "canary_tx_signature_hint".to_string()
}

fn submit_contract_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-submit-contract:leader-wallet:{signal_id}:TokenMint"),
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

fn record_submit_contract_quote(
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

#[derive(Debug, Clone)]
struct SubmitReadyAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for SubmitReadyAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: None,
            })
        })
    }

    fn build_signing_envelope(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
        crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
            request,
            plan,
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: signed_transaction_payload(),
                tx_signature_hint: Some(tx_signature_hint()),
            },
        )
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        let plan = crate::execution_submit_adapter::NoSubmitExecutionAdapter
            .build_transaction_plan(request)?;
        let envelope = self.build_signing_envelope(request, &plan)?;
        let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
            request,
            &envelope,
            "rpc".to_string(),
        )?;
        Ok(crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(intent))
    }
}

#[derive(Debug, Clone)]
struct BadSigningEnvelopeAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for BadSigningEnvelopeAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: None,
            })
        })
    }

    fn build_signing_envelope(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        _plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
        Ok(
            crate::execution_signing_envelope::ExecutionSigningEnvelope {
                envelope_id: "copybot:sign:bad".to_string(),
                idempotency_key: "wrong-key".to_string(),
                mode: crate::execution_signing_envelope::EXECUTION_SIGNING_ENVELOPE_MODE_DRY_RUN
                    .to_string(),
                payload_kind: "execution_transaction_plan_v1".to_string(),
                payload_fingerprint: "fnv64:0000000000000001".to_string(),
                submit_enabled: false,
                serialized_transaction_base64: None,
                signed_transaction_base64: None,
                tx_signature_hint: None,
            },
        )
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.plan_submit(request)
    }
}

#[derive(Debug, Clone)]
struct BadIdempotencyAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for BadIdempotencyAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: None,
            })
        })
    }

    fn plan_submit(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        Ok(
            crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitDisabled {
                idempotency_key: "wrong-key".to_string(),
                reason: "bad_key".to_string(),
            },
        )
    }
}

#[derive(Debug, Clone)]
struct MalformedSubmitReadyPayloadAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter
    for MalformedSubmitReadyPayloadAdapter
{
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: None,
            })
        })
    }

    fn build_signing_envelope(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
        crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
            request,
            plan,
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: signed_transaction_payload(),
                tx_signature_hint: Some(tx_signature_hint()),
            },
        )
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        Ok(
            crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(
                crate::execution_submit_adapter::ExecutionSubmitIntent {
                    idempotency_key:
                        crate::execution_submit_adapter::execution_submit_idempotency_key(request),
                    submit_route: "rpc".to_string(),
                    signed_transaction_base64: "not base64".to_string(),
                    tx_signature_hint: Some(tx_signature_hint()),
                },
            ),
        )
    }
}

fn unique_submit_contract_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-submit-contract-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
