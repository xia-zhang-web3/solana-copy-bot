use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;

#[tokio::test]
async fn execution_canary_file_signer_signs_serialized_transaction_from_keypair() -> Result<()> {
    let keypair = file_signer_test_keypair(7);
    let keypair_path = write_file_signer_keypair_file("signed", &keypair.keypair_bytes)?;
    let db_path = unique_file_signer_test_path("signed-envelope");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = file_signer_signal("signed-envelope", now);
    store.insert_copy_signal(&signal)?;
    record_file_signer_quote(&store, &signal, now)?;
    let config = file_signer_config(&keypair.pubkey_string, &keypair_path);
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config.clone(),
        FileSignerAdapter {
            config,
            serialized_transaction_base64: serialized_legacy_transaction(keypair.public_key),
        },
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
    assert_eq!(summary.submit_ready_rejected, 1);
    assert_eq!(
        summary.skipped_reason,
        Some("submit_ready_rejected_in_dry_run")
    );
    assert!(summary
        .last_submit_idempotency_key
        .as_deref()
        .is_some_and(|key| key.contains("copybot:submit:copybot:canary")));
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
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[test]
fn jupiter_metis_dry_run_plans_submit_ready_from_signed_envelope() -> Result<()> {
    let request = file_signer_submit_request("adapter-submit-ready");
    let plan = crate::execution_submit_adapter::NoSubmitExecutionAdapter
        .build_transaction_plan(&request)?;
    let envelope = crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
        &request,
        &plan,
        crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
            signed_transaction_base64: "AQIDBA==".to_string(),
            tx_signature_hint: Some("canary_tx_signature_hint".to_string()),
        },
    )?;
    let adapter = crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(
        ExecutionConfig::default(),
    );

    let submit_plan = adapter.plan_submit_with_envelope(&request, &envelope)?;

    match submit_plan {
        crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(intent) => {
            assert_eq!(intent.submit_route, "rpc_dry_run");
            assert_eq!(intent.signed_transaction_base64, "AQIDBA==");
            assert_eq!(
                intent.tx_signature_hint.as_deref(),
                Some("canary_tx_signature_hint")
            );
        }
        crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitDisabled { .. } => {
            panic!("signed envelope should produce submit-ready dry-run intent")
        }
    }
    Ok(())
}

#[tokio::test]
async fn execution_canary_file_signer_rejects_wrong_first_signer() -> Result<()> {
    let keypair = file_signer_test_keypair(8);
    let wrong_first_signer = file_signer_test_keypair(9);
    let keypair_path =
        write_file_signer_keypair_file("wrong-first-signer", &keypair.keypair_bytes)?;
    let db_path = unique_file_signer_test_path("wrong-first-signer");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = file_signer_signal("wrong-first-signer", now);
    store.insert_copy_signal(&signal)?;
    record_file_signer_quote(&store, &signal, now)?;
    let config = file_signer_config(&keypair.pubkey_string, &keypair_path);
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config.clone(),
        FileSignerAdapter {
            config,
            serialized_transaction_base64: serialized_legacy_transaction(
                wrong_first_signer.public_key,
            ),
        },
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
        .contains("first signer"));
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(keypair_path);
    Ok(())
}

#[derive(Debug, Clone)]
struct FileSignerAdapter {
    config: ExecutionConfig,
    serialized_transaction_base64: String,
}

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for FileSignerAdapter {
    fn build_transaction_plan(
        &self,
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

    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> crate::execution_submit_adapter::ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            let slot = plan
                .serialized_transaction_payload_slot
                .as_ref()
                .expect("test plan should carry serialized payload slot");
            slot.store(
                crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
                    source: "test_metis_swap_transaction".to_string(),
                    serialized_transaction_base64: self.serialized_transaction_base64.clone(),
                },
            )?;
            Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
                status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: Some("serialized_transaction_base64_ready=true".to_string()),
            })
        })
    }

    fn sign_serialized_transaction(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
        payload: &crate::execution_signing_envelope::ExecutionSerializedTransactionPayload,
    ) -> Result<Option<crate::execution_signing_envelope::ExecutionSignedTransactionPayload>> {
        let signed = crate::execution_submit_adapter::sign_serialized_transaction_from_config(
            &self.config,
            request,
            plan,
            payload,
        )?;
        if let Some(signed_payload) = signed.as_ref() {
            assert_ne!(
                signed_payload.signed_transaction_base64,
                payload.serialized_transaction_base64
            );
            assert!(signed_payload
                .tx_signature_hint
                .as_deref()
                .is_some_and(|hint| !hint.is_empty()));
        }
        Ok(signed)
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.plan_submit(request)
    }

    fn plan_submit_with_envelope(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
            request,
            envelope,
            "rpc_dry_run".to_string(),
        )?;
        Ok(crate::execution_submit_adapter::ExecutionSubmitPlan::SubmitReady(intent))
    }
}

struct FileSignerTestKeypair {
    public_key: [u8; 32],
    pubkey_string: String,
    keypair_bytes: Vec<u8>,
}

fn file_signer_test_keypair(seed: u8) -> FileSignerTestKeypair {
    let secret = [seed; 32];
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key().to_bytes();
    let mut keypair_bytes = Vec::with_capacity(64);
    keypair_bytes.extend_from_slice(&secret);
    keypair_bytes.extend_from_slice(&public_key);
    FileSignerTestKeypair {
        public_key,
        pubkey_string: bs58::encode(public_key).into_string(),
        keypair_bytes,
    }
}

fn serialized_legacy_transaction(first_account_key: [u8; 32]) -> String {
    let mut transaction = vec![1_u8];
    transaction.extend_from_slice(&[0_u8; 64]);
    transaction.extend_from_slice(&[1_u8, 0, 0]);
    transaction.push(1);
    transaction.extend_from_slice(&first_account_key);
    transaction.extend_from_slice(&[9_u8; 32]);
    transaction.push(0);
    BASE64_STANDARD.encode(transaction)
}

fn write_file_signer_keypair_file(name: &str, keypair_bytes: &[u8]) -> Result<PathBuf> {
    let path = unique_file_signer_test_path(name).with_extension("json");
    let json = serde_json::to_string(keypair_bytes)?;
    std::fs::write(&path, json)?;
    Ok(path)
}

fn file_signer_config(pubkey: &str, keypair_path: &Path) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_wallet_pubkey = pubkey.to_string();
    config.execution_signer_pubkey = pubkey.to_string();
    config.execution_signer_keypair_path = keypair_path.to_string_lossy().to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn file_signer_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-file-signer:leader-wallet:{signal_id}:TokenMint"),
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

fn record_file_signer_quote(
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

fn file_signer_submit_request(
    name: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: format!("canary-order-{name}"),
        signal_id: format!("shadow:sig-file-signer:{name}"),
        client_order_id: format!("copybot:canary:{name}"),
        attempt: 1,
        route: "metis-canary".to_string(),
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "DryRunWallet11111111111111111111111111111111".to_string(),
        entry_route_plan_json: None,
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default(),
    }
}

fn unique_file_signer_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-file-signer-{name}-{}-{nanos}",
        std::process::id()
    ))
}
