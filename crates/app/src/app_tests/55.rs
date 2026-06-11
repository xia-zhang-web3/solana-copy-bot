use super::*;

#[test]
fn submit_transport_unknown_without_signature_retries_after_timeout_without_new_order() -> Result<()>
{
    let db_path = unique_submit_transport_outcome_test_path("unknown-retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = simulated_submit_transport_request(&store, "unknown-retry", now)?;

    let record: crate::execution_submit_adapter::ExecutionSubmitTransportRecordOutcome =
        crate::execution_submit_adapter::record_submit_transport_outcome(
            &store,
            &request,
            crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
                idempotency_key: crate::execution_submit_adapter::execution_submit_idempotency_key(
                    &request,
                ),
                tx_signature: None,
            },
            now + chrono::Duration::seconds(3),
        )?;
    let state_machine = submit_transport_outcome_state_machine();
    let summary = state_machine.process_submit_timeout(
        &store,
        &request.order_id,
        chrono::Duration::seconds(60),
        now + chrono::Duration::seconds(65),
    )?;
    let reserve = store.reserve_execution_canary_order(
        &request.signal_id,
        &request.route,
        now + chrono::Duration::seconds(66),
    )?;

    assert_eq!(record.submitted, 1);
    assert_eq!(record.tx_signature, None);
    assert_eq!(
        record.reason.as_deref(),
        Some("submit_transport_submitted_unknown_no_signature")
    );
    assert_eq!(summary.submit_timeout_retry, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(reserve.order.order_id, request.order_id);
    assert_eq!(reserve.order.attempt, 2);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn submit_transport_unknown_with_signature_expires_unsafe_instead_of_retry() -> Result<()> {
    let db_path = unique_submit_transport_outcome_test_path("signature-expire");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = simulated_submit_transport_request(&store, "signature-expire", now)?;

    let record = crate::execution_submit_adapter::record_submit_transport_outcome(
        &store,
        &request,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key: crate::execution_submit_adapter::execution_submit_idempotency_key(
                &request,
            ),
            tx_signature: Some("tx-sig-submit-transport".to_string()),
        },
        now + chrono::Duration::seconds(3),
    )?;
    let state_machine = submit_transport_outcome_state_machine();
    let summary = state_machine.process_submit_timeout(
        &store,
        &request.order_id,
        chrono::Duration::seconds(60),
        now + chrono::Duration::seconds(65),
    )?;
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("canary order should exist");

    assert_eq!(record.submitted, 1);
    assert_eq!(
        record.tx_signature.as_deref(),
        Some("tx-sig-submit-transport")
    );
    assert_eq!(summary.submit_timeout_expire_unsafe, 1);
    assert_eq!(summary.submit_timeout_retry, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(order.attempt, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn submit_transport_outcome_rejects_wrong_idempotency_without_state_change() -> Result<()> {
    let db_path = unique_submit_transport_outcome_test_path("bad-idempotency");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let request = simulated_submit_transport_request(&store, "bad-idempotency", now)?;

    let error = crate::execution_submit_adapter::record_submit_transport_outcome(
        &store,
        &request,
        crate::execution_submit_adapter::ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key: "copybot:submit:wrong:attempt:1".to_string(),
            tx_signature: None,
        },
        now + chrono::Duration::seconds(3),
    )
    .expect_err("wrong idempotency must fail");
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .expect("canary order should exist");

    assert!(error.to_string().contains("idempotency key mismatch"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
    );
    assert_eq!(order.attempt, 1);
    assert!(order.tx_signature.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn submit_transport_outcome_state_machine(
) -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config,
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    )
}

fn simulated_submit_transport_request(
    store: &SqliteStore,
    name: &str,
    now: chrono::DateTime<Utc>,
) -> Result<crate::execution_submit_adapter::ExecutionSubmitRequest> {
    let signal = submit_transport_outcome_signal(name, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: reserve.order.order_id,
        signal_id: signal.signal_id,
        client_order_id: reserve.order.client_order_id,
        attempt: reserve.order.attempt,
        route: "metis-canary".to_string(),
        wallet_id: signal.wallet_id,
        token: signal.token,
        side: signal.side,
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "DryRunWallet11111111111111111111111111111111".to_string(),
        entry_route_plan_json: None,
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default(),
    })
}

fn submit_transport_outcome_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-submit-transport:{signal_id}:TokenMint"),
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

fn unique_submit_transport_outcome_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-submit-transport-outcome-{name}-{}-{nanos}",
        std::process::id()
    ))
}
