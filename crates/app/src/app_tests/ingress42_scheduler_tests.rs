//! Actual strict scheduler tick with all financial flags off, on a NEW empty DB.
use super::*;
#[path = "association_fixture.rs"]
mod a;
use anyhow::{ensure, Result};
use copybot_ingestion::IngestionService;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn ingress42_financial_flags_off_preserve_actual_strict_tick_without_actions() -> Result<()> {
    let _serial = crate::app_tests::b70_hooks::acquire().await;
    let (hooks, _unused_legacy_input) = crate::app_tests::b70_hooks::Installed::new();
    let root = std::env::var("B89_DB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&root)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = root.join(format!(
        "ingress42-scheduler-{}-{nonce}.sqlite",
        std::process::id()
    ));
    ensure!(!path.exists());
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(
        &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
    )?;
    let db = a::Db {
        sql: rusqlite::Connection::open(&path)?,
        path,
        store,
    };
    let actual = std::env::var("INGRESS42_CONFIG").ok();
    let mut config = if let Some(path) = &actual {
        copybot_config::load_from_path(path)?
    } else {
        let mut value = a::config(&json!({
            "programs":["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"],
            "raydium":[],"pumpswap":[]}));
        value.execution.enabled = false;
        value.execution.canary_enabled = false;
        value.execution.canary_tiny_submit_enabled = false;
        value.execution.canary_entry_submit_enabled = false;
        value.execution.simulate_before_submit = false;
        value.execution.swap_transaction_dry_run_enabled = false;
        value.execution.quote_canary_enabled = false;
        value.execution.swap_instructions_dry_run_enabled = false;
        value
    };
    copybot_config::validate_association_delivery(&config)?;
    let e = &config.execution;
    ensure!(
        !e.enabled
            && !e.canary_enabled
            && !e.canary_tiny_submit_enabled
            && !e.canary_entry_submit_enabled
            && !e.quote_canary_enabled
            && !e.simulate_before_submit
            && !e.swap_instructions_dry_run_enabled
            && !e.swap_transaction_dry_run_enabled
    );
    ensure!(
        e.owner_exit.is_none()
            && e.owner_technical_buy.is_none()
            && e.technical_cohort.is_none()
            && e.owned_sell_preparation.is_none()
    );
    if actual.is_some() {
        ensure!(e.execution_signer_keypair_path.is_empty() && e.execution_signer_pubkey.is_empty());
    }
    // Validate/construct the exact candidate before any local transport aliases.
    let exact = ExecutionCanaryRunner::new(config.execution.clone())
        .for_ingestion(&config.ingestion, &db.path.to_string_lossy())?;
    ensure!(
        exact.is_enabled(),
        "exact durable config schedules canonical obligations"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    // Local fixture aliases only; no financial flag/scope or timestamp changes.
    config.execution.quote_canary_base_url = endpoint.clone();
    config.execution.submit_adapter_http_url = endpoint.clone();
    config.execution.priority_fee_canary_rpc_url = endpoint;
    config.execution.execution_signer_keypair_path = "/nonexistent/DIAGNOSTIC42_NEVER_SIGN".into();
    let (_input, replay) = tokio::sync::mpsc::channel(1);
    let mut ingestion = IngestionService::with_replay_scoped(
        &config,
        replay,
        "ingress42-precheck".into(),
        (!config.ingestion.yellowstone_replay_wallets.is_empty()).then(|| {
            config
                .ingestion
                .yellowstone_replay_wallets
                .iter()
                .cloned()
                .collect()
        }),
    )?;
    let consumer = AssociationConsumer::start(
        &mut ingestion,
        &config.ingestion,
        &db.path.to_string_lossy(),
    )
    .await?
    .unwrap();
    let runner = ExecutionCanaryRunner::new(config.execution.clone())
        .for_ingestion(&config.ingestion, &db.path.to_string_lossy())?
        .with_ingress_hold(consumer.http_continuity_hold());
    ensure!(
        runner.is_enabled(),
        "durable strict scheduling survives financial flags=false"
    );
    let financial = || -> Result<Vec<i64>> {
        [
            "orders",
            "fills",
            "execution_canary_dispatch",
            "execution_canary_receipt_proofs",
            "execution_quote_canary_events",
            "rpc_owned_sell_dispatches",
        ]
        .iter()
        .map(|t| {
            Ok(db
                .sql
                .query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))?)
        })
        .collect()
    };
    let before = financial()?;
    ensure!(before.iter().all(|n| *n == 0));
    let mut interval = tokio::time::interval(Duration::from_secs(runner.interval_seconds()));
    interval.tick().await; // Same interval opportunity as the unchanged app-loop branch.
    let precheck = observe(&config.execution, runner.is_enabled(), Some(&consumer));
    ensure!(
        !precheck.execution_enabled
            && !precheck.canary_enabled
            && !precheck.tiny_submit_enabled
            && !precheck.quote_enabled
    );
    ensure!(precheck.scheduler_enabled && precheck.process_tick_about_to_invoke);
    let trace = precheck.continuity.as_ref().unwrap();
    ensure!(
        trace.began_ns <= trace.snapshot.sampled_ns && trace.snapshot.sampled_ns <= trace.ended_ns
    );
    ensure!(precheck.phase == "before_process_tick_call");
    tick(&runner, &db.store, &config.execution, Some(&consumer)).await;
    ensure!(
        hooks.0.count("execution_tick", "") == 1,
        "actual tick reached its existing completion hook"
    );
    ensure!(
        financial()? == before,
        "observation must not create financial state"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "no quote, simulation or provider request with financial flags off"
    );
    ensure!(
        db.sql
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?
            == "ok"
    );
    println!("INGRESS42_REAL_STRICT_PRECHECK exact_candidate={} flags=false scheduler=true process_tick=true financial_rows=0 loopback_requests=0", actual.is_some());
    Ok(())
}
