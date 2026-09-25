//! Periodic cohort RPC fence failure and recovery through the daemon consumer.
use super::native_buy_runner_tests::setup_case;
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::{Context, Result};
use copybot_ingestion::IngestionService;
use rusqlite::Connection;
use serde_json::{json, Value};

#[tokio::test]
async fn cohort_periodic_fence_transport_failure_keeps_daemon_consumer_live() -> Result<()> {
    let case = setup_case("10000", false, true, true, true).await?;
    let chain: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b135_fixture::inputs().join("chain.json"))?)?;
    let mut app = super::association_fixture::config(&chain);
    app.execution = case.config.clone();
    let authority = crate::execution_technical_cohort::authority(&case.config)?
        .context("cohort authority")?;
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &case.config)?;
    let (_sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = IngestionService::with_replay_scoped(
        &app, receiver, "cohort-fence-retry".into(), Some(scope))?;
    let mut consumer = crate::association_consumer::AssociationConsumer::start_with_execution(
        &mut ingestion, &app.ingestion, &app.execution, &case.path).await?
        .context("cohort consumer")?;
    let mut good = Parsed(|request: Value| async move {
        let result = match request["method"].as_str() {
            Some("getGenesisHash") => json!("11111111111111111111111111111111"),
            Some("getSlot") => json!(200),
            other => anyhow::bail!("unexpected fence method: {other:?}"),
        };
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    });
    consumer.poll_with_transport(&case.store, Some(&mut good)).await?;
    let db = Connection::open(&case.path)?;
    let epochs = || -> Result<i64> {
        Ok(db.query_row("SELECT count(*) FROM native_buy_fence_epochs", [], |r| r.get(0))?)
    };
    let before = epochs()?;
    consumer.next_fence_at = Some(tokio::time::Instant::now());
    let mut failed = Parsed(|_request: Value| async move {
        anyhow::bail!("owned_sell_rpc_transport")
    });
    for _ in 0..4 {
        tokio::time::timeout(std::time::Duration::from_secs(2),
            consumer.poll_with_transport(&case.store, Some(&mut failed))).await??;
        if consumer.next_fence_at.is_some_and(|at| at > tokio::time::Instant::now()) {
            break;
        }
    }
    assert_eq!(epochs()?, before, "failed request cannot mint a fence epoch");
    assert!(consumer.next_fence_at.is_some_and(|at| at > tokio::time::Instant::now()));
    consumer.next_fence_at = Some(tokio::time::Instant::now());
    consumer.poll_with_transport(&case.store, Some(&mut good)).await?;
    assert_eq!(epochs()?, before + 1, "same consumer resumes after transport recovery");
    consumer.next_fence_at = Some(tokio::time::Instant::now());
    let mut wrong_genesis = Parsed(|request: Value| async move {
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":"wrong-genesis"}))
    });
    assert!(consumer.poll_with_transport(&case.store, Some(&mut wrong_genesis))
        .await.unwrap_err().to_string().contains("native_buy_genesis_mismatch"));
    assert_eq!(epochs()?, before + 1, "invalid proof cannot create an epoch");
    Ok(())
}
