use super::{association_fixture as a, association_sell_fixture as s, strict_quote_fixture as q};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_storage_core::ordered_sell_quote::*;
use std::time::Duration;
#[tokio::test]
#[ignore = "actual quote tick, held SQLite writer after HTTP; captured inputs and loopback only"]
async fn strict_quote_r1_sqlite_wait_retries_fresh_without_status_flip() -> Result<()> {
    let (db, m) = q::seeded("r1-http-completion-wait").await?;
    let mut server = q::Server::new().await?;
    let mut c = a::config(&m);
    c.ingestion
        .yellowstone_association
        .as_mut()
        .unwrap()
        .sqlite_busy_ms = 9000;
    c.execution.quote_canary_enabled = true;
    c.execution.quote_canary_base_url = server.url.clone();
    c.execution.quote_canary_timeout_ms = 4000;
    c.execution.canary_enabled = true;
    c.execution.priority_fee_canary_enabled = false;
    c.execution.quote_canary_pump_fun_parallel_enabled = false;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(c.execution)
        .for_ingestion(&c.ingestion, &db.path.to_string_lossy())?;
    let before = s::snapshot(&db)?;
    let first = q::first(&db, &m)?;
    q::tick(&runner, &db).await?;
    let held = server.request().await?;
    let blocker = rusqlite::Connection::open(&db.path)?;
    blocker.execute_batch("BEGIN IMMEDIATE")?;
    held.answer()?;
    tokio::time::sleep(Duration::from_millis(6200)).await;
    ensure!(
        db.sql.query_row(
            "SELECT count(*) FROM ordered_sell_quote_results WHERE record IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0)
        )? == 0
    );
    let released = Utc::now();
    blocker.execute_batch("COMMIT")?;
    let old = q::result(&db, &m).await?;
    ensure!(
        old.outcome != QuoteOutcome::Current
            && released - old.http_ended > chrono::Duration::seconds(6),
        "{old:?}"
    );
    q::tick(&runner, &db).await?;
    let retry = server.request().await?;
    ensure!(retry.query["amount"] == "7000");
    retry.answer()?;
    let current = q::result(&db, &m).await?;
    ensure!(current.outcome == QuoteOutcome::Current && current.http_started.unwrap() > released);
    ensure!(s::snapshot(&db)? == before && q::first(&db, &m)? == first);
    ensure!(server.seen.lock().unwrap().len() == 2);
    let firstrow: i64 = db.sql.query_row(
        "SELECT count(*) FROM execution_quote_canary_events",
        [],
        |r| r.get(0),
    )?;
    ensure!(firstrow == 0);
    std::fs::write(
        std::path::Path::new(&std::env::var("B89_DB_DIR")?).join("completion-wait-app.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"db":db.path,"released":released,"expired":old,"fresh":current,"requests":*server.seen.lock().unwrap(),"financial_unchanged":true,"manual_status_flips":0}),
        )?,
    )?;
    Ok(())
}
