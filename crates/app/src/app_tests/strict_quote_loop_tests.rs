//! Actual app select loop, held HTTP, independent durable intake and receipt mutation.
use super::{
    association_fixture as a, association_parent_fixture as p, association_sell_fixture as s,
    b70_fixture::Fixture, b70_hooks, b93_fixture as receipt, strict_quote_fixture as q,
};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::{ordered_sell_quote::*, SqliteStore};
use std::time::Duration;
async fn until(mut condition: impl FnMut() -> Result<bool>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition()? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
#[tokio::test]
#[ignore = "full app loop; captured inputs, seeded receipts and loopback only"]
async fn strict_quote_full_loop_held_http_intake_and_exact_retry() -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let mut server = q::Server::new().await?;
    let f = Fixture::new(&server.url, false).await?;
    let m = p::meta("direct")?;
    let db = a::Db {
        path: f.f.path.clone(),
        store: SqliteStore::open(&f.f.path)?,
        sql: f.f.conn()?,
    };
    s::seed(&db, &m)?;
    let before = s::snapshot(&db)?;
    let mut c = a::config(&m);
    c.execution = f.execution.clone();
    c.execution.enabled = false;
    c.execution.canary_tiny_submit_enabled = false;
    c.execution.quote_canary_enabled = true;
    c.execution.quote_canary_pump_fun_parallel_enabled = false;
    c.execution.priority_fee_canary_enabled = false;
    c.execution.quote_canary_timeout_ms = 4000;
    let (input, replay) = tokio::sync::mpsc::channel(1);
    let service = IngestionService::with_replay(&c, replay, "strict-loop".into())?;
    let (installed, _unused) = b70_hooks::Installed::new();
    let h = &installed.0;
    let daemon = crate::app_loop::run_app_loop(
        SqliteStore::open(&db.path)?,
        service,
        f.discovery.clone(),
        f.f.shadow.clone(),
        c.execution.clone(),
        f.risk.clone(),
        c.ingestion.clone(),
        f.shadow.clone(),
        db.path.to_string_lossy().into(),
        3600,
        Default::default(),
        f.journal.clone(),
        3600,
        3600,
        30,
        "yellowstone_grpc".into(),
        3600,
        false,
        0,
        false,
        None,
    );
    let controller = async {
        let result:Result<()>=async {
            let frames=p::frames(&m);let count=frames.len() as u64;
            for (n,name) in frames.iter().enumerate() {
                input.send(ReplayInput::Update{offset_ns:n as u64+1,payload:std::fs::read(p::root("direct").join(format!("{name}.pb")))?}).await?;
            }
            let held=server.request().await?;assert_eq!(held.query["amount"],"7000");
            ensure!(s::snapshot(&db)?==before,"no actual financial activity before receipt fixture");
            let first=q::first(&db,&m)?;
            let events:i64=db.sql.query_row("SELECT count(*) FROM association_inbox_events",[],|r|r.get(0))?;
            input.send(ReplayInput::Update{offset_ns:count+1,payload:std::fs::read(p::root("direct").join("sell.pb"))?}).await?;
            // This must complete before releasing the held HTTP response.
            until(||Ok(db.sql.query_row("SELECT count(*) FROM association_inbox_events",[],|r|r.get::<_,i64>(0))?>events)).await?;
            let facts=receipt::receipt(&db,&m,"full-loop-partial",3000)?;receipt::settle(&db,&facts)?;
            let after_fixture=s::snapshot(&db)?;held.answer()?;
            let stale=q::result(&db,&m).await?;ensure!(stale.outcome==QuoteOutcome::Stale,"{stale:?}");
            ensure!(stale.binding.as_ref().unwrap().raw==7000);
            let next=server.request().await?;assert_eq!(next.query["amount"],"4000");next.answer()?;
            let current=q::result(&db,&m).await?;
            ensure!(current.outcome==QuoteOutcome::Current && current.binding.as_ref().unwrap().raw==4000);
            ensure!(q::first(&db,&m)?==first && s::snapshot(&db)?==after_fixture);
            let legacy:i64=db.sql.query_row("SELECT count(*) FROM execution_quote_canary_events",[],|r|r.get(0))?;
            ensure!(legacy==0 && server.seen.lock().unwrap().len()==2);
            std::fs::write(std::path::Path::new(&std::env::var("B89_DB_DIR")?).join("full-loop-held.json"),serde_json::to_vec_pretty(&serde_json::json!({"db":db.path,"stale":stale,"current":current,"requests":*server.seen.lock().unwrap(),"financial_actions_outside_explicit_seed_and_partial_receipt":0,"intake_during_held_http":true}))?)?;
            Ok(())
        }.await;
        h.stop();
        result
    };
    let (d, c) = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(daemon, controller)
    })
    .await?;
    d?;
    c?;
    ensure!(h.count("checked_shutdown", "") == 1);
    Ok(())
}
#[tokio::test]
#[ignore = "real lease expiry after worker crash; bounded 35 second loopback test"]
async fn strict_quote_actual_crashed_claim_recovers_without_status_flip() -> Result<()> {
    let (db, m) = q::seeded("strict-claim-crash").await?;
    let now = Utc::now();
    let server = q::Server::new().await?;
    let endpoint = reqwest::Url::parse(&server.url)?.to_string();
    let QuoteClaimStep::Claimed(claim) =
        db.store
            .claim_strict_sell_quote(p::limits(), &endpoint, Utc::now)?
    else {
        anyhow::bail!("claim missing");
    };
    // Simulated process loss after durable claim, before HTTP; no status mutation.
    drop(claim);
    drop(server);
    let mut server = q::Server::new().await?;
    let restarted = q::runner(&db, &m, &server.url, true)?;
    q::tick(&restarted, &db).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    ensure!(server.seen.lock().unwrap().is_empty());
    tokio::time::sleep(Duration::from_secs(30)).await;
    q::tick(&restarted, &db).await?;
    let request = server.request().await?;
    assert_eq!(request.query["amount"], "7000");
    request.answer()?;
    ensure!(q::result(&db, &m).await?.outcome == QuoteOutcome::Current);
    let attempt: i64 = db.sql.query_row(
        "SELECT attempt FROM ordered_sell_quote_results WHERE intent_id=?1",
        [q::id(&m)],
        |r| r.get(0),
    )?;
    ensure!(attempt == 2);
    Ok(())
}
