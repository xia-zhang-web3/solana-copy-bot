use super::{
    association_parent_fixture as p, b135_fixture::Fixture, b135_server::Server,
    strict_quote_fixture as q,
};
use anyhow::{Context, Result};
use chrono::Utc;
use copybot_storage_core::ordered_sell_quote::QuoteOutcome;
#[tokio::test]
async fn b135_mock_peer_survives_cancelled_socket() -> Result<()> {
    let server = Server::new().await?;
    let socket = tokio::net::TcpStream::connect(server.url.trim_start_matches("http://")).await?;
    drop(socket);
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reqwest::Client::new()
            .post(&server.url)
            .json(
                &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"getGenesisHash","params":[]}),
            )
            .send(),
    )
    .await??;
    let wire: serde_json::Value = response.json().await?;
    assert_eq!(wire["result"], "11111111111111111111111111111111");
    use tokio::io::AsyncWriteExt;
    let mut partial =
        tokio::net::TcpStream::connect(server.url.trim_start_matches("http://")).await?;
    partial
        .write_all(b"POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n{}")
        .await?;
    drop(partial);
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reqwest::Client::new()
            .post(&server.url)
            .json(
                &serde_json::json!({"jsonrpc":"2.0","id":2,"method":"getGenesisHash","params":[]}),
            )
            .send(),
    )
    .await??;
    assert_eq!(
        response.json::<serde_json::Value>().await?["result"],
        "11111111111111111111111111111111"
    );
    server.healthy();
    Ok(())
}
#[tokio::test]
async fn b135_actual_ingress_recovery_runner_unsigned_handoff() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    let c = f.config(&server.url)?;
    assert!(!c.execution.enabled && !c.execution.canary_tiny_submit_enabled);
    f.ingress(&c).await?;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(c.execution)
        .for_ingestion(&c.ingestion, &f.db.path.to_string_lossy())?;
    q::tick(&runner, &f.db).await?;
    let quote = q::result(&f.db, &f.meta).await?;
    assert_eq!(quote.outcome, QuoteOutcome::Current);
    assert_eq!(quote.binding.as_ref().unwrap().raw, 7000);
    assert!(quote.event_time.is_none() && quote.event_delay_ns.is_none());
    f.drive(&runner).await.with_context(|| {
        format!(
            "handoff did not become unsigned_prepared: rows={:?} server_terminal={:?}",
            f.rows("rpc_owned_sell_handoffs"),
            server.terminal()
        )
    })?;
    println!(
        "B135_CAUSAL strict_quote={:?} owned_raw=7000 source_utc=Unknown handoffs={} calls={:?}",
        f.db.store
            .load_strict_sell_quote(&q::id(&f.meta), p::limits(), Utc::now())?
            .unwrap()
            .outcome,
        f.handoffs()?,
        server
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|v| v["method"].clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        f.handoffs()?,
        1,
        "actual quote-only runner has no financial unsigned handoff"
    );
    Ok(())
}
