use super::{
    association_parent_fixture as p, b135_fixture::Fixture, b135_server::Server,
    strict_quote_fixture as q,
};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::ordered_sell_quote::QuoteOutcome;
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
    for _ in 0..100 {
        if f.handoffs()? == 1 {
            break;
        }
        q::tick(&runner, &f.db).await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
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
