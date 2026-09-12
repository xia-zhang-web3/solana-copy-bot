use super::source_sell_producer_fixture::Fixture;
use crate::execution_source_sell_producer::RAW_VISIT_BUDGET;
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellStagingVisit as Visit;

#[tokio::test]
async fn source_sell_producer_budget_restart_arrivals_deleted_cursor_and_repair_wrap() -> Result<()>
{
    let mut f = Fixture::new().await?;
    f.config.canary_tiny_submit_enabled = false;
    f.config.canary_batch_limit = 1;
    let b = f.stage("queue-b").await?;
    let bad_count = RAW_VISIT_BUDGET * 2 + 3;
    for i in 0..bad_count {
        let a = f.stage(&format!("bad-{i}")).await?;
        f.f.conn()?.execute("UPDATE execution_source_sell_intents SET event_ts='SYNTHETIC_PRIVATE_BAD_TIME' WHERE intent_id=?1", [&a.intent_id])?;
    }
    let first = f.tick().await?;
    assert_eq!(first.source_sell_production.visits, RAW_VISIT_BUDGET);
    assert_eq!(first.source_sell_production.malformed, RAW_VISIT_BUDGET);
    assert_eq!(first.source_sell_production.inserted, 0);
    let boundary: i64 = f.f.conn()?.query_row(
        "SELECT last_rowid FROM execution_source_sell_staging_cursor",
        [],
        |r| r.get(0),
    )?;
    f.f.conn()?.execute(
        "DELETE FROM execution_source_sell_intents WHERE rowid=?1",
        [boundary],
    )?;
    // Every tick uses a new runner and this also replaces the SQLite handle.
    f.f.reopen()?;
    let newer = f.stage("new-arrival").await?;
    f.f.conn()?.execute(
        "UPDATE execution_source_sell_intents SET staged_at='bad' WHERE intent_id=?1",
        [&newer.intent_id],
    )?;
    let second = f.tick().await?;
    assert_eq!(second.source_sell_production.visits, RAW_VISIT_BUDGET);
    assert_eq!(second.source_sell_production.inserted, 0);
    let newest = f.stage("next-arrival").await?;
    f.f.conn()?.execute(
        "UPDATE execution_source_sell_intents SET staged_at='bad' WHERE intent_id=?1",
        [&newest.intent_id],
    )?;
    f.f.reopen()?;
    let third = f.tick().await?;
    assert_eq!(third.source_sell_production.inserted, 1, "{third:?}");
    assert!(third.source_sell_production.visits <= RAW_VISIT_BUDGET);
    assert!(third.source_sell_production.wrapped);
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id("shadow:queue-b:source-a:sell:mint")?
        .is_some());
    assert_eq!(f.f.staged("queue-b")?.unwrap().position_id, b.position_id);
    // Repair one old row with its original observed time, then admit new rows every tick.
    let repaired = format!("source-sell:bad-{}", bad_count - 1);
    let repaired_signal = format!("shadow:bad-{}:source-a:sell:mint", bad_count - 1);
    f.f.conn()?.execute(
        "UPDATE execution_source_sell_intents SET event_ts=?1 WHERE intent_id=?2",
        rusqlite::params![b.event.ts_utc.to_rfc3339(), repaired],
    )?;
    let mut repaired_seen = false;
    for i in 0..3 {
        let new = f.stage(&format!("continuous-{i}")).await?;
        f.f.conn()?.execute(
            "UPDATE execution_source_sell_intents SET staged_at='bad' WHERE intent_id=?1",
            [&new.intent_id],
        )?;
        f.f.reopen()?;
        let tick = f.tick().await?;
        assert!(tick.source_sell_production.visits <= RAW_VISIT_BUDGET);
        if f.f
            .store
            .load_copy_signal_by_signal_id(&repaired_signal)?
            .is_some()
        {
            repaired_seen = true;
            break;
        }
    }
    f.finish().await?;
    assert!(
        repaired_seen,
        "repaired retained A must be revisited after wrap"
    );
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_checkpoint_crash_defers_a_until_wrap_without_losing_it() -> Result<()>
{
    let mut f = Fixture::new().await?;
    f.config.canary_tiny_submit_enabled = false;
    let b = f.stage("older-b").await?;
    let a = f.stage("newer-a").await?;
    assert!(matches!(f.f.store.advance_execution_source_sell_staging()?,
        Visit::Row { intent_id: Some(ref id), .. } if id == &a.intent_id));
    f.f.reopen()?; // Crash after durable checkpoint, before promotion.
    let first = f.tick().await?;
    assert_eq!(first.source_sell_production.inserted, 1);
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id("shadow:newer-a:source-a:sell:mint")?
        .is_none());
    assert!(first.source_sell_production.wrapped);
    f.f.reopen()?;
    let second = f.tick().await?;
    f.finish().await?;
    assert_eq!(
        (
            second.source_sell_production.inserted,
            second.source_sell_production.existing
        ),
        (1, 1)
    );
    assert!(f
        .f
        .store
        .load_copy_signal_by_signal_id("shadow:newer-a:source-a:sell:mint")?
        .is_some());
    assert_eq!(f.f.staged("older-b")?.unwrap().event.ts_utc, b.event.ts_utc);
    assert_eq!(f.f.staged("newer-a")?.unwrap().position_id, a.position_id);
    assert_eq!(
        f.f.conn()?.query_row(
            "SELECT count(*) FROM execution_source_sell_promotions",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn source_sell_producer_refused_a_then_submitted_b_keeps_a_in_production_event() -> Result<()>
{
    let mut f = Fixture::new().await?;
    f.stage("event-b").await?;
    let a = f.stage("event-a").await?;
    f.f.conn()?.execute("UPDATE execution_source_sell_intents SET staged_at='SYNTHETIC_PRIVATE_PAYLOAD' WHERE intent_id=?1", [&a.intent_id])?;
    let result = f.tick().await;
    f.finish().await?;
    let tick = result?;
    assert_eq!(
        (
            tick.source_sell_production.malformed,
            tick.source_sell_production.inserted
        ),
        (1, 1)
    );
    assert_eq!(f.rpc.count("sendTransaction"), 1, "{tick:?}");
    let b =
        f.f.store
            .load_execution_canary_order_by_signal("shadow:event-b:source-a:sell:mint")?
            .unwrap();
    assert_eq!(
        tick.last_state_machine_order_id.as_deref(),
        Some(b.order_id.as_str())
    );
    let event = super::submit_refusal_fixture::capture(|| {
        crate::telemetry::record_execution_canary_tick(&tick)
    });
    assert!(tick.has_status_change());
    assert_eq!(event["source_sell_staging_visits"], "2");
    assert_eq!(event["source_sell_promoted"], "1");
    assert_eq!(event["source_sell_promotion_malformed"], "1");
    assert_eq!(event["source_sell_promotion_refusal_id"], a.intent_id);
    assert_eq!(
        event["source_sell_promotion_refusal_reason"],
        "source_sell_staged_data_invalid"
    );
    assert!(event["source_sell_promotion_refusal_reason"].len() <= 64);
    assert_eq!(event["last_state_machine_order_id"], b.order_id);
    assert!(event
        .values()
        .all(|v| !v.contains("SYNTHETIC_PRIVATE_PAYLOAD")));
    eprintln!("B46_PRODUCTION_EVENT {}", serde_json::to_string(&event)?);
    Ok(())
}
