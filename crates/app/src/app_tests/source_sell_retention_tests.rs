use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::observed_swap_writer::ObservedSwapWriter;
use crate::source_sell_staging::StageNotice;
use anyhow::Result;
use std::time::Duration;

#[tokio::test]
async fn source_sell_unproved_duplicate_hint_cannot_gain_authority_after_retention() -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("unproved-a", "source-a");
    // Observed data can survive loss of process-local recent/binding state.
    // Populate it through the real writer; neither dedupe nor generation proof exists.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), f.writer.as_ref().unwrap().write(&a))
            .await??
    );
    f.store
        .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
    f.buy("b", "source-a")?;
    let position_b = f.position()?;
    // Fresh to recent-dedupe, but Duplicate ACK leaves the captured B hint unproved.
    let (result, first_events) = capture(f.send(&a, true)).await;
    result?;
    assert!(f.scheduler.source_sells.is_empty());
    assert!(first_events
        .iter()
        .any(|e| e["reason"] == "original_generation_unknown"));
    f.store
        .delete_observed_swaps_before_batch(a.ts_utc + chrono::Duration::seconds(1), 10)?;
    let before = f.money()?;
    let mut deliveries = Vec::new();
    for _ in 0..2 {
        let (result, events) = capture(f.send(&a, true)).await;
        result?;
        let worker_started = !f.scheduler.source_sells.is_empty();
        if worker_started {
            f.stage_completion().await?;
        }
        deliveries.push((events, worker_started, f.staged(&a.signature)?));
    }
    let fresh_b = f.sell("fresh-b", "source-a");
    f.send(&fresh_b, true).await?;
    let completion = f.stage_completion().await?;
    let row = f.staged(&fresh_b.signature)?.unwrap();
    let after = f.money()?;
    f.finish().await?;
    assert_eq!(completion.notice, StageNotice::Staged);
    assert_eq!(row.position_id, position_b);
    assert_eq!(after, before);
    for (events, worker_started, row) in deliveries {
        assert!(
            !worker_started,
            "retention must not promote an unproved hint"
        );
        assert!(row.is_none());
        assert!(
            events
                .iter()
                .any(|e| e["signature"] == a.signature
                    && e["reason"] == "original_generation_unknown")
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_failed_ack_allows_first_delivery_and_legacy_once_after_writer_restart(
) -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    f.follow_source("source-a")?;
    let mut a = f.sell("ack-retry-a", "source-a");
    a.amount_out = 0.0000001; // Existing legacy owned-intent fallback.
    a.exact_amounts.as_mut().unwrap().amount_out_raw = "100".into();
    f.conn()?.execute_batch(
        "CREATE TRIGGER fail_ack BEFORE INSERT ON observed_swaps
        BEGIN SELECT RAISE(ABORT,'ack_retry_refused'); END;",
    )?;
    if let Err(error) = f.send(&a, false).await {
        assert!(format!("{error:#}").contains("ack_retry_refused"));
    }
    assert!(
        !f.recent.contains(&a.signature),
        "failed ACK must remove recent marker"
    );
    assert!(f.scheduler.source_sells.is_empty());
    assert!(f.staged(&a.signature)?.is_none());
    assert!(f.shadow_completion().await?.is_none());
    // Preserve the writer's terminal-failure policy, join it, then simulate its restart.
    let writer = f.writer.take().unwrap();
    let shutdown = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || writer.shutdown()),
    )
    .await??;
    assert!(format!("{:#}", shutdown.unwrap_err()).contains("ack_retry_refused"));
    f.conn()?.execute_batch("DROP TRIGGER fail_ack")?;
    f.writer = Some(ObservedSwapWriter::start_for_test(
        f.path.to_string_lossy().into(),
        8,
        8,
    )?);
    f.send(&a, false).await?;
    f.stage_completion().await?;
    let signal = f
        .shadow_completion()
        .await?
        .expect("first durable legacy delivery");
    let row = f
        .store
        .load_copy_signal_by_signal_id(&signal.signal_id)?
        .unwrap();
    assert_eq!(row.status, "execution_sell_intent");
    let before_repeat = f.money()?;
    f.send(&a, false).await?;
    f.stage_completion().await?;
    let repeated_signal = f.shadow_completion().await?;
    let after_repeat = f.money()?;
    f.finish().await?;
    assert!(repeated_signal.is_none());
    assert_eq!(after_repeat, before_repeat);
    Ok(())
}
