use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::source_sell_staging::{StageNotice, SOURCE_SELL_BINDING_CAPACITY};
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellReject as Reject;

async fn known_redelivery_after_binding_loss(remove_observed: bool) -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("original-a", "source-a")?;
    let old_position = f.position()?;
    let a = f.sell("known-repeat-a", "source-a");
    f.scheduler.source_sells.before_proof =
        Some(Box::new(|| panic!("bounded first-attempt fault")));
    f.send(&a, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::WorkerPanic);
    assert!(f.staged(&a.signature)?.is_none());
    // Evict through actual incoming events; do not replace scheduler state in this probe.
    for i in 0..SOURCE_SELL_BINDING_CAPACITY {
        let filler = f.sell(&format!("real-eviction-{i}"), "foreign-source");
        f.send(&filler, true).await?;
        f.stage_completion().await?;
    }
    if remove_observed {
        f.store
            .delete_observed_swaps_before_batch(a.ts_utc + chrono::Duration::seconds(1), 10)?;
    }
    f.store
        .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
    f.buy("next-b", "source-a")?; // Equal timestamps, distinct actual BUY generation.
    let next_position = f.position()?;
    assert_ne!(old_position, next_position);
    assert!(
        f.recent.contains(&a.signature),
        "ingress knows this is a redelivery"
    );
    let before = f.money()?;
    let observed_before: i64 = f.conn()?.query_row(
        "SELECT count(*) FROM observed_swaps WHERE signature=?1",
        [&a.signature],
        |r| r.get(0),
    )?;
    let (result, first_events) = capture(async {
        f.send(&a, true).await?;
        f.stage_completion().await
    })
    .await;
    let completion = result?;
    let wrongly_rebound = f.staged(&a.signature)?;
    // Durable P survives binding loss; both visits refuse Q with the original ID.
    let (result, repeat_events) = capture(async {
        f.send(&a, true).await?;
        f.stage_completion().await
    })
    .await;
    let repeated = result?;
    let repeat_record = f.staged(&a.signature)?;
    let observed_after: i64 = f.conn()?.query_row(
        "SELECT count(*) FROM observed_swaps WHERE signature=?1",
        [&a.signature],
        |r| r.get(0),
    )?;
    // Independent new event is still eligible for current B.
    let fresh_b = f.sell("actual-new-event-b", "source-a");
    f.send(&fresh_b, true).await?;
    let fresh_completion = f.stage_completion().await?;
    let fresh_record = f.staged(&fresh_b.signature)?.unwrap();
    let after = f.money()?;
    f.finish().await?;
    assert_eq!(fresh_completion.notice, StageNotice::Staged);
    assert_eq!(fresh_record.position_id, next_position);
    assert!(wrongly_rebound.is_none(),
        "KNOWN REDELIVERY RETARGETED: original={old_position}, next={next_position}, outcome={completion:?}, staged={wrongly_rebound:?}");
    assert_eq!(
        completion.notice,
        StageNotice::Rejected(Reject::GenerationMismatch)
    );
    assert_eq!(
        repeated.notice,
        StageNotice::Rejected(Reject::GenerationMismatch)
    );
    assert_eq!(completion.signature, a.signature);
    assert_eq!(repeated.signature, a.signature);
    assert_eq!(
        f.store
            .load_source_sell_handoff(&a.signature)?
            .unwrap()
            .original_position_id
            .as_deref(),
        Some(old_position.as_str())
    );
    assert!(repeat_record.is_none());
    assert_eq!(
        observed_before, 1,
        "pending P remains pinned before terminal refusal"
    );
    assert_eq!(
        observed_after, observed_before,
        "duplicate ACK keeps the original canonical row"
    );
    assert_eq!(after, before);
    for events in [first_events, repeat_events] {
        assert!(events
            .iter()
            .any(|e| e["signature"] == a.signature && e["reason"] == "generation_mismatch"));
        assert!(!events
            .iter()
            .any(|e| matches!(e["reason"].as_str(), "staged" | "existing")));
    }
    Ok(())
}

#[tokio::test]
async fn root_source_sell_retained_recent_after_retention_must_not_retarget() -> Result<()> {
    known_redelivery_after_binding_loss(true).await
}

#[tokio::test]
async fn root_source_sell_retained_recent_with_observed_keeps_original_generation() -> Result<()> {
    known_redelivery_after_binding_loss(false).await
}
