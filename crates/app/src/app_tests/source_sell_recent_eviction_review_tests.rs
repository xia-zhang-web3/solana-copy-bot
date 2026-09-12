use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::source_sell_staging::StageNotice;
use anyhow::Result;
use std::time::Duration;

async fn duplicate_unknown_after_recent_eviction(evict_recent: bool) -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("original-a", "source-a")?;
    let original_position = f.position()?;
    let a = f.sell("retained-unproved-a", "source-a");
    assert!(
        tokio::time::timeout(Duration::from_secs(5), f.writer.as_ref().unwrap().write(&a))
            .await??
    );
    f.store
        .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
    f.buy("next-b", "source-a")?;
    let next_position = f.position()?;
    assert_ne!(original_position, next_position);
    let (result, first_events) = capture(f.send(&a, true)).await;
    result?;
    assert!(f.scheduler.source_sells.is_empty());
    assert!(first_events
        .iter()
        .any(|e| e["reason"] == "original_generation_unknown"));
    assert!(f.recent.contains(&a.signature));
    // Exercise the real dedupe FIFO/capacity without 32768 unrelated SQLite writes.
    // Only recent cache changes; the retained Duplicate-ACK hint is not cleared.
    if evict_recent {
        f.root_evict_recent_with_production_dedupe();
    }
    assert_eq!(f.recent.contains(&a.signature), !evict_recent);
    f.store
        .delete_observed_swaps_before_batch(a.ts_utc + chrono::Duration::seconds(1), 10)?;
    let before = f.money()?;
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let (result, events) = capture(f.send(&a, true)).await;
        result?;
        let completion = if f.scheduler.source_sells.is_empty() {
            None
        } else {
            Some(f.stage_completion().await?)
        };
        outcomes.push((completion, f.staged(&a.signature)?, events));
    }
    let fresh_b = f.sell("independent-fresh-b", "source-a");
    f.send(&fresh_b, true).await?;
    let fresh_completion = f.stage_completion().await?;
    let fresh_row = f.staged(&fresh_b.signature)?.unwrap();
    let after = f.money()?;
    f.finish().await?;
    assert_eq!(before, after);
    assert_eq!(fresh_completion.notice, StageNotice::Staged);
    assert_eq!(fresh_row.position_id, next_position);
    for (completion, row, events) in outcomes {
        assert!(row.is_none(), "RETAINED UNKNOWN UPGRADED: original={original_position}, next={next_position}, completion={completion:?}, row={row:?}");
        assert!(completion.is_none(), "unknown must not launch a worker");
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
async fn root_source_sell_duplicate_unknown_survives_real_recent_eviction() -> Result<()> {
    duplicate_unknown_after_recent_eviction(true).await
}
#[tokio::test]
async fn root_source_sell_duplicate_unknown_with_recent_marker_control() -> Result<()> {
    duplicate_unknown_after_recent_eviction(false).await
}
