use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::observed_swap_writer::ObservedSwapWriter;
use crate::source_sell_staging::{StageCompletion, StageNotice};
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellReject as Reject;
use std::time::Duration;

async fn completion_if_running(f: &mut Ingress) -> Result<Option<StageCompletion>> {
    if f.scheduler.source_sells.is_empty() {
        Ok(None)
    } else {
        f.stage_completion().await.map(Some)
    }
}

#[tokio::test]
async fn source_sell_proven_retry_after_recent_eviction_preserves_original_generation() -> Result<()>
{
    for (capacity_refusal, replace_position) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let mut f = Ingress::new()?;
        let order_a = f.buy("original-a", "source-a")?;
        let position_a = f.position()?;
        let a = f.sell("durable-a", "source-a");
        if capacity_refusal {
            let busy = f.sell("busy", "source-a");
            let release = f.pause_worker();
            f.send(&busy, true).await?;
            let (result, events) = capture(f.send(&a, true)).await;
            result?;
            release.send(())?;
            assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
            assert!(events
                .iter()
                .any(|e| e["signature"] == a.signature && e["reason"] == "worker_capacity"));
        } else {
            f.conn()?.execute_batch(
                "CREATE TRIGGER fail_stage BEFORE INSERT ON execution_source_sell_intents
                BEGIN SELECT RAISE(ABORT,'retry_original_a'); END;",
            )?;
            f.send(&a, true).await?;
            assert_eq!(
                f.stage_completion().await?.notice,
                StageNotice::WorkerFailed
            );
            f.conn()?.execute_batch("DROP TRIGGER fail_stage")?;
        }
        assert!(f.staged(&a.signature)?.is_none());
        f.root_evict_recent_with_production_dedupe();
        assert!(!f.recent.contains(&a.signature));
        if replace_position {
            // Reinsert may retry a proven A, but cannot change its captured ID to B.
            f.store
                .delete_observed_swaps_before_batch(a.ts_utc + chrono::Duration::seconds(1), 10)?;
            f.store.record_execution_canary_manual_terminal_write_off(
                "mint", "tiny", "close-a", f.now,
            )?;
            f.buy("next-b", "source-a")?;
        }
        let current_position = f.position()?;
        let before = f.money()?;
        // Without retention this receives Duplicate ACK, which must keep proof.
        f.send(&a, true).await?;
        let first = completion_if_running(&mut f).await?;
        let first_row = f.staged(&a.signature)?;
        f.send(&a, true).await?;
        let second = completion_if_running(&mut f).await?;
        let second_row = f.staged(&a.signature)?;
        let fresh = f.sell("fresh-current", "source-a");
        f.send(&fresh, true).await?;
        let fresh_completion = completion_if_running(&mut f).await?;
        let fresh_row = f.staged(&fresh.signature)?;
        let after = f.money()?;
        f.finish().await?;
        assert_eq!(before, after);
        assert_eq!(fresh_completion.unwrap().notice, StageNotice::Staged);
        assert_eq!(fresh_row.unwrap().position_id, current_position);
        if replace_position {
            assert_ne!(position_a, current_position);
            for completion in [first, second] {
                assert_eq!(
                    completion.unwrap().notice,
                    StageNotice::Rejected(Reject::GenerationMismatch)
                );
            }
            assert!(first_row.is_none() && second_row.is_none());
        } else {
            assert_eq!(first.unwrap().notice, StageNotice::Staged);
            assert_eq!(second.unwrap().notice, StageNotice::Existing);
            let row = first_row.unwrap();
            assert_eq!(row.position_id, position_a);
            assert_eq!(row.buy_witness.order_id, order_a);
            assert_eq!(format!("{row:?}"), format!("{:?}", second_row.unwrap()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_failed_ack_retry_keeps_captured_a_when_new_b_opens() -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("original-a", "source-a")?;
    let position_a = f.position()?;
    let a = f.sell("pending-ack-a", "source-a");
    f.conn()?.execute_batch(
        "CREATE TRIGGER fail_ack BEFORE INSERT ON observed_swaps
        BEGIN SELECT RAISE(ABORT,'pending_ack_fault'); END;",
    )?;
    if let Err(error) = f.send(&a, true).await {
        assert!(format!("{error:#}").contains("pending_ack_fault"));
    }
    assert!(!f.recent.contains(&a.signature));
    assert!(f.scheduler.source_sells.is_empty());
    let writer = f.writer.take().unwrap();
    let shutdown = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || writer.shutdown()),
    )
    .await??;
    assert!(format!("{:#}", shutdown.unwrap_err()).contains("pending_ack_fault"));
    f.conn()?.execute_batch("DROP TRIGGER fail_ack")?;
    f.store
        .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
    f.buy("next-b", "source-a")?;
    let position_b = f.position()?;
    f.writer = Some(ObservedSwapWriter::start_for_test(
        f.path.to_string_lossy().into(),
        8,
        8,
    )?);
    let before = f.money()?;
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        f.send(&a, true).await?;
        outcomes.push((
            completion_if_running(&mut f).await?,
            f.staged(&a.signature)?,
        ));
    }
    let fresh_b = f.sell("fresh-b", "source-a");
    f.send(&fresh_b, true).await?;
    let fresh_completion = completion_if_running(&mut f).await?;
    let fresh_row = f.staged(&fresh_b.signature)?;
    let after = f.money()?;
    f.finish().await?;
    assert_ne!(position_a, position_b);
    assert_eq!(before, after);
    assert_eq!(fresh_completion.unwrap().notice, StageNotice::Staged);
    assert_eq!(fresh_row.unwrap().position_id, position_b);
    for (completion, row) in outcomes {
        assert_eq!(
            completion.unwrap().notice,
            StageNotice::Rejected(Reject::GenerationMismatch)
        );
        assert!(row.is_none());
    }
    Ok(())
}
