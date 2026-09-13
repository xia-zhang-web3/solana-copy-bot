use super::source_sell_eviction_fixture::HeldEviction;
use super::{source_sell_event_capture::capture, source_sell_ingress_fixture::Ingress};
use crate::source_sell_staging::{SourceSellStaging, StageNotice};
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellReject as Reject;

#[tokio::test]
async fn source_sell_delivery_a_failure_b_success_and_redelivery_preserve_identity_and_money(
) -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("event-a", "source-a");
    let b = f.sell("event-b", "source-a");
    f.conn()?.execute_batch(
        "CREATE TRIGGER fail_a BEFORE INSERT ON execution_source_sell_intents
        WHEN NEW.event_signature='event-a' BEGIN SELECT RAISE(ABORT,'fault_a'); END;",
    )?;
    let before = f.money()?;
    let (result, events) = capture(async {
        f.send(&a, true).await?;
        assert_eq!(
            f.stage_completion().await?.notice,
            StageNotice::WorkerFailed
        );
        assert!(f.staged(&a.signature)?.is_none());
        assert!(f.recent.contains(&a.signature));
        assert_eq!(
            f.conn()?.query_row(
                "SELECT count(*) FROM observed_swaps WHERE signature='event-a'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        f.send(&b, true).await?;
        assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
        Ok::<_, anyhow::Error>(())
    })
    .await;
    result?;
    let error = events
        .iter()
        .find(|e| e["reason"] == "staging_failed")
        .expect("production failure event");
    assert_eq!(error["signature"], a.signature);
    assert!(error["detail"].contains("fault_a"));
    assert!(error["detail"].len() <= 512);
    assert!(events
        .iter()
        .any(|e| e["reason"] == "staged" && e["signature"] == b.signature));
    f.conn()?.execute_batch("DROP TRIGGER fail_a")?;
    f.send(&a, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
    let row = f.staged(&a.signature)?.unwrap();
    f.send(&a, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::Existing);
    assert_eq!(
        format!("{:?}", f.staged(&a.signature)?.unwrap()),
        format!("{row:?}")
    );
    assert_eq!(f.money()?, before);
    assert_eq!(
        f.conn()?.query_row(
            "SELECT count(*) FROM execution_source_sell_intents",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        2
    );
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn source_sell_delivery_busy_worker_is_bounded_and_allows_explicit_redelivery() -> Result<()>
{
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("a", "source-a");
    let b = f.sell("b", "source-a");
    let release = f.pause_worker();
    f.send(&a, true).await?;
    let (result, events) = capture(f.send(&b, true)).await;
    result?;
    assert!(events
        .iter()
        .any(|e| e["signature"] == "b" && e["reason"] == "worker_capacity"));
    assert!(f.staged("b")?.is_none());
    release.send(())?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
    assert!(
        f.scheduler.source_sells.is_empty(),
        "no active proof worker before the next recovery visit"
    );
    f.send(&b, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn source_sell_delivery_lost_or_evicted_binding_never_uses_new_same_mint_generation(
) -> Result<()> {
    for evict in [false, true] {
        let mut f = Ingress::new()?;
        f.buy("a", "source-a")?;
        let original_position = f.position()?;
        let a = f.sell("lost-a", "source-a");
        f.scheduler.source_sells.before_proof = Some(Box::new(|| panic!("bounded test panic")));
        f.send(&a, true).await?;
        assert_eq!(f.stage_completion().await?.notice, StageNotice::WorkerPanic);
        let held = if evict {
            Some(HeldEviction::enter(&mut f, &a).await?)
        } else {
            f.scheduler.source_sells = SourceSellStaging::new();
            f.reopen()?;
            None
        };
        f.store
            .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
        f.buy("b", "source-a")?; // Same timestamp, different position ID.
        let before = f.money()?;
        if let Some(held) = held {
            held.release_and_drain(&mut f, &a).await?;
        }
        for _ in 0..2 {
            let (result, events) = capture(async {
                f.send(&a, true).await?;
                f.stage_completion().await
            })
            .await;
            let completion = result?;
            assert_eq!(completion.signature, a.signature);
            assert_eq!(
                completion.notice,
                StageNotice::Rejected(Reject::GenerationMismatch)
            );
            assert!(events
                .iter()
                .any(|e| e["signature"] == "lost-a" && e["reason"] == "generation_mismatch"));
            assert_eq!(
                f.store
                    .load_source_sell_handoff(&a.signature)?
                    .unwrap()
                    .original_position_id
                    .as_deref(),
                Some(original_position.as_str())
            );
            assert!(f.scheduler.source_sells.is_empty());
            assert!(f.staged(&a.signature)?.is_none());
        }
        assert_eq!(f.money()?, before);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_delivery_mutated_payload_does_not_replace_retry_binding() -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("a", "source-a");
    f.scheduler.source_sells.before_proof = Some(Box::new(|| panic!("before proof")));
    f.send(&a, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::WorkerPanic);
    let mut altered = a.clone();
    altered.exact_amounts.as_mut().unwrap().amount_in_raw = "4001".into();
    let (result, events) = capture(f.send(&altered, true)).await;
    result?;
    assert!(events
        .iter()
        .any(|e| e["signature"] == "a" && e["reason"] == "event_identity_conflict"));
    assert!(f.scheduler.source_sells.is_empty());
    f.send(&a, true).await?;
    assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
    assert_eq!(f.staged("a")?.unwrap().event.exact_amounts, a.exact_amounts);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn source_sell_delivery_failed_ack_cannot_start_proof_or_claim_staged() -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("ack-a", "source-a");
    f.conn()?.execute_batch(
        "CREATE TRIGGER fail_ack BEFORE INSERT ON observed_swaps
        BEGIN SELECT RAISE(ABORT,'ack_refused'); END;",
    )?;
    let (result, events) = capture(f.send(&a, true)).await;
    // Writer policy owns whether this nonfatal SQL refusal terminates the writer.
    if let Err(error) = result {
        assert!(format!("{error:#}").contains("ack_refused"));
    }
    assert!(events
        .iter()
        .any(|e| e["signature"] == "ack-a" && e["reason"] == "observed_ack_failed"));
    assert!(!events
        .iter()
        .any(|e| matches!(e["reason"].as_str(), "scheduled" | "staged" | "existing")));
    assert!(f.scheduler.source_sells.is_empty());
    assert!(f.staged("ack-a")?.is_none());
    let result = f.finish().await;
    if let Err(error) = result {
        assert!(format!("{error:#}").contains("ack_refused"));
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_delivery_fatal_sqlite_completion_keeps_restart_contract_and_signature(
) -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let a = f.sell("fatal-a", "source-a");
    f.conn()?.execute_batch(
        "CREATE TRIGGER fail_stage BEFORE INSERT ON execution_source_sell_intents
        BEGIN SELECT RAISE(ABORT,'disk I/O error'); END;",
    )?;
    f.send(&a, true).await?;
    let (result, events) = capture(f.stage_completion()).await;
    assert!(format!("{:#}", result.unwrap_err()).contains("fatal sqlite I/O"));
    assert!(events
        .iter()
        .any(|e| e["signature"] == "fatal-a" && e["reason"] == "fatal_sqlite"));
    assert!(f.staged("fatal-a")?.is_none());
    f.finish().await?;
    Ok(())
}
