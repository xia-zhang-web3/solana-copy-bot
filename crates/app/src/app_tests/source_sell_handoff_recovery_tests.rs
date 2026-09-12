use super::{source_sell_event_capture::capture, source_sell_handoff_fixture::*};
use crate::source_sell_staging::StageNotice;
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellReject as Reject;

#[tokio::test]
async fn batch57_pre_worker_restart_recovers_p_and_never_rebinds_to_q() -> Result<()> {
    for replace in [false, true] {
        let mut f = new_unfollowed()?;
        let e = f.sell("restart-b", "source-b");
        let p = f.position()?;
        capacity_cut(&mut f, &e).await?;
        f.reopen_without_delivery_memory().await?; // Cut before B ever spawned, not a claimed process kill.
        if replace {
            f.store.record_execution_canary_manual_terminal_write_off(
                "mint", "tiny", "close-p", f.now,
            )?;
            f.buy("q", "source-b")?;
            assert_ne!(f.position()?, p);
        }
        let before = f.money()?;
        let outputs = recover(&mut f, &e.signature).await?;
        assert!(outputs.iter().any(|c| c.signature == e.signature
            && c.notice
                == if replace {
                    StageNotice::Rejected(Reject::GenerationMismatch)
                } else {
                    StageNotice::Staged
                }));
        assert_eq!(
            f.store
                .load_source_sell_handoff(&e.signature)?
                .unwrap()
                .original_position_id,
            Some(p.clone())
        );
        if replace {
            assert!(f.staged(&e.signature)?.is_none());
        } else {
            assert_eq!(f.staged(&e.signature)?.unwrap().position_id, p);
            produce(&f, &e.signature)?;
        }
        money_unchanged_except_signal(&f, &before)?;
        // Repeated upstream delivery and recent eviction cannot replace the durable original.
        f.root_evict_recent_with_production_dedupe();
        f.send(&e, true).await?;
        let repeated = f.stage_completion().await?;
        assert_eq!(
            repeated.notice,
            if replace {
                StageNotice::Rejected(Reject::GenerationMismatch)
            } else {
                StageNotice::Existing
            }
        );
        let fresh = f.sell("fresh-current", "source-b");
        f.send(&fresh, true).await?;
        assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
        assert_eq!(
            f.staged(&fresh.signature)?.unwrap().position_id,
            f.position()?
        );
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn batch57_retryable_a_recovers_without_replay_and_new_b_progresses() -> Result<()> {
    let mut f = new_unfollowed()?;
    let a = f.sell("retry-a", "source-b");
    let before = f.money()?;
    f.conn()?.execute_batch("CREATE TRIGGER fault BEFORE INSERT ON execution_source_sell_intents WHEN NEW.event_signature='retry-a' BEGIN SELECT RAISE(ABORT,'database is locked: batch57'); END;")?;
    let (result, events) = capture(async {
        f.send(&a, true).await?;
        f.stage_completion().await
    })
    .await;
    assert_eq!(result?.notice, StageNotice::RetryableSqlite);
    event_reason(&events, &a.signature, "retryable_sqlite");
    assert_eq!(
        f.store
            .load_source_sell_handoff(&a.signature)?
            .unwrap()
            .disposition,
        "pending"
    );
    f.conn()?.execute_batch("DROP TRIGGER fault")?;
    f.reopen_without_delivery_memory().await?;
    let b = f.sell("new-b", "source-b");
    capacity_cut(&mut f, &b).await?;
    recover(&mut f, &b.signature).await?;
    recover(&mut f, &a.signature).await?;
    assert_eq!(f.staged(&a.signature)?.unwrap().position_id, f.position()?);
    assert_eq!(f.staged(&b.signature)?.unwrap().position_id, f.position()?);
    let out = crate::execution_source_sell_producer::produce(&f.store)?;
    assert_eq!(out.inserted, 2);
    money_unchanged_except_signal(&f, &before)?;
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn batch57_recovery_empty_pass_is_throttled_and_does_not_advance_again() -> Result<()> {
    let mut f = new_unfollowed()?;
    f.scheduler
        .source_sells
        .recover(&f.store, &f.path.to_string_lossy())?;
    f.conn()?.execute_batch("CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoff_cursor BEGIN SELECT RAISE(ABORT,'second empty pass'); END;")?;
    for _ in 0..100 {
        f.scheduler
            .source_sells
            .recover(&f.store, &f.path.to_string_lossy())?;
    }
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn batch57_periodic_recovery_stages_with_execution_disabled_and_no_publication() -> Result<()>
{
    let mut f = new_unfollowed()?;
    let mut config = copybot_config::ExecutionConfig::default();
    config.enabled = false;
    config.canary_enabled = false;
    config.canary_tiny_submit_enabled = false;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(config);
    assert!(!runner.is_enabled());
    assert!(f.follow.active.is_empty());
    // Startup empty pass establishes the cooldown before an isolated single delivery.
    f.scheduler
        .source_sells
        .recover(&f.store, &f.path.to_string_lossy())?;
    let b = f.sell("periodic-b", "source-b");
    capacity_cut(&mut f, &b).await?;
    let before = f.money()?;
    let mut timer = crate::source_sell_staging::SourceSellStaging::recovery_interval();
    let mut visits = 0;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while f.staged(&b.signature)?.is_none() {
            timer.tick().await;
            visits += 1;
            f.scheduler
                .source_sells
                .recover(&f.store, &f.path.to_string_lossy())?;
            if !f.scheduler.source_sells.is_empty() {
                f.stage_completion().await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert!((1..=3).contains(&visits), "bounded real interval: {visits}");
    assert_eq!(f.staged(&b.signature)?.unwrap().position_id, f.position()?);
    assert_eq!(
        f.money()?,
        before,
        "disabled execution recovery only stages"
    );
    f.finish().await?;
    Ok(())
}
