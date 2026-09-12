use super::source_guard_fixture::Fixture;
use super::source_sell_sweep_fixture::add_signal;
use anyhow::Result;
use copybot_storage_core::*;

async fn prefix(candidate: bool, cross_family: bool) -> Result<()> {
    let mut f = if cross_family {
        Fixture::new().await?
    } else {
        Fixture::legacy_parent().await?
    };
    f.config.canary_batch_limit = 1;
    f.config.quote_canary_enabled = false;
    let mut old = Vec::new();
    for n in 0..12 {
        f.f.signal = add_signal(&f.f, &format!("old-{n:03}"))?;
        f.event_id = format!("quote:old-{n:03}");
        old.push(f.retry()?);
    }
    if cross_family {
        f.config.canary_entry_submit_enabled = true;
        f.config.canary_max_open_positions = 10;
        for id in &old {
            f.f.store
                .mark_execution_canary_submitted_unknown(id, f.f.now, "unknown")?;
            // Explicit later proof corruption, while the unrelated B source remains valid.
            f.f.conn()?.execute("UPDATE execution_source_sell_intents SET staged_at='unknown' WHERE intent_id IN
                (SELECT intent_id FROM execution_source_sell_promotions WHERE signal_id=(SELECT signal_id FROM orders WHERE order_id=?1))",[id])?;
        }
    } else {
        f.f.replace(4000)?;
    }
    f.f.signal = add_signal(&f.f, "valid-b")?;
    f.event_id = "quote:valid-b".into();
    let b = f.retry()?;
    if candidate {
        for (n, id) in std::iter::once(&b).chain(old.iter()).enumerate() {
            let at = f.f.now + chrono::Duration::seconds(2 + n as i64);
            f.f.store.mark_execution_canary_failed(
                id,
                at,
                EXECUTION_ERROR_BUILD_FAILED,
                "transient quote failure",
            )?;
            f.f.store
                .mark_execution_canary_failed_build_retry_candidate(
                    id,
                    at,
                    "retry_failed_sell_with_owned_position_amount",
                )?;
        }
    }
    let before = old
        .iter()
        .map(|id| f.f.store.load_execution_canary_order(id))
        .collect::<Result<Vec<_>>>()?;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone());
    let first_at = if cross_family {
        chrono::Utc::now()
    } else {
        f.f.now + chrono::Duration::seconds(20)
    };
    let first = runner.process_tick(&f.f.store, first_at).await?;
    if cross_family {
        assert_eq!(first.source_sell_refusals.count(), 0);
        for n in 1..=old.len() + 1 {
            if f.rpc.count("sendTransaction") == 1 {
                break;
            }
            f.f.store = SqliteStore::open(&f.f.path)?;
            runner
                .process_tick(&f.f.store, first_at + chrono::Duration::seconds(n as i64))
                .await?;
        }
        f.finish().await?;
        // Unknown SELLs own this same position. Fair traversal must not duplicate its exit.
        assert_eq!(f.rpc.count("sendTransaction"), 0, "{first:?}");
        assert!(f
            .f
            .store
            .load_execution_canary_order(&b)?
            .unwrap()
            .tx_signature
            .is_none());
        assert!(f.f.store.load_execution_canary_dispatch(&b)?.is_none());
        assert_eq!(
            old.iter()
                .map(|id| f.f.store.load_execution_canary_order(id))
                .collect::<Result<Vec<_>>>()?,
            before
        );
        return Ok(());
    }
    assert_eq!(first.source_sell_refusals.count(), 8, "{first:?}");
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    // Reopening storage does not reset the live runner's bounded traversal.
    f.f.store = SqliteStore::open(&f.f.path)?;
    let second = runner
        .process_tick(&f.f.store, f.f.now + chrono::Duration::seconds(21))
        .await;
    f.finish().await?;
    let second = second?;
    assert_eq!(
        f.rpc.count("sendTransaction"),
        1,
        "candidate={candidate}: {second:?}"
    );
    assert_eq!(
        f.f.store.load_execution_canary_order(&b)?.unwrap().status,
        EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(second.source_sell_refusals.count(), 4);
    assert!(old
        .iter()
        .any(|id| id == second.source_sell_refusals.order_id()));
    let event = super::submit_refusal_fixture::capture(|| {
        crate::telemetry::record_execution_canary_tick(&second)
    });
    assert_eq!(
        event["source_sell_refusal_id"],
        second.source_sell_refusals.order_id()
    );
    assert_eq!(
        event["source_sell_refusal_reason"],
        "source_sell_generation_mismatch"
    );
    assert_eq!(
        old.iter()
            .map(|id| f.f.store.load_execution_canary_order(id))
            .collect::<Result<Vec<_>>>()?,
        before
    );
    Ok(())
}
#[tokio::test]
async fn source_guard_actual_tick_candidate_prefix_exceeding_budget_progresses_after_reopen(
) -> Result<()> {
    prefix(true, false).await
}
#[tokio::test]
async fn source_guard_actual_tick_not_sent_prefix_exceeding_budget_progresses_after_reopen(
) -> Result<()> {
    prefix(false, false).await
}
#[tokio::test]
async fn source_guard_unsigned_unknown_prefix_reconciles_only_and_keeps_sell_progress() -> Result<()>
{
    prefix(false, true).await
}

#[tokio::test]
async fn source_guard_actual_tick_fresh_quote_prefix_progresses_without_reserving_a() -> Result<()>
{
    let mut f = Fixture::legacy_parent().await?;
    f.config.canary_batch_limit = 1;
    let mut old = vec![f.f.signal.signal_id.clone()];
    for n in 0..11 {
        old.push(add_signal(&f.f, &format!("old-{n:03}"))?.signal_id);
    }
    f.f.replace(4000)?;
    let b = add_signal(&f.f, "valid-b")?;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone());
    let first = runner
        .process_tick(&f.f.store, f.f.now + chrono::Duration::seconds(3))
        .await?;
    // One refusal comes from the pre-existing owned-quote sweep, eight from fresh-submit traversal.
    assert_eq!(first.quote_close_candidates, 1);
    assert_eq!(first.source_sell_refusals.count(), 9, "{first:?}");
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    let second = runner
        .process_tick(&f.f.store, f.f.now + chrono::Duration::seconds(4))
        .await;
    f.finish().await?;
    let second = second?;
    assert_eq!(f.rpc.count("sendTransaction"), 1, "{second:?}");
    assert_eq!(second.quote_close_candidates, 1);
    assert_eq!(second.source_sell_refusals.count(), 5);
    assert_eq!(
        f.f.store
            .load_execution_canary_order_by_signal(&b.signal_id)?
            .unwrap()
            .status,
        EXECUTION_STATUS_CANARY_SUBMITTED
    );
    for id in &old {
        assert!(f
            .f
            .store
            .load_execution_canary_order_by_signal(id)?
            .is_none());
    }
    let event = super::submit_refusal_fixture::capture(|| {
        crate::telemetry::record_execution_canary_tick(&second)
    });
    assert!(old
        .iter()
        .any(|id| event["source_sell_refusal_id"] == id.as_str()));
    assert_eq!(
        event["source_sell_refusal_reason"],
        "source_sell_generation_mismatch"
    );
    Ok(())
}
