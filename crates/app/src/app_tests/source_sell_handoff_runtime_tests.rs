use super::{source_sell_event_capture::capture, source_sell_handoff_fixture::*};
use crate::source_sell_staging::{SourceSellStaging, StageNotice};
use anyhow::{Context, Result};
use std::time::Duration;

#[tokio::test]
async fn batch57_single_acked_sell_reaches_staging_without_redelivery() -> Result<()> {
    let mut f = new_unfollowed()?;
    let b = f.sell("batch56-required-b", "source-b");
    let position = f.position()?;
    let before = f.money()?;
    capacity_cut(&mut f, &b).await?; // B delivered exactly once, no runnable worker yet.
    recover(&mut f, &b.signature).await?; // New real scheduler recovery, independent of execution flags.
    let row = f
        .staged(&b.signature)?
        .context("ACKed eligible B must autonomously reach staging")?;
    assert_eq!(row.position_id, position);
    assert_eq!(format!("{:?}", row.event), format!("{b:?}"));
    produce(&f, &b.signature)?;
    money_unchanged_except_signal(&f, &before)?;
    f.finish().await?;
    eprintln!(
        "B57_DELIVERY_GREEN signature={} position={} observed=1 staging=1 signal=1 deliveries=1",
        b.signature, position
    );
    Ok(())
}

#[tokio::test]
async fn batch57_observed_ack_failure_never_claims_scheduled_and_rolls_back() -> Result<()> {
    let mut f = new_unfollowed()?;
    let e = f.sell("ack-fault", "source-b");
    let before = f.money()?;
    f.conn()?.execute_batch("CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoffs BEGIN SELECT RAISE(ABORT,'handoff_ack_fault'); END;")?;
    let (result, events) = capture(f.send(&e, true)).await;
    let shutdown = f.finish().await;
    result?; // Existing ingress policy represents this as a refused candidate.
    assert!(format!("{:#}", shutdown.unwrap_err()).contains("handoff_ack_fault"));
    event_reason(&events, &e.signature, "observed_ack_failed");
    assert!(events.iter().all(|e| e["reason"] != "scheduled"));
    assert!(f.store.load_source_sell_handoff(&e.signature)?.is_none());
    assert!(f.store.load_observed_swaps_since(f.now)?.is_empty());
    assert_eq!(f.money()?, before);
    Ok(())
}

#[tokio::test]
async fn batch57_committed_staging_lost_completion_reopen_preserves_original_signal_once(
) -> Result<()> {
    let mut f = new_unfollowed()?;
    let e = f.sell("committed", "source-b");
    let p = f.position()?;
    let before = f.money()?;
    f.send(&e, true).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.staged(&e.signature)?.is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let mut old = std::mem::replace(&mut f.scheduler.source_sells, SourceSellStaging::new());
    f.reopen_without_delivery_memory().await?;
    recover(&mut f, &e.signature).await?;
    assert_eq!(f.staged(&e.signature)?.unwrap().position_id, p);
    produce(&f, &e.signature)?;
    let first = f.money()?;
    f.reopen_without_delivery_memory().await?;
    recover(&mut f, &e.signature).await?;
    assert_eq!(
        crate::execution_source_sell_producer::produce(&f.store)?.inserted,
        0
    );
    let completion = tokio::time::timeout(Duration::from_secs(5), old.finish_next())
        .await??
        .context("old committed worker")?;
    assert_eq!(completion.signature, e.signature);
    assert_eq!(completion.notice, StageNotice::Staged);
    assert!(old.is_empty());
    f.finish().await?;
    assert_eq!(first, f.money()?);
    money_unchanged_except_signal(&f, &before)?;
    Ok(())
}
