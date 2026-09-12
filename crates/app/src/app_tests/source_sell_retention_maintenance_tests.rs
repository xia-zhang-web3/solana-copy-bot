use super::source_sell_ingress_fixture::Ingress;
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_storage_core::ExecutionSourceSellPromotionOutcome;

#[tokio::test]
async fn actual_retention_maintenance_keeps_staged_sell_of_open_position() -> Result<()> {
    let mut f = Ingress::new()?;
    f.now = Utc::now() - Duration::days(3);
    f.buy("retention-buy", "source-a")?;
    let a = f.sell("retention-a", "source-a");
    f.send(&a, true).await?;
    f.stage_completion().await?;
    let staged = f.staged(&a.signature)?.unwrap();
    let b = f.sell("ordinary-b", "other-source");
    assert!(f.store.insert_observed_swap(&b)?);
    f.finish().await?;
    let summary = crate::observed_swap_writer::run_observed_swap_retention_maintenance_once(
        f.path.to_str().unwrap(),
        crate::observed_swap_writer::ObservedSwapRetentionConfig::production(1),
        None,
    )?;
    assert_eq!(summary.raw_deleted_rows, 1);
    assert_eq!(summary.raw_delete_batches, 1);
    assert!(summary.completed_full_sweep);
    f.reopen()?;
    let ExecutionSourceSellPromotionOutcome::Inserted(binding) = f
        .store
        .promote_execution_source_sell_intent(&staged.intent_id)?
    else {
        panic!("maintenance must preserve source proof");
    };
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&binding.signal_id)?
        .unwrap();
    assert_eq!(
        f.store
            .execution_sell_intent_position_block_reason(&signal)?,
        None
    );
    assert_eq!(f.position()?, staged.position_id);
    Ok(())
}
