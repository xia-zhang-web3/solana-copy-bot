#[path = "source_sell_promotion_fixture.rs"]
mod fixture;
use crate::backend::SqliteStore;
use anyhow::Result;
use chrono::Duration;
use fixture::*;
#[path = "source_retention_coverage_cases.rs"]
mod coverage;
#[path = "source_retention_failure_cases.rs"]
mod failures;
#[path = "source_retention_lifecycle_cases.rs"]
mod lifecycle;

#[test]
fn actual_cleanup_preserves_open_source_and_advances_past_pin_at_limit_one() -> Result<()> {
    for prune in [false, true] {
        for was_promoted in [false, true] {
            let mut d = Db::new()?;
            d.proven("buy-a", "source-a")?;
            let a = d.sell("pin-a", "source-a");
            let mut b = d.sell("delete-b", "other-source");
            b.ts_utc += Duration::seconds(1);
            let mut c = d.sell("fresh-c", "other-source");
            c.ts_utc += Duration::seconds(20);
            let cutoff = d.now + Duration::seconds(10);
            let store = SqliteStore::open(&d.path)?;
            store.insert_recent_raw_journal_batch(&[a.clone(), b.clone(), c.clone()], c.ts_utc)?;
            let staged = inserted(
                d.store
                    .stage_execution_source_sell_intent(&a, &d.position()?)?,
            );
            let before = format!("{staged:?}");
            if was_promoted {
                promoted(
                    d.store
                        .promote_execution_source_sell_intent(&staged.intent_id)?,
                );
            }
            let deleted = if prune {
                store.prune_recent_raw_journal_before_batch(cutoff, 1, c.ts_utc)?
            } else {
                store.delete_observed_swaps_before_batch(cutoff, 1)?
            };
            assert_eq!(deleted, 1);
            drop(store);
            d.reopen()?;
            let exists = |signature: &str| -> Result<bool> {
                Ok(d.conn()?.query_row(
                    "SELECT EXISTS(SELECT 1 FROM observed_swaps WHERE signature=?1)",
                    [signature],
                    |r| r.get(0),
                )?)
            };
            assert!(
                exists(&a.signature)?,
                "actual cleanup removed staged OPEN SELL source"
            );
            assert!(!exists(&b.signature)?, "pin must not consume LIMIT1");
            assert!(exists(&c.signature)?);
            assert_eq!(
                format!(
                    "{:?}",
                    d.store
                        .load_execution_source_sell_intent(&staged.intent_id)?
                        .unwrap()
                ),
                before
            );
            let outcome = d
                .store
                .promote_execution_source_sell_intent(&staged.intent_id)?;
            let binding = match outcome {
                Outcome::Inserted(b) if !was_promoted => b,
                Outcome::Existing(b) if was_promoted => b,
                other => panic!("source must still prove original generation: {other:?}"),
            };
            assert_eq!(
                d.store
                    .execution_sell_intent_position_block_reason(&signal(&d, &binding)?)?,
                None
            );
        }
    }
    Ok(())
}
