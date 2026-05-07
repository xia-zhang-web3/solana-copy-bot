use std::collections::{BTreeMap, HashSet};

use anyhow::anyhow;

use super::*;
use crate::shadow_scheduler::{ShadowTaskKey, ShadowTaskOutput};

fn test_task_output(error: anyhow::Error) -> ShadowTaskOutput {
    ShadowTaskOutput {
        signature: "sig-shadow-test".to_string(),
        key: ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-a".to_string(),
        },
        outcome: Err(error),
    }
}

#[test]
fn handle_shadow_task_output_returns_error_on_fatal_sqlite_io() {
    let mut open_shadow_lots = HashSet::new();
    let mut shadow_drop_reason_counts = BTreeMap::new();
    let mut shadow_drop_stage_counts = BTreeMap::new();
    let task_output = test_task_output(anyhow!(
        "disk I/O error: Error code 4874: I/O error within the xShmMap method"
    ));

    let error = handle_shadow_task_output(
        task_output,
        &mut open_shadow_lots,
        &mut shadow_drop_reason_counts,
        &mut shadow_drop_stage_counts,
    )
    .expect_err("fatal sqlite I/O must bubble out of shadow task output handler");
    let error_text = format!("{error:#}");
    assert!(
        error_text.contains("shadow processing failed with fatal sqlite I/O"),
        "expected fatal shadow-processing context, got: {error_text}"
    );
    assert!(
        error_text.contains("xShmMap"),
        "expected fatal sqlite marker to survive error chain, got: {error_text}"
    );
    assert!(open_shadow_lots.is_empty());
    assert!(shadow_drop_reason_counts.is_empty());
    assert!(shadow_drop_stage_counts.is_empty());
}

#[test]
fn handle_shadow_task_output_warns_and_continues_on_busy_lock() -> Result<()> {
    let mut open_shadow_lots = HashSet::new();
    let mut shadow_drop_reason_counts = BTreeMap::new();
    let mut shadow_drop_stage_counts = BTreeMap::new();
    let task_output = test_task_output(anyhow!("database is locked"));

    handle_shadow_task_output(
        task_output,
        &mut open_shadow_lots,
        &mut shadow_drop_reason_counts,
        &mut shadow_drop_stage_counts,
    )?;

    assert!(open_shadow_lots.is_empty());
    assert!(shadow_drop_reason_counts.is_empty());
    assert!(shadow_drop_stage_counts.is_empty());
    Ok(())
}
