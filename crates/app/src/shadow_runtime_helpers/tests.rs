use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::SwapEvent;
use copybot_shadow::{FollowSnapshot, ShadowDropReason, ShadowService};
use copybot_storage_core::SqliteStore;

use super::*;
use crate::shadow_scheduler::{ShadowTaskKey, ShadowTaskOutput};

fn temp_shadow_helper_db_path(name: &str) -> PathBuf {
    let unique = format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    std::env::temp_dir().join(format!("copybot-app-shadow-helper-{unique}.db"))
}

fn test_task_output(error: anyhow::Error) -> ShadowTaskOutput {
    ShadowTaskOutput {
        signature: "sig-shadow-test".to_string(),
        key: ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-a".to_string(),
        },
        signal_id: None,
        side: None,
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
        None,
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
        None,
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

#[test]
fn shadow_task_retry_recovers_from_transient_sqlite_lock() -> Result<()> {
    let attempts = AtomicUsize::new(0);

    let result = retry_shadow_task_on_sqlite_contention("sig-shadow-test", || {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < 2 {
            Err(anyhow!("database is locked"))
        } else {
            Ok("recorded")
        }
    })?;

    assert_eq!(result, "recorded");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[test]
fn shadow_task_retry_does_not_retry_non_sqlite_errors() {
    let attempts = AtomicUsize::new(0);

    let error = retry_shadow_task_on_sqlite_contention("sig-shadow-test", || -> Result<()> {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(anyhow!("token quality rejected"))
    })
    .expect_err("non-sqlite errors must not be retried");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(error.to_string().contains("token quality rejected"));
}

#[test]
fn shadow_task_rechecks_followlist_for_queued_buy() -> Result<()> {
    let db_path = temp_shadow_helper_db_path("queued-buy-followlist-recheck");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let buy_ts = DateTime::parse_from_rfc3339("2026-05-26T12:51:08Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.activate_follow_wallet(
        "wallet-test",
        buy_ts - chrono::Duration::minutes(10),
        "test-promote",
    )?;
    store.deactivate_follow_wallet(
        "wallet-test",
        buy_ts - chrono::Duration::minutes(1),
        "test-demote",
    )?;

    let mut follow_snapshot = FollowSnapshot::default();
    follow_snapshot.active.insert("wallet-test".to_string());
    let swap = SwapEvent {
        wallet: "wallet-test".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-test".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: "sig-queued-buy-stale-followlist".to_string(),
        slot: 42,
        ts_utc: buy_ts,
        exact_amounts: None,
    };
    let task_input = ShadowTaskInput {
        key: ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_out.clone(),
        },
        swap,
        follow_snapshot: Arc::new(follow_snapshot),
    };

    let mut config = ShadowConfig::default();
    config.quality_gates_enabled = false;
    let output = shadow_task(
        ShadowService::new(config),
        db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?,
        task_input,
    );

    match output.outcome {
        Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotFollowed)) => {}
        other => panic!("queued stale buy must be dropped as not_followed, got {other:?}"),
    }
    assert!(store
        .list_copy_signals_by_status("shadow_recorded", 10)?
        .is_empty());

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
    Ok(())
}
