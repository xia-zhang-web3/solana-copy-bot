use super::*;
use anyhow::Context;
use std::path::Path;
use tempfile::tempdir;

fn make_publication_state_test_store(name: &str) -> Result<SqliteStore> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join(format!("{name}.db"));
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;
    std::mem::forget(temp);
    Ok(store)
}

#[test]
fn publication_state_write_diagnostics_distinguish_fresh_publish_vs_carried_forward_stale_row_stage1(
) -> Result<()> {
    let store = make_publication_state_test_store(
        "publication-state-write-diagnostics-distinguish-fresh-vs-carried",
    )?;
    let fresh_publish_at = DateTime::parse_from_rfc3339("2026-04-12T15:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    let fresh_window_start = DateTime::parse_from_rfc3339("2026-04-12T14:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    let fresh_update = DiscoveryPublicationStateUpdate {
        runtime_mode: DiscoveryRuntimeMode::Healthy,
        reason: "fresh_publish".to_string(),
        last_published_at: Some(fresh_publish_at),
        last_published_window_start: Some(fresh_window_start),
        published_scoring_source: Some("discovery_v2_operational_window".to_string()),
        published_wallet_ids: Some(vec!["wallet-a".to_string(), "wallet-b".to_string()]),
    };
    let previous = store.discovery_publication_state_read_only()?;
    store.set_discovery_publication_state_with_options(&fresh_update, false, Some("policy-a"))?;
    let fresh_row = store
        .discovery_publication_state_read_only()?
        .expect("fresh publication row should exist");
    let fresh_diagnostics = snapshot_discovery_publication_state_write_diagnostics(
        previous.as_ref(),
        &fresh_row,
        &fresh_update,
        false,
    );
    assert_eq!(fresh_diagnostics.write_kind, "fresh_publish");
    assert!(fresh_diagnostics.published_universe_persisted);
    assert_eq!(fresh_diagnostics.previous_published_wallet_count, 0);
    assert_eq!(fresh_diagnostics.new_published_wallet_count, 2);
    assert!(!fresh_diagnostics.stale_fields_carried_forward);
    assert_eq!(
        fresh_diagnostics.new_last_published_at,
        Some(fresh_publish_at)
    );

    let carried_update = DiscoveryPublicationStateUpdate {
        runtime_mode: DiscoveryRuntimeMode::FailClosed,
        reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
        last_published_at: None,
        last_published_window_start: None,
        published_scoring_source: Some("discovery_v2_operational_window".to_string()),
        published_wallet_ids: None,
    };
    let previous = Some(fresh_row.clone());
    store.set_discovery_publication_state_with_options(&carried_update, false, None)?;
    let carried_row = store
        .discovery_publication_state_read_only()?
        .expect("carried publication row should exist");
    let carried_diagnostics = snapshot_discovery_publication_state_write_diagnostics(
        previous.as_ref(),
        &carried_row,
        &carried_update,
        false,
    );
    assert_eq!(
        carried_diagnostics.write_kind,
        "carried_forward_stale_truth"
    );
    assert!(!carried_diagnostics.published_universe_persisted);
    assert!(carried_diagnostics.stale_fields_carried_forward);
    assert!(carried_diagnostics.stale_last_published_at_carried_forward);
    assert!(carried_diagnostics.stale_published_wallet_ids_carried_forward);
    assert_eq!(
        carried_diagnostics.previous_published_wallet_count,
        carried_diagnostics.new_published_wallet_count
    );
    assert_eq!(
        carried_diagnostics.new_last_published_at,
        Some(fresh_publish_at)
    );
    Ok(())
}

#[test]
fn publication_state_write_diagnostics_preserve_fail_closed_semantics_while_surfacing_carry_forward_metadata_stage1(
) -> Result<()> {
    let store = make_publication_state_test_store(
        "publication-state-write-diagnostics-preserve-fail-closed",
    )?;
    let stale_publish_at = DateTime::parse_from_rfc3339("2026-04-06T17:55:23Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    let stale_window_start = DateTime::parse_from_rfc3339("2026-04-06T17:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
        runtime_mode: DiscoveryRuntimeMode::Healthy,
        reason: "seed_stale_complete_row".to_string(),
        last_published_at: Some(stale_publish_at),
        last_published_window_start: Some(stale_window_start),
        published_scoring_source: Some("discovery_v2_operational_window".to_string()),
        published_wallet_ids: Some((0..7usize).map(|idx| format!("wallet-{idx}")).collect()),
    })?;
    let before = store
        .discovery_publication_state_read_only()?
        .expect("seeded publication row should exist");

    let withheld_update = DiscoveryPublicationStateUpdate {
        runtime_mode: DiscoveryRuntimeMode::FailClosed,
        reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
        last_published_at: None,
        last_published_window_start: None,
        published_scoring_source: Some("discovery_v2_operational_window".to_string()),
        published_wallet_ids: None,
    };
    store.set_discovery_publication_state_with_options(&withheld_update, false, None)?;
    let after = store
        .discovery_publication_state_read_only()?
        .expect("withheld publication row should exist");
    let diagnostics = snapshot_discovery_publication_state_write_diagnostics(
        Some(&before),
        &after,
        &withheld_update,
        false,
    );

    assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
    assert_eq!(
        after.reason,
        "publication_truth_withheld_missing_exact_published_wallet_ids"
    );
    assert_eq!(after.last_published_at, Some(stale_publish_at));
    assert_eq!(after.last_published_window_start, Some(stale_window_start));
    assert_eq!(after.published_wallet_ids.as_ref().map(Vec::len), Some(7));
    assert!(
            after.updated_at > before.updated_at,
            "withheld fail-closed writes must still refresh updated_at so operators can distinguish stale-but-updating rows from frozen rows"
        );
    assert_eq!(diagnostics.write_kind, "carried_forward_stale_truth");
    assert!(!diagnostics.published_universe_persisted);
    assert!(diagnostics.stale_fields_carried_forward);
    assert_eq!(diagnostics.runtime_mode, DiscoveryRuntimeMode::FailClosed);
    assert_eq!(
        diagnostics.reason,
        "publication_truth_withheld_missing_exact_published_wallet_ids"
    );
    assert_eq!(diagnostics.new_published_wallet_count, 7);
    Ok(())
}
