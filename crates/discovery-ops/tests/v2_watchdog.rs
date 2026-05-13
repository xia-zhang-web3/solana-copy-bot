use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery_ops::v2_watchdog::{run_watchdog, WatchdogConfig, WatchdogState};
use copybot_discovery_v2::{
    discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, SqliteDiscoveryStore,
};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

fn prod_config_path() -> PathBuf {
    repo_root().join("configs/prod.toml")
}

fn seed_publication(
    db_path: &Path,
    now: DateTime<Utc>,
    last_published_at: DateTime<Utc>,
    active_wallets: &[&str],
) -> Result<()> {
    let loaded = load_from_path(&prod_config_path())?;
    let store = SqliteDiscoveryStore::open(db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let options =
        DiscoveryV2BuildOptions::from_config(&loaded.discovery, loaded.execution.enabled, now);
    let publish_options = DiscoveryV2BuildOptions::from_config(
        &loaded.discovery,
        loaded.execution.enabled,
        last_published_at,
    );
    let fingerprint = discovery_v2_policy_fingerprint(&loaded.discovery, &loaded.shadow, &options);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::seconds(10),
        slot: 42,
        signature: "watchdog-test-cursor".to_string(),
    };
    store.upsert_discovery_runtime_cursor(&cursor)?;
    for wallet in active_wallets {
        store.activate_follow_wallet(wallet, now, "watchdog_test")?;
    }
    store.set_discovery_publication_state_with_identity(
        &DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "watchdog_test_ready".to_string(),
            last_published_at: Some(last_published_at),
            last_published_window_start: Some(publish_options.window_start()),
            published_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
            published_wallet_ids: Some(
                active_wallets
                    .iter()
                    .map(|wallet| wallet.to_string())
                    .collect(),
            ),
        },
        false,
        Some(fingerprint.as_str()),
        Some(&cursor),
    )?;
    Ok(())
}

fn watchdog_config(db_path: &Path, now: DateTime<Utc>) -> WatchdogConfig {
    WatchdogConfig {
        config_path: prod_config_path(),
        db_path: Some(db_path.to_path_buf()),
        min_active_wallets: Some(2),
        json: true,
        fail_on_warn: true,
        now,
    }
}

#[test]
fn v2_watchdog_reports_ok_for_fresh_publication() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("runtime.db");
    let now = DateTime::parse_from_rfc3339("2026-05-13T08:10:00Z")?.with_timezone(&Utc);
    seed_publication(
        &db_path,
        now,
        now - Duration::minutes(20),
        &["wallet_a", "wallet_b"],
    )?;

    let output = run_watchdog(watchdog_config(&db_path, now))?;

    assert_eq!(output.state, WatchdogState::Ok);
    assert!(output.findings.is_empty());
    assert_eq!(output.active_follow_wallet_count, 2);
    Ok(())
}

#[test]
fn v2_watchdog_warns_after_one_missed_publish_cycle() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("runtime.db");
    let now = DateTime::parse_from_rfc3339("2026-05-13T08:10:00Z")?.with_timezone(&Utc);
    seed_publication(
        &db_path,
        now,
        now - Duration::minutes(61),
        &["wallet_a", "wallet_b"],
    )?;

    let output = run_watchdog(watchdog_config(&db_path, now))?;

    assert_eq!(output.state, WatchdogState::Warn);
    assert!(output
        .findings
        .iter()
        .any(|finding| finding.code == "discovery_v2_publication_one_cycle_late"));
    Ok(())
}

#[test]
fn v2_watchdog_critical_after_two_missed_publish_cycles() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("runtime.db");
    let now = DateTime::parse_from_rfc3339("2026-05-13T08:10:00Z")?.with_timezone(&Utc);
    seed_publication(
        &db_path,
        now,
        now - Duration::minutes(91),
        &["wallet_a", "wallet_b"],
    )?;

    let output = run_watchdog(watchdog_config(&db_path, now))?;

    assert_eq!(output.state, WatchdogState::Critical);
    assert!(output
        .findings
        .iter()
        .any(|finding| finding.code == "discovery_v2_publication_stale"));
    Ok(())
}
