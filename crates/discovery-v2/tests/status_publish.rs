use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{
    build_discovery_v2_status, discovery_v2_policy_fingerprint, publish_discovery_v2_status,
    DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only, DiscoveryRuntimeMode,
    SqliteDiscoveryStore,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_MINT: &str = "TokenMint111111111111111111111111111111111";

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn swap(wallet: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap_with_token(wallet, TOKEN_MINT, signature, slot, ts_utc)
}

fn swap_with_token(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token_mint.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn tail_coverage_swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap_with_token(
        "tail_wallet",
        "TailCoverageToken11111111111111111111111111",
        signature,
        slot,
        ts_utc,
    )
}

fn sell_with_token(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: token_mint.to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 10.0,
        amount_out: 1.2,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions {
        now,
        window_minutes: 24 * 60,
        max_tail_lag_seconds: 1_200,
        max_rows: 100,
        time_budget_ms: 5_000,
        execution_enabled: false,
    }
}

fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 60;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}

fn insert_quality(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    liquidity_sol: Option<f64>,
) -> Result<()> {
    insert_quality_for_token(store, TOKEN_MINT, now, liquidity_sol)
}

fn insert_quality_for_token(
    store: &SqliteDiscoveryStore,
    token_mint: &str,
    now: DateTime<Utc>,
    liquidity_sol: Option<f64>,
) -> Result<()> {
    store.upsert_token_quality_cache(token_mint, Some(5), liquidity_sol, Some(60), now)
}

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

#[test]
fn prod_live_v2_configs_are_active_day_satisfiable() -> Result<()> {
    for relative in [
        "configs/prod.toml",
        "configs/live.toml",
        "ops/server_templates/live.server.toml.example",
    ] {
        let config = copybot_config::load_from_path(repo_path(relative))?;
        assert!(
            config.discovery.min_active_days <= config.discovery.scoring_window_days,
            "{relative} has min_active_days above scoring_window_days"
        );
    }
    Ok(())
}

#[test]
fn publish_commit_on_old_schema_does_not_prepare_schema_before_rejection() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("old-runtime.db");
    drop(SqliteDiscoveryStore::open(&db_path)?);
    let config_path = dir.path().join("live.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[sqlite]
path = "{}"
"#,
            db_path.display()
        ),
    )?;

    let output = Command::new(env!("CARGO_BIN_EXE_discovery_v2_publish"))
        .args([
            "--config",
            config_path.to_str().expect("utf8 config path"),
            "--commit",
            "--now",
            "2026-05-03T10:00:00Z",
        ])
        .output()?;

    assert!(!output.status.success());
    let read_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let schema_error = validate_discovery_v2_schema_read_only(&read_store)
        .expect_err("publish --commit must not prepare schema before status rejection");
    assert!(schema_error.to_string().contains("missing required table"));
    Ok(())
}

#[test]
fn policy_fingerprint_changes_when_shadow_or_execution_identity_changes() -> Result<()> {
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let (mut discovery, mut shadow) = strict_policy();
    let options = options(now);
    let base = discovery_v2_policy_fingerprint(&discovery, &shadow, &options);

    shadow.min_liquidity_sol += 1.0;
    let shadow_changed = discovery_v2_policy_fingerprint(&discovery, &shadow, &options);
    assert_ne!(base, shadow_changed);

    discovery.rug_lookahead_seconds += 1;
    let rug_changed = discovery_v2_policy_fingerprint(&discovery, &shadow, &options);
    assert_ne!(shadow_changed, rug_changed);

    discovery.decay_window_days += 1;
    let decay_changed = discovery_v2_policy_fingerprint(&discovery, &shadow, &options);
    assert_ne!(rug_changed, decay_changed);

    discovery.metric_snapshot_interval_seconds += 1;
    let metric_snapshot_changed = discovery_v2_policy_fingerprint(&discovery, &shadow, &options);
    assert_ne!(decay_changed, metric_snapshot_changed);

    let mut execution_changed = options;
    execution_changed.execution_enabled = true;
    let execution_changed =
        discovery_v2_policy_fingerprint(&discovery, &shadow, &execution_changed);
    assert_ne!(metric_snapshot_changed, execution_changed);
    Ok(())
}

#[test]
fn status_ready_when_tail_sample_scan_and_candidates_are_valid() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let token_a = "ReadyToken11111111111111111111111111111111";
    let token_b = "ReadyToken22222222222222222222222222222222";
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "wallet_a",
            token_a,
            "sig-a",
            10,
            now - Duration::minutes(10),
        ),
        swap_with_token("wallet_b", token_b, "sig-b", 11, now - Duration::minutes(5)),
        tail_coverage_swap("sig-tail", 12, now - Duration::minutes(2)),
    ])?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green);
    assert!(status.blockers.is_empty());
    assert_eq!(status.scan.rows_scanned, 3);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert_eq!(status.source, DISCOVERY_V2_SCORING_SOURCE);
    Ok(())
}

#[test]
fn status_blocks_when_active_days_exceed_scan_window() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.min_active_days = 2;
    let mut options = options(now);
    options.window_minutes = 24 * 60;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options)?;

    assert!(!status.production_green);
    assert!(status
        .blockers
        .contains(&"discovery_v2_active_days_unsatisfiable".to_string()));
    Ok(())
}

include!("status_publish/quality.rs");

include!("status_publish/runtime.rs");

#[test]
fn publish_commit_writes_followlist_and_publication_state_when_green() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(5)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let report = publish_discovery_v2_status(&store, status, true)?;

    assert!(report.committed);
    assert_eq!(report.published_wallet_count, 1);
    assert!(store.list_active_follow_wallets()?.contains("wallet_a"));
    let state = store
        .discovery_publication_state_read_only()?
        .expect("publication state");
    assert_eq!(state.runtime_mode, DiscoveryRuntimeMode::Healthy);
    assert_eq!(
        state.published_scoring_source.as_deref(),
        Some(DISCOVERY_V2_SCORING_SOURCE)
    );
    let cursor = store
        .load_discovery_runtime_cursor()?
        .expect("runtime cursor persisted");
    assert_eq!(state.publication_runtime_cursor.as_ref(), Some(&cursor));
    assert_eq!(cursor.slot, 11);
    assert_eq!(cursor.signature, "sig-tail");
    Ok(())
}

#[test]
fn publish_commit_refuses_to_mutate_when_blocked() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let (discovery, shadow) = strict_policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let err = publish_discovery_v2_status(&store, status, true).expect_err("blocked publish");

    assert!(err
        .to_string()
        .contains("refusing to mutate publication state"));
    assert!(store.discovery_publication_state_read_only()?.is_none());
    Ok(())
}
