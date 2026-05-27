use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{materialize_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only, SqliteDiscoveryStore,
};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tempfile::tempdir;

const CLI_TEST_TIMEOUT: StdDuration = StdDuration::from_secs(60);
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_MINT: &str = "CliMaterializedToken111111111111111111111";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

fn command_output_with_timeout(command: &mut Command) -> Result<Output> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().context("failed spawning CLI under test")?;
    let deadline = StdInstant::now() + CLI_TEST_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().context("failed polling CLI under test")? {
            let mut stdout = Vec::new();
            if let Some(mut pipe) = child.stdout.take() {
                pipe.read_to_end(&mut stdout)
                    .context("failed reading CLI stdout")?;
            }
            let mut stderr = Vec::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_end(&mut stderr)
                    .context("failed reading CLI stderr")?;
            }
            return Ok(Output {
                status,
                stdout,
                stderr,
            });
        }
        if StdInstant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("CLI under test exceeded {CLI_TEST_TIMEOUT:?} timeout");
        }
        thread::sleep(StdDuration::from_millis(10));
    }
}

fn buy(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn seed_green_materialized_status(
    store: &SqliteDiscoveryStore,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<()> {
    store.insert_observed_swaps_batch(&[
        buy(
            "tail-wallet",
            "CliCoverageToken111111111111111111111",
            "sig-coverage",
            9,
            now - Duration::hours(25),
        ),
        buy(
            "wallet-a",
            TOKEN_MINT,
            "sig-wallet-a",
            10,
            now - Duration::minutes(10),
        ),
        buy(
            "tail-wallet",
            "CliTailToken111111111111111111111111",
            "sig-tail",
            11,
            now - Duration::seconds(30),
        ),
    ])?;
    store.upsert_token_quality_cache(TOKEN_MINT, Some(5), Some(1.0), Some(60), now)?;
    let loaded = copybot_config::load_from_path(config_path)?;
    let options =
        DiscoveryV2BuildOptions::from_config(&loaded.discovery, loaded.execution.enabled, now);
    let (status, _report) =
        materialize_discovery_v2_status(store, &loaded.discovery, &loaded.shadow, options)?;
    assert!(
        status.production_green,
        "unexpected materialized blockers: {:?}",
        status.blockers
    );
    Ok(())
}

fn write_green_config(config_path: &Path, db_path: &Path) -> Result<()> {
    fs::write(
        config_path,
        format!(
            r#"
[sqlite]
path = "{}"

[risk]
shadow_universe_min_active_follow_wallets = 1

[discovery]
scoring_window_days = 1
decay_window_days = 1
follow_top_n = 1
min_leader_notional_sol = 0.0
min_trades = 1
min_active_days = 1
min_score = 0.0
min_buy_count = 1
min_tradable_ratio = 0.25
require_open_positions_for_publication = true
max_rug_ratio = 0.6
rug_lookahead_seconds = 60
metric_snapshot_interval_seconds = 3600
refresh_seconds = 60
thin_market_min_volume_sol = 0.5
thin_market_min_unique_traders = 1
max_window_swaps_in_memory = 100
max_fetch_swaps_per_cycle = 100
fetch_time_budget_ms = 5000

[shadow]
quality_gates_enabled = true
min_token_age_seconds = 30
min_holders = 5
min_liquidity_sol = 1.0
min_volume_5m_sol = 0.5
min_unique_traders_5m = 1
"#,
            db_path.display()
        ),
    )?;
    Ok(())
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
fn scheduled_publish_unit_uses_materialized_status() -> Result<()> {
    let unit = fs::read_to_string(repo_path(
        "ops/server_templates/copybot-discovery-v2-publish.service",
    ))?;
    let exec_start = unit
        .lines()
        .find(|line| line.starts_with("ExecStart="))
        .context("publish service missing ExecStart")?;

    assert!(
        exec_start.contains(" --materialized-status "),
        "scheduled publish must commit materialized status, not rebuild full V2 status: {exec_start}"
    );
    assert!(
        !exec_start.contains(" -n "),
        "scheduled publish must wait for the shared discovery lock instead of skipping: {exec_start}"
    );
    assert!(
        !unit.contains("SuccessExitStatus=75"),
        "scheduled publish must fail visibly instead of marking lock skips as success"
    );
    Ok(())
}

#[test]
fn scheduled_prepare_quality_unit_avoids_incremental_evidence_writer() -> Result<()> {
    let unit = fs::read_to_string(repo_path(
        "ops/server_templates/copybot-discovery-v2-prepare-quality.service",
    ))?;
    let exec_start = unit
        .lines()
        .find(|line| line.starts_with("ExecStart="))
        .context("prepare quality service missing ExecStart")?;

    assert!(
        exec_start.contains(" --materialize-status"),
        "scheduled prepare quality must keep materializing publish status: {exec_start}"
    );
    assert!(
        !exec_start.contains(" --incremental"),
        "scheduled prepare quality must not prune/write incremental evidence on the live DB: {exec_start}"
    );
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

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_v2_publish")).args([
            "--config",
            config_path.to_str().expect("utf8 config path"),
            "--commit",
        ]),
    )?;

    assert!(!output.status.success());
    let read_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let schema_error = validate_discovery_v2_schema_read_only(&read_store)
        .expect_err("publish --commit must not prepare schema before status rejection");
    assert!(schema_error.to_string().contains("missing required table"));
    Ok(())
}

#[test]
fn publish_commit_requires_daemon_restart_acknowledgement() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);
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

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_v2_publish")).args([
            "--config",
            config_path.to_str().expect("utf8 config path"),
            "--commit",
        ]),
    )?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--acknowledge-daemon-restart-required")
    );
    Ok(())
}

#[test]
fn publish_cli_rejects_operator_supplied_clock_and_policy_overrides() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
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

    for args in [
        vec!["--now", "2026-05-03T10:00:00Z"],
        vec!["--window-minutes", "60"],
        vec!["--max-tail-lag-seconds", "3600"],
        vec!["--max-rows", "10"],
        vec!["--time-budget-ms", "1"],
    ] {
        let output = command_output_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_discovery_v2_publish"))
                .arg("--config")
                .arg(config_path.to_str().expect("utf8 config path"))
                .arg("--commit")
                .args(args),
        )?;
        assert!(
            !output.status.success(),
            "publish override unexpectedly succeeded: stdout=\n{}\nstderr=\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("not accepted by production publish"),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn status_cli_rejects_operator_supplied_clock_and_policy_overrides() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
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

    for args in [
        vec!["--now", "2026-05-03T10:00:00Z"],
        vec!["--window-minutes", "60"],
        vec!["--max-tail-lag-seconds", "3600"],
        vec!["--max-rows", "10"],
        vec!["--time-budget-ms", "1"],
    ] {
        let output = command_output_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_discovery_v2_status"))
                .arg("--config")
                .arg(config_path.to_str().expect("utf8 config path"))
                .args(args),
        )?;
        assert!(
            !output.status.success(),
            "status override unexpectedly succeeded: stdout=\n{}\nstderr=\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("not accepted by production status"),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn status_cli_uses_materialized_snapshot_by_default() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
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
    let loaded = copybot_config::load_from_path(&config_path)?;
    let materialized_now = Utc::now();
    let options = DiscoveryV2BuildOptions::from_config(
        &loaded.discovery,
        loaded.execution.enabled,
        materialized_now,
    );
    let (status, _report) =
        materialize_discovery_v2_status(&store, &loaded.discovery, &loaded.shadow, options)?;
    drop(store);

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_v2_status"))
            .args(["--config", config_path.to_str().expect("utf8 config path")]),
    )?;

    assert!(
        output.status.success(),
        "status cli failed: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let cli_now = chrono::DateTime::parse_from_rfc3339(
        json["now"].as_str().expect("status now must be a string"),
    )?
    .with_timezone(&Utc);
    assert_eq!(cli_now, status.now);
    assert_eq!(
        json["build_elapsed_ms"].as_u64(),
        Some(status.build_elapsed_ms)
    );
    Ok(())
}

#[test]
fn prepare_quality_materialize_reuses_fresh_green_snapshot_before_scan() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let config_path = dir.path().join("live.toml");
    write_green_config(&config_path, &db_path)?;
    let now = Utc::now();
    seed_green_materialized_status(&store, &config_path, now)?;
    drop(store);

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_v2_prepare_quality")).args([
            "--config",
            config_path.to_str().expect("utf8 config path"),
            "--commit",
            "--materialize-status",
        ]),
    )?;

    assert!(
        output.status.success(),
        "prepare cli failed: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["skipped"].as_bool(), Some(true));
    assert_eq!(
        json["skip_reason"].as_str(),
        Some("discovery_v2_materialized_status_reusable")
    );
    assert_eq!(json["rows_scanned"].as_u64(), Some(0));
    assert_eq!(
        json["materialized_status"]["reused_existing_snapshot"].as_bool(),
        Some(true)
    );
    Ok(())
}

#[test]
fn incremental_prepare_quality_runs_even_when_materialized_status_is_reusable() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let config_path = dir.path().join("live.toml");
    write_green_config(&config_path, &db_path)?;
    let now = Utc::now();
    seed_green_materialized_status(&store, &config_path, now)?;
    drop(store);

    let output = command_output_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_discovery_v2_prepare_quality")).args([
            "--config",
            config_path.to_str().expect("utf8 config path"),
            "--commit",
            "--incremental",
            "--materialize-status",
        ]),
    )?;

    assert!(
        output.status.success(),
        "prepare cli failed: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["skipped"].as_bool(), Some(false));
    assert_eq!(json["committed"].as_bool(), Some(true));
    assert!(
        json["rows_scanned"].as_u64().unwrap_or(0) > 0,
        "incremental quality prepare should still scan the new tail while materialized status is reusable: {json}"
    );
    assert_eq!(
        json["materialized_status"]["committed"].as_bool(),
        Some(false)
    );
    assert_eq!(
        json["materialized_status"]["reused_existing_snapshot"].as_bool(),
        Some(true)
    );
    Ok(())
}
