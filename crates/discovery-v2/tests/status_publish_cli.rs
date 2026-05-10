use anyhow::{bail, Context, Result};
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
