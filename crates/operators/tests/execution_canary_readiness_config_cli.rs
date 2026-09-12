mod readiness_cli_support;

use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::SqliteStore;
use readiness_cli_support::*;
use rusqlite::Connection;

#[test]
fn db_only_preserves_known_readiness_and_unknown_economics() -> Result<()> {
    let f = Fixture::new(true)?;
    let r = f.cli("db-only", &["--db-path", path(&f.db), "--json"], 0)?;
    assert_success(&r, true);
    assert_db_report(&r, &f.db, false)
}

#[test]
fn config_only_loads_and_preserves_unknown_readiness() -> Result<()> {
    let f = Fixture::new(false)?;
    let r = f.cli(
        "config-only-unknown",
        &["--config", path(&f.config), "--json"],
        0,
    )?;
    assert_success(&r, false);
    assert_db_report(&r, &f.db, true)
}

#[test]
fn config_only_loads_and_preserves_known_readiness() -> Result<()> {
    let f = Fixture::new(true)?;
    let r = f.cli(
        "config-only-known",
        &["--config", path(&f.config), "--json"],
        0,
    )?;
    assert_success(&r, true);
    assert_db_report(&r, &f.db, true)
}

#[test]
fn explicit_db_overrides_valid_config_without_loading_it() -> Result<()> {
    let explicit = Fixture::new(true)?;
    let other = Fixture::new(false)?;
    write_config(&explicit.config, &other.db)?;
    let control = other.cli(
        "config-db-control",
        &["--config", path(&other.config), "--json"],
        0,
    )?;
    assert_success(&control, false);
    let r = explicit.cli(
        "override-valid",
        &[
            "--config",
            path(&explicit.config),
            "--db-path",
            path(&explicit.db),
            "--json",
        ],
        0,
    )?;
    assert_success(&r, true);
    assert_db_report(&r, &explicit.db, false)
}

#[test]
fn explicit_db_ignores_missing_config() -> Result<()> {
    let f = Fixture::new(true)?;
    std::fs::remove_file(&f.config)?;
    let r = f.cli(
        "override-missing",
        &[
            "--db-path",
            path(&f.db),
            "--config",
            path(&f.config),
            "--json",
        ],
        0,
    )?;
    assert_success(&r, true);
    assert_db_report(&r, &f.db, false)
}

#[test]
fn explicit_db_ignores_invalid_config() -> Result<()> {
    let f = Fixture::new(true)?;
    std::fs::write(&f.config, "[invalid TOML")?;
    let r = f.cli(
        "override-invalid",
        &[
            "--db-path",
            path(&f.db),
            "--config",
            path(&f.config),
            "--json",
        ],
        0,
    )?;
    assert_success(&r, true);
    assert_db_report(&r, &f.db, false)
}

#[test]
fn missing_and_invalid_config_keep_config_failure() -> Result<()> {
    let f = Fixture::new(true)?;
    for invalid in [false, true] {
        if invalid {
            std::fs::write(&f.config, "[invalid TOML")?;
        } else {
            std::fs::remove_file(&f.config)?;
        }
        let r = f.cli(
            if invalid {
                "config-invalid"
            } else {
                "config-missing"
            },
            &["--config", path(&f.config), "--json"],
            1,
        )?;
        assert_failure(
            &r,
            "config_unreadable",
            &format!("failed to load config: {}", f.config.display()),
        );
        assert_eq!(r["config_loaded"], false);
    }
    Ok(())
}

#[test]
fn loaded_config_survives_db_open_failure_without_creating_db() -> Result<()> {
    let f = Fixture::new(true)?;
    let missing = f.dir.path().join("absent-parent/missing.db");
    write_config(&f.config, &missing)?;
    let error = SqliteStore::open_read_only(&missing)
        .err()
        .expect("missing DB must fail")
        .to_string();
    assert!(error.contains("failed opening read-only discovery sqlite store"));
    let direct = f.cli(
        "db-missing-control",
        &["--db-path", path(&missing), "--json"],
        1,
    )?;
    assert_failure(&direct, "db_unreadable", &error);
    assert_db_report(&direct, &missing, false)?;
    let r = f.cli(
        "config-loaded-db-open-failure",
        &["--config", path(&f.config), "--json"],
        1,
    )?;
    assert!(!missing.exists());
    assert!(!missing.parent().unwrap().exists());
    assert_failure(&r, "db_unreadable", &error);
    assert_db_report(&r, &missing, true)
}

#[test]
fn loaded_config_survives_summary_query_failure() -> Result<()> {
    let f = Fixture::new(true)?;
    Connection::open(&f.db)?.execute_batch("DROP TABLE orders;")?;
    let store = SqliteStore::open_read_only(&f.db)?;
    let error = store
        .execution_canary_readiness_summary(Utc::now())
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed to prepare execution canary status count query"
    );
    assert!(format!("{error:#}").contains("no such table: orders"));
    drop(store);
    assert_query_failure(&f, "summary", &error.to_string())
}

#[test]
fn loaded_config_survives_failed_expense_query_failure() -> Result<()> {
    let f = Fixture::new(true)?;
    Connection::open(&f.db)?
        .execute_batch("CREATE TABLE execution_failed_expense_tasks(order_id TEXT);")?;
    let store = SqliteStore::open_read_only(&f.db)?;
    let now = Utc::now();
    assert_eq!(
        store
            .execution_canary_readiness_summary(now)?
            .readiness_status,
        "would_enter"
    );
    let error = store
        .execution_failed_expense_report(now - chrono::Duration::hours(24), now, 50)
        .unwrap_err();
    assert!(format!("{error:#}").contains("no such column: t.operation_at"));
    drop(store);
    assert_query_failure(&f, "failed-expenses", &error.to_string())
}

#[test]
fn loaded_config_survives_window_query_failure_after_two_successful_queries() -> Result<()> {
    let f = Fixture::new(true)?;
    // Only the older row is malformed: latest summary and expense queries still succeed.
    Connection::open(&f.db)?.execute_batch(
        "INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id)
         VALUES('exec-canary:malformed-older','older','metis-canary','0000-invalid',
                'execution_canary_reserved','older-client');",
    )?;
    let store = SqliteStore::open_read_only(&f.db)?;
    let now = Utc::now();
    assert_eq!(
        store
            .execution_canary_readiness_summary(now)?
            .readiness_status,
        "would_enter"
    );
    store.execution_failed_expense_report(now - chrono::Duration::hours(24), now, 50)?;
    let error = store
        .execution_canary_readiness_window_summary(now, 50)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed reading execution canary readiness window"
    );
    assert!(matches!(
        error.downcast_ref::<rusqlite::Error>(),
        Some(rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            _
        ))
    ));
    drop(store);
    assert_query_failure(&f, "window", &error.to_string())
}

fn assert_query_failure(f: &Fixture, stage: &str, error: &str) -> Result<()> {
    eprintln!("intended query failure {stage}: {error}");
    let direct = f.cli(
        &format!("db-query-{stage}-control"),
        &["--db-path", path(&f.db), "--json"],
        1,
    )?;
    assert_failure(&direct, "db_unreadable", error);
    assert_db_report(&direct, &f.db, false)?;
    let r = f.cli(
        &format!("config-loaded-query-{stage}-failure"),
        &["--config", path(&f.config), "--json"],
        1,
    )?;
    assert_failure(&r, "db_unreadable", error);
    assert_db_report(&r, &f.db, true)
}

#[test]
fn cli_and_json_required_errors_happen_before_config_loading() -> Result<()> {
    let f = Fixture::new(true)?;
    for (label, args, reason, error) in [
        (
            "json-required",
            vec!["--config", path(&f.config)],
            "json_required",
            "--json is required for operator output",
        ),
        (
            "cli-unknown",
            vec!["--config", path(&f.config), "--json", "--unknown"],
            "cli_error",
            "unknown argument: --unknown",
        ),
        (
            "cli-missing-source",
            vec!["--json"],
            "cli_error",
            "either --config or --db-path is required",
        ),
        (
            "cli-missing-value",
            vec!["--config"],
            "cli_error",
            "--config requires a value",
        ),
        (
            "cli-limit-zero",
            vec!["--config", path(&f.config), "--json", "--limit", "0"],
            "cli_error",
            "--limit must be between 1 and 200, got 0",
        ),
        (
            "cli-limit-high",
            vec!["--config", path(&f.config), "--json", "--limit", "201"],
            "cli_error",
            "--limit must be between 1 and 200, got 201",
        ),
    ] {
        let r = f.cli(label, &args, 1)?;
        assert_failure(&r, reason, error);
        assert_eq!(r["config_loaded"], false);
    }
    Ok(())
}
