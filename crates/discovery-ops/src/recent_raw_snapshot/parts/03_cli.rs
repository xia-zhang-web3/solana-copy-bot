use super::*;

pub(crate) fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

pub(crate) fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut journal_db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--journal-db-path" => {
                journal_db_path = Some(PathBuf::from(parse_string_arg(
                    "--journal-db-path",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--scheduled" => scheduled = true,
            "--force" => force = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if scheduled == output_path.is_some() {
        bail!("exactly one of --output or --scheduled must be provided");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        journal_db_path,
        output_path,
        scheduled,
        force,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
}

pub(crate) fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

pub(crate) fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

pub(crate) fn run(config: Config) -> Result<SnapshotExecution> {
    run_with_snapshot_policy_override(config, None)
}

pub(crate) fn run_with_snapshot_policy_override(
    config: Config,
    snapshot_policy_override: Option<SqliteSnapshotPolicy>,
) -> Result<SnapshotExecution> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let source_db_path = resolve_db_path(
        &config.config_path,
        config.journal_db_path.as_deref(),
        &loaded_config.recent_raw_journal.path,
    );
    if !source_db_path.exists() {
        bail!(
            "recent raw journal db does not exist: {}",
            source_db_path.display()
        );
    }
    let source_store = SqliteStore::open_read_only(&source_db_path).with_context(|| {
        format!(
            "failed opening recent raw journal db {}",
            source_db_path.display()
        )
    })?;
    let mut snapshot_context = snapshot_context(&source_db_path, &source_store)?;
    if let Some(snapshot_policy_override) = snapshot_policy_override {
        snapshot_context.policy = snapshot_policy_override;
    }

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &source_db_path,
            &source_store,
            &loaded_config.runtime_restore_ops.journal_snapshot_dir,
            loaded_config
                .runtime_restore_ops
                .journal_snapshot_cadence_minutes,
            loaded_config.runtime_restore_ops.journal_snapshot_retention,
            &snapshot_context,
        )?
    } else {
        let snapshot_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let metadata_path = journal_snapshot_metadata_path(&snapshot_path);
        match write_snapshot_with_policy(
            &source_db_path,
            &source_store,
            &snapshot_path,
            config.now,
            &snapshot_context.policy,
        ) {
            Ok((manifest, summary)) => {
                match write_json_atomic(&metadata_path, &manifest)
                    .with_context(|| format!("failed writing {}", metadata_path.display()))
                {
                    Ok(()) => render_output(
                        SnapshotState::Written,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::ExplicitOutput,
                        &snapshot_context,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        None,
                        SnapshotOutputContext::default(),
                    ),
                    Err(error) => render_output(
                        SnapshotState::HardFailure,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::UnchangedDueToHardFailure,
                        &snapshot_context,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        Some(error.to_string()),
                        SnapshotOutputContext::default(),
                    ),
                }
            }
            Err(SnapshotWriteError::RetryableBusy { summary }) => render_output(
                SnapshotState::RetryableBusy,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToRetryableBusy,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                Some(&summary),
                Some(summary_reason(&summary)),
                None,
                None,
                SnapshotOutputContext::default(),
            ),
            Err(SnapshotWriteError::Deferred { summary }) => render_output(
                SnapshotState::Deferred,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::ExplicitOutputDeferred,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                Some(&summary),
                None,
                Some(deferred_summary_reason(&summary)),
                None,
                SnapshotOutputContext::default(),
            ),
            Err(SnapshotWriteError::HardFailure { summary, reason }) => render_output(
                SnapshotState::HardFailure,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                summary.as_ref(),
                None,
                None,
                Some(reason),
                SnapshotOutputContext::default(),
            ),
        }
    };

    let rendered_output = if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing journal snapshot json")?
    } else {
        render_human(&output)
    };
    Ok(SnapshotExecution {
        rendered_output,
        exit_code: state_exit_code(&output),
    })
}
