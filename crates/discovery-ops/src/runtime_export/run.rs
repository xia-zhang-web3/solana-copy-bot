fn run_command(command: Command) -> Result<String> {
    match command {
        Command::Export(config) => run_export(config),
    }
}

fn run_export(config: ExportConfig) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let freshness_gate = publication_freshness_gate(&loaded_config.discovery);

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &loaded_config.runtime_restore_ops.artifact_dir,
            loaded_config.runtime_restore_ops.artifact_cadence_minutes,
            loaded_config.runtime_restore_ops.artifact_retention,
            &db_path,
            &store,
            freshness_gate,
        )?
    } else {
        let output_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let artifact = export_runtime_artifact(&store, freshness_gate, config.now)
            .context("artifact export")?;
        let freshness = assess_runtime_artifact_freshness(&artifact, freshness_gate, config.now);
        write_json_atomic(&output_path, &artifact)
            .with_context(|| format!("failed writing {}", output_path.display()))?;
        render_output(
            "written",
            &config.config_path,
            &db_path,
            &output_path,
            None,
            None,
            None,
            &[],
            &artifact,
            freshness.fresh_under_export_gate,
            freshness.fresh_under_current_gate,
        )
    };

    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing export output json")
    } else {
        Ok(render_human(&output))
    }
}

fn run_scheduled(
    config: &ExportConfig,
    configured_artifact_dir: &str,
    cadence_minutes: u64,
    retention: usize,
    db_path: &Path,
    store: &SqliteStore,
    freshness_gate: DiscoveryPublicationFreshnessGate,
) -> Result<ExportOutput> {
    let artifact_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_artifact_dir));
    let latest_path = artifact_latest_path(&artifact_dir);
    if !config.force && latest_path.exists() {
        let latest_artifact: DiscoveryRuntimeArtifact = load_json(&latest_path)?;
        if config
            .now
            .signed_duration_since(latest_artifact.exported_at)
            < Duration::minutes(cadence_minutes.max(1) as i64)
        {
            let freshness =
                assess_runtime_artifact_freshness(&latest_artifact, freshness_gate, config.now);
            return Ok(render_output(
                "skipped_not_due",
                &config.config_path,
                db_path,
                &latest_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                &latest_artifact,
                freshness.fresh_under_export_gate,
                freshness.fresh_under_current_gate,
            ));
        }
    }

    let artifact =
        export_runtime_artifact(store, freshness_gate, config.now).context("artifact export")?;
    let archive_path = artifact_archive_path(&artifact_dir, config.now);
    write_json_atomic(&archive_path, &artifact)
        .with_context(|| format!("failed writing {}", archive_path.display()))?;
    copy_atomic(&archive_path, &latest_path)
        .with_context(|| format!("failed updating {}", latest_path.display()))?;
    let pruned = prune_rotated_archives(
        &artifact_dir,
        ARTIFACT_ARCHIVE_PREFIX,
        ARTIFACT_ARCHIVE_SUFFIX,
        retention,
    )?;
    let freshness = assess_runtime_artifact_freshness(&artifact, freshness_gate, config.now);
    Ok(render_output(
        "written",
        &config.config_path,
        db_path,
        &latest_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        &pruned,
        &artifact,
        freshness.fresh_under_export_gate,
        freshness.fresh_under_current_gate,
    ))
}

fn export_runtime_artifact(
    store: &SqliteStore,
    freshness_gate: DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    store.export_discovery_runtime_artifact(now, freshness_gate)
}
