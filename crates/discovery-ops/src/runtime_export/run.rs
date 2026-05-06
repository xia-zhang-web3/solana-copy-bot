use super::freshness::{assess_runtime_artifact_freshness, publication_freshness_gate};
use super::render::{render_human, render_output};
use super::types::{Command, ExportConfig, ExportOutput};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_runtime_artifacts::{
    artifact_archive_path, artifact_latest_path, copy_atomic, load_json, prune_rotated_archives,
    resolve_db_path, resolve_relative_to_config, write_json_atomic, ARTIFACT_ARCHIVE_PREFIX,
    ARTIFACT_ARCHIVE_SUFFIX,
};
use copybot_storage_core::{
    validate_discovery_runtime_artifact_export_readiness, validate_discovery_v2_schema_read_only,
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow, DiscoveryRuntimeArtifact,
    PersistedWalletMetricSnapshotRow, SqliteStore,
};
use std::collections::HashSet;
use std::path::Path;

pub(super) fn run_command(command: Command) -> Result<String> {
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
    let store = SqliteStore::open_read_only(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    validate_discovery_v2_schema_read_only(&store).with_context(|| {
        format!(
            "sqlite db is not ready for discovery v2 export: {}",
            db_path.display()
        )
    })?;
    let freshness_gate = publication_freshness_gate(
        &loaded_config.discovery,
        &loaded_config.shadow,
        loaded_config.execution.enabled,
        config.now,
    );

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &loaded_config.runtime_restore_ops.artifact_dir,
            loaded_config.runtime_restore_ops.artifact_cadence_minutes,
            loaded_config.runtime_restore_ops.artifact_retention,
            &db_path,
            &store,
            freshness_gate.clone(),
        )?
    } else {
        let output_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let artifact = export_runtime_artifact(&store, freshness_gate.clone(), config.now)
            .context("artifact export")?;
        let freshness = assess_runtime_artifact_freshness(&artifact, &freshness_gate, config.now);
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
                assess_runtime_artifact_freshness(&latest_artifact, &freshness_gate, config.now);
            if validate_discovery_runtime_artifact_export_readiness(
                &latest_artifact,
                &freshness_gate,
                config.now,
            )
            .is_ok()
                && freshness.fresh_under_current_gate
                && artifact_matches_current_publication_truth(store, &latest_artifact)?
            {
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
    }

    let artifact = export_runtime_artifact(store, freshness_gate.clone(), config.now)
        .context("artifact export")?;
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
    let freshness = assess_runtime_artifact_freshness(&artifact, &freshness_gate, config.now);
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

fn artifact_matches_current_publication_truth(
    store: &SqliteStore,
    artifact: &DiscoveryRuntimeArtifact,
) -> Result<bool> {
    let Some(current_state) = store.discovery_publication_state_read_only()? else {
        return Ok(false);
    };
    let Some(current_cursor) = store.load_discovery_runtime_cursor()? else {
        return Ok(false);
    };
    if !current_state.matches_publication_runtime_cursor(&current_cursor) {
        return Ok(false);
    }
    Ok(artifact.runtime_cursor == current_cursor
        && publication_states_match(&artifact.publication_state, &current_state)
        && current_publication_metric_snapshot_matches(store, artifact, &current_state)?)
}

fn publication_states_match(
    artifact: &DiscoveryPublicationStateRow,
    current: &DiscoveryPublicationStateRow,
) -> bool {
    artifact.runtime_mode == current.runtime_mode
        && artifact.reason == current.reason
        && artifact.last_published_at == current.last_published_at
        && artifact.last_published_window_start == current.last_published_window_start
        && artifact.published_scoring_source == current.published_scoring_source
        && artifact.published_wallet_ids == current.published_wallet_ids
        && artifact.publication_policy_fingerprint == current.publication_policy_fingerprint
        && artifact.publication_runtime_cursor == current.publication_runtime_cursor
        && artifact.updated_at == current.updated_at
}

fn current_publication_metric_snapshot_matches(
    store: &SqliteStore,
    artifact: &DiscoveryRuntimeArtifact,
    current_state: &DiscoveryPublicationStateRow,
) -> Result<bool> {
    let Some(window_start) = current_state.last_published_window_start else {
        return Ok(false);
    };
    let Some(published_wallet_ids) = current_state.published_wallet_ids.as_ref() else {
        return Ok(false);
    };
    let published_wallet_ids = published_wallet_ids.iter().cloned().collect::<HashSet<_>>();
    let mut current_rows = store
        .load_wallet_metric_snapshots_for_window(window_start)?
        .into_iter()
        .filter(|row| published_wallet_ids.contains(&row.wallet_id))
        .collect::<Vec<_>>();
    let mut artifact_rows = artifact.published_wallet_metrics_snapshot.clone();
    sort_metric_snapshot_rows(&mut current_rows);
    sort_metric_snapshot_rows(&mut artifact_rows);
    Ok(current_rows == artifact_rows)
}

fn sort_metric_snapshot_rows(rows: &mut [PersistedWalletMetricSnapshotRow]) {
    rows.sort_by(|left, right| {
        left.wallet_id
            .cmp(&right.wallet_id)
            .then_with(|| left.window_start.cmp(&right.window_start))
            .then_with(|| left.score.total_cmp(&right.score))
    });
}

fn export_runtime_artifact(
    store: &SqliteStore,
    freshness_gate: DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    store.export_discovery_runtime_artifact(now, freshness_gate)
}
