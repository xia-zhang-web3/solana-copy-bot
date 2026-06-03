use super::types::{WatchdogConfig, WatchdogFinding, WatchdogOutput, WatchdogState};
use anyhow::{Context, Result};
use copybot_config::load_from_path;
use copybot_discovery_v2::{
    discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_runtime_artifacts::resolve_db_path;
use copybot_storage_core::{
    validate_discovery_v2_status_schema_read_only, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryRuntimeMode, SqliteStore,
};
use std::path::Path;

pub fn run_watchdog(config: WatchdogConfig) -> Result<WatchdogOutput> {
    let loaded = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded.sqlite.path,
    );
    let store = SqliteStore::open_read_only(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    validate_discovery_v2_status_schema_read_only(&store).with_context(|| {
        format!(
            "sqlite db is not discovery v2 schema-ready: {}",
            db_path.display()
        )
    })?;
    let gate = freshness_gate(
        &loaded.discovery,
        &loaded.shadow,
        loaded.execution.enabled,
        config.now,
    );
    let min_active_wallets = config
        .min_active_wallets
        .unwrap_or_else(|| loaded.discovery.effective_publish_min_candidate_wallets());
    let publication = store.discovery_publication_state_read_only()?;
    let active_follow_wallet_count = store.active_follow_wallet_row_count()?;

    Ok(assess_publication(
        &config,
        &db_path,
        &gate,
        min_active_wallets,
        publication.as_ref(),
        active_follow_wallet_count,
    ))
}

fn freshness_gate(
    discovery: &copybot_config::DiscoveryConfig,
    shadow: &copybot_config::ShadowConfig,
    execution_enabled: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> DiscoveryPublicationFreshnessGate {
    let options = DiscoveryV2BuildOptions::from_config(discovery, execution_enabled, now);
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: discovery.scoring_window_days as i64,
        window_minutes: Some(options.window_minutes),
        metric_snapshot_interval_seconds: discovery.metric_snapshot_interval_seconds,
        refresh_seconds: discovery.refresh_seconds,
        expected_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
        expected_policy_fingerprint: Some(discovery_v2_policy_fingerprint(
            discovery, shadow, &options,
        )),
    }
}

fn assess_publication(
    config: &WatchdogConfig,
    db_path: &Path,
    gate: &DiscoveryPublicationFreshnessGate,
    min_active_wallets: usize,
    publication: Option<&DiscoveryPublicationStateRow>,
    active_follow_wallet_count: usize,
) -> WatchdogOutput {
    let max_age_seconds = gate.published_universe_max_age().num_seconds();
    let warn_age_seconds = max_age_seconds;
    let mut findings = Vec::new();
    let mut output = base_output(
        config,
        db_path,
        warn_age_seconds,
        max_age_seconds,
        min_active_wallets,
        active_follow_wallet_count,
        publication,
    );

    let Some(publication) = publication else {
        push_finding(
            &mut findings,
            WatchdogState::Critical,
            "discovery_v2_publication_missing",
            "discovery_strategy_state has no publication row",
        );
        output.findings = findings;
        output.state = WatchdogState::Critical;
        return output;
    };

    assess_truth(&mut findings, &mut output, publication, gate);
    assess_followlist(
        &mut findings,
        active_follow_wallet_count,
        min_active_wallets,
        publication,
    );
    output.state = fold_state(&findings);
    output.findings = findings;
    output
}

fn base_output(
    config: &WatchdogConfig,
    db_path: &Path,
    warn_age_seconds: i64,
    max_age_seconds: i64,
    min_active_wallets: usize,
    active_follow_wallet_count: usize,
    publication: Option<&DiscoveryPublicationStateRow>,
) -> WatchdogOutput {
    let published_wallet_count = publication
        .and_then(|row| row.published_wallet_ids.as_ref())
        .map_or(0, Vec::len);
    let publication_age_seconds = publication
        .and_then(|row| row.last_published_at)
        .map(|published_at| config.now.signed_duration_since(published_at).num_seconds());
    WatchdogOutput {
        event: "discovery_v2_watchdog".to_string(),
        state: WatchdogState::Ok,
        config_path: config.config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        checked_at: config.now,
        publication_runtime_mode: publication.map(|row| row.runtime_mode.as_str().to_string()),
        publication_reason: publication.map(|row| row.reason.clone()),
        publication_last_published_at: publication.and_then(|row| row.last_published_at),
        publication_age_seconds,
        publication_warn_age_seconds: warn_age_seconds,
        publication_max_age_seconds: max_age_seconds,
        publication_fresh: false,
        publication_identity_matches: false,
        publication_cursor_fresh: false,
        published_wallet_count,
        active_follow_wallet_count,
        min_active_wallets,
        findings: Vec::new(),
    }
}

fn assess_truth(
    findings: &mut Vec<WatchdogFinding>,
    output: &mut WatchdogOutput,
    publication: &DiscoveryPublicationStateRow,
    gate: &DiscoveryPublicationFreshnessGate,
) {
    output.publication_fresh =
        publication_truth_is_recent_for_runtime(publication, gate, output.checked_at);
    output.publication_identity_matches = publication.matches_expected_publication_identity(gate)
        && publication.published_scoring_source.is_some()
        && publication.publication_policy_fingerprint.is_some()
        && publication.publication_runtime_cursor.is_some();
    output.publication_cursor_fresh =
        publication_runtime_cursor_is_fresh_for_publication(publication, gate, output.checked_at);

    if publication.runtime_mode != DiscoveryRuntimeMode::Healthy {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_not_healthy",
            "publication runtime mode is not healthy",
        );
    }
    if !publication.has_complete_publication_truth() {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_truth_incomplete",
            "publication truth is missing timestamp, window, or wallet ids",
        );
    }
    if !output.publication_identity_matches {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_identity_mismatch",
            "publication source, policy fingerprint, or bound cursor is missing or mismatched",
        );
    }
    if !output.publication_cursor_fresh {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_cursor_stale",
            "publication-bound runtime cursor is missing, future-dated, or stale",
        );
    }
    let age_fault = assess_publication_age(findings, output);
    if !output.publication_fresh && !age_fault && publication.has_complete_publication_truth() {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_window_invalid",
            "publication window is not valid for the recorded publication time",
        );
    }
}

fn publication_truth_is_recent_for_runtime(
    publication: &DiscoveryPublicationStateRow,
    gate: &DiscoveryPublicationFreshnessGate,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(last_published_at) = publication.last_published_at else {
        return false;
    };
    let published_age = now.signed_duration_since(last_published_at);
    publication.has_complete_publication_truth()
        && published_age >= chrono::Duration::zero()
        && published_age <= gate.published_universe_max_age()
        && publication.has_valid_published_window_under_gate(gate, last_published_at)
}

fn publication_runtime_cursor_is_fresh_for_publication(
    publication: &DiscoveryPublicationStateRow,
    gate: &DiscoveryPublicationFreshnessGate,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    publication
        .publication_runtime_cursor
        .as_ref()
        .is_some_and(|cursor| {
            let cursor_age = now.signed_duration_since(cursor.ts_utc);
            cursor_age >= chrono::Duration::zero()
                && cursor_age <= gate.published_universe_max_age()
        })
}

fn assess_publication_age(findings: &mut Vec<WatchdogFinding>, output: &WatchdogOutput) -> bool {
    let Some(age_seconds) = output.publication_age_seconds else {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_timestamp_missing",
            "publication timestamp is missing",
        );
        return true;
    };
    if age_seconds < 0 {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_future_dated",
            "publication timestamp is in the future",
        );
        true
    } else if age_seconds > output.publication_max_age_seconds {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_publication_stale",
            "publication is older than the fail-closed freshness gate",
        );
        true
    } else if age_seconds > output.publication_warn_age_seconds {
        push_finding(
            findings,
            WatchdogState::Warn,
            "discovery_v2_publication_one_cycle_late",
            "publication is past the warning age; one scheduled publish may have been missed",
        );
        false
    } else {
        false
    }
}

fn assess_followlist(
    findings: &mut Vec<WatchdogFinding>,
    active_follow_wallet_count: usize,
    min_active_wallets: usize,
    publication: &DiscoveryPublicationStateRow,
) {
    if active_follow_wallet_count == 0 {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_active_followlist_empty",
            "active followlist is empty",
        );
    } else if active_follow_wallet_count < min_active_wallets {
        push_finding(
            findings,
            WatchdogState::Critical,
            "discovery_v2_active_followlist_below_floor",
            "active followlist is below the configured floor",
        );
    }
    let published_count = publication
        .published_wallet_ids
        .as_ref()
        .map_or(0, Vec::len);
    if published_count > 0 && active_follow_wallet_count != published_count {
        push_finding(
            findings,
            WatchdogState::Warn,
            "discovery_v2_active_followlist_count_mismatch",
            "active followlist count differs from published wallet count",
        );
    }
}

fn push_finding(
    findings: &mut Vec<WatchdogFinding>,
    severity: WatchdogState,
    code: &str,
    detail: &str,
) {
    findings.push(WatchdogFinding {
        severity,
        code: code.to_string(),
        detail: detail.to_string(),
    });
}

fn fold_state(findings: &[WatchdogFinding]) -> WatchdogState {
    if findings
        .iter()
        .any(|finding| finding.severity == WatchdogState::Critical)
    {
        WatchdogState::Critical
    } else if findings
        .iter()
        .any(|finding| finding.severity == WatchdogState::Warn)
    {
        WatchdogState::Warn
    } else {
        WatchdogState::Ok
    }
}
