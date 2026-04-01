use super::{
    DiscoveryBootstrapDegradedStateRow, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate,
    DiscoveryRecentRawRestoreStateRow, DiscoveryRecentRawRestoreStateUpdate,
    DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor, DiscoveryRuntimeMode,
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate,
    DiscoveryWalletFreshnessCaptureRow, DiscoveryWalletFreshnessCaptureWrite,
    FollowlistUpdateResult, PersistedWalletMetricSnapshotRow, SqliteStore,
    StartupTrustedSelectionGateStatus, TrustedSelectionState, TrustedSnapshotSourceKind,
    TrustedWalletMetricsSnapshotRow, TrustedWalletMetricsSnapshotWrite, WalletActivityDayRow,
    WalletMetricRow, WalletRecentActivityCountRow, WalletUpsertRow,
    DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION, DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};

pub(crate) fn canonical_wallet_metrics_window_start(window_start: DateTime<Utc>) -> String {
    window_start.to_rfc3339()
}

fn wallet_metrics_window_start_query_variants(window_start: DateTime<Utc>) -> (String, String) {
    let canonical = canonical_wallet_metrics_window_start(window_start);
    let legacy_z = canonical
        .strip_suffix("+00:00")
        .map(|prefix| format!("{prefix}Z"))
        .unwrap_or_else(|| canonical.clone());
    (canonical, legacy_z)
}

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
}

fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

fn canonicalize_wallet_ids(wallet_ids: &[String]) -> Vec<String> {
    let mut canonical = wallet_ids.to_vec();
    canonical.sort();
    canonical.dedup();
    canonical
}

fn parse_optional_wallet_ids_json(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<Vec<String>>> {
    raw.map(|raw| {
        let wallet_ids = serde_json::from_str::<Vec<String>>(&raw)
            .with_context(|| format!("invalid {field_name} JSON payload: {raw}"))?;
        Ok(canonicalize_wallet_ids(&wallet_ids))
    })
    .transpose()
}

fn parse_wallet_ids_json(raw: String, field_name: &str) -> Result<Vec<String>> {
    serde_json::from_str::<Vec<String>>(&raw)
        .with_context(|| format!("invalid {field_name} JSON payload: {raw}"))
}

fn read_discovery_wallet_freshness_capture_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DiscoveryWalletFreshnessCaptureRow> {
    let capture_id: i64 = row.get(0)?;
    let captured_at_raw: String = row.get(1)?;
    let recent_cycles_raw: i64 = row.get(2)?;
    let verdict: String = row.get(3)?;
    let reason: String = row.get(4)?;
    let publication_age_seconds_raw: Option<i64> = row.get(5)?;
    let raw_truth_sufficient: i64 = row.get(6)?;
    let raw_truth_reason: String = row.get(7)?;
    let shadow_signal_verdict: String = row.get(8)?;
    let shadow_signal_reason: String = row.get(9)?;
    let published_wallet_ids_json: String = row.get(10)?;
    let active_follow_wallet_ids_json: String = row.get(11)?;
    let current_raw_top_wallet_ids_json: String = row.get(12)?;
    let audit_json: String = row.get(13)?;
    let shadow_signal_json: String = row.get(14)?;
    let captured_at = parse_rfc3339_utc(
        &captured_at_raw,
        "discovery_wallet_freshness_history.captured_at",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    let published_wallet_ids =
        parse_wallet_ids_json(published_wallet_ids_json, "published_wallet_ids_json").map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Text,
                    Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
                )
            },
        )?;
    let active_follow_wallet_ids = parse_wallet_ids_json(
        active_follow_wallet_ids_json,
        "active_follow_wallet_ids_json",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    let current_raw_top_wallet_ids = parse_wallet_ids_json(
        current_raw_top_wallet_ids_json,
        "current_raw_top_wallet_ids_json",
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(IoErrorKind::InvalidData, error.to_string())),
        )
    })?;
    Ok(DiscoveryWalletFreshnessCaptureRow {
        capture_id,
        captured_at,
        recent_cycles: recent_cycles_raw.max(1) as usize,
        verdict,
        reason,
        publication_age_seconds: publication_age_seconds_raw.map(|value| value.max(0) as u64),
        raw_truth_sufficient: raw_truth_sufficient != 0,
        raw_truth_reason,
        shadow_signal_verdict,
        shadow_signal_reason,
        published_wallet_ids,
        active_follow_wallet_ids,
        current_raw_top_wallet_ids,
        audit_json,
        shadow_signal_json,
    })
}

fn validate_runtime_artifact_snapshot_shape(artifact: &DiscoveryRuntimeArtifact) -> Result<()> {
    if artifact.format_version != DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION {
        return Err(anyhow::anyhow!(
            "unsupported discovery runtime artifact format_version={} expected={}",
            artifact.format_version,
            DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION
        ));
    }
    if !artifact.publication_state.has_complete_publication_truth() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication truth is incomplete"
        ));
    }
    let published_window_start = artifact
        .publication_state
        .last_published_window_start
        .expect("validated complete publication truth above");
    if artifact.published_wallet_metrics_snapshot.is_empty() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact is missing published wallet_metrics snapshot rows"
        ));
    }
    if artifact
        .published_wallet_metrics_snapshot
        .iter()
        .any(|row| row.window_start != published_window_start)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot rows do not match publication window_start={}",
            published_window_start.to_rfc3339()
        ));
    }
    let snapshot_wallet_ids: HashSet<String> = artifact
        .published_wallet_metrics_snapshot
        .iter()
        .map(|row| row.wallet_id.clone())
        .collect();
    let published_wallet_ids = artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth above");
    if published_wallet_ids
        .iter()
        .any(|wallet_id| !snapshot_wallet_ids.contains(wallet_id))
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication wallet ids are not fully covered by the published wallet_metrics snapshot"
        ));
    }
    Ok(())
}

fn format_optional_export_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn incomplete_publication_truth_fields(
    publication_state: &DiscoveryPublicationStateRow,
) -> Vec<&'static str> {
    let mut missing_fields = Vec::new();
    if publication_state.last_published_at.is_none() {
        missing_fields.push("last_published_at");
    }
    if publication_state.last_published_window_start.is_none() {
        missing_fields.push("last_published_window_start");
    }
    if !publication_state
        .published_wallet_ids
        .as_ref()
        .is_some_and(|wallet_ids| !wallet_ids.is_empty())
    {
        missing_fields.push("published_wallet_ids");
    }
    missing_fields
}

fn runtime_artifact_export_truth_detail(
    publication_state: &DiscoveryPublicationStateRow,
    gate: DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> String {
    let missing_fields = incomplete_publication_truth_fields(publication_state);
    format!(
        "runtime_mode={} reason={} complete={} fresh_under_export_gate={} last_published_at={} last_published_window_start={} published_wallet_count={} updated_at={} gate_scoring_window_days={} gate_metric_snapshot_interval_seconds={} gate_refresh_seconds={} missing_fields={}",
        publication_state.runtime_mode.as_str(),
        publication_state.reason,
        publication_state.has_complete_publication_truth(),
        publication_state.is_fresh_under_gate(gate, now),
        format_optional_export_ts(publication_state.last_published_at),
        format_optional_export_ts(publication_state.last_published_window_start),
        publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        publication_state.updated_at.to_rfc3339(),
        gate.scoring_window_days,
        gate.metric_snapshot_interval_seconds,
        gate.refresh_seconds,
        if missing_fields.is_empty() {
            "none".to_string()
        } else {
            missing_fields.join(",")
        }
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeArtifactRestoreDirtyTable {
    pub table: String,
    pub category: &'static str,
}

fn runtime_artifact_restore_table_category(table: &str) -> &'static str {
    match table {
        "risk_events" => "risk gating",
        "wallets"
        | "wallet_metrics"
        | "followlist"
        | "observed_swaps"
        | "discovery_strategy_state"
        | "discovery_runtime_state"
        | "discovery_recent_raw_restore_state"
        | "discovery_persisted_rebuild_state"
        | "trusted_wallet_metrics_snapshots"
        | "wallet_activity_days" => "discovery runtime",
        "positions" | "trades" | "execution_orders" | "copy_signals" => "execution runtime",
        "system_heartbeat" | "alert_delivery_state" => "runtime sidecar",
        _ if table.starts_with("shadow_") => "shadow accounting",
        _ => "durable runtime state",
    }
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn discovery_runtime_restore_dirty_tables_on_conn(
    conn: &Connection,
) -> Result<Vec<RuntimeArtifactRestoreDirtyTable>> {
    let mut stmt = conn
        .prepare(
            "SELECT name
             FROM sqlite_master
             WHERE type = 'table'
               AND name NOT LIKE 'sqlite_%'
               AND name <> 'schema_migrations'
             ORDER BY name ASC",
        )
        .context("failed to prepare runtime artifact restore table inventory query")?;
    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("failed to query runtime artifact restore table inventory")?;

    let mut dirty_tables = Vec::new();
    for table_name in table_names {
        let table_name = table_name
            .context("failed reading sqlite_master.name during runtime restore preflight")?;
        let has_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
                    quote_sql_identifier(&table_name)
                ),
                [],
                |row| row.get(0),
            )
            .with_context(|| {
                format!(
                    "failed checking table {table_name} during runtime artifact restore preflight"
                )
            })?;
        if has_rows != 0 {
            dirty_tables.push(RuntimeArtifactRestoreDirtyTable {
                table: table_name.clone(),
                category: runtime_artifact_restore_table_category(&table_name),
            });
        }
    }

    Ok(dirty_tables)
}

fn format_runtime_artifact_restore_dirty_tables(
    dirty_tables: &[RuntimeArtifactRestoreDirtyTable],
) -> String {
    dirty_tables
        .iter()
        .map(|entry| format!("{} ({})", entry.table, entry.category))
        .collect::<Vec<_>>()
        .join(", ")
}

fn discovery_bootstrap_degraded_state_query(
    conn: &Connection,
) -> Result<DiscoveryBootstrapDegradedStateRow> {
    let row = conn
        .query_row(
            "SELECT
                bootstrap_degraded_active,
                bootstrap_degraded_reason,
                bootstrap_degraded_armed_at
             FROM discovery_strategy_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .context("failed reading discovery bootstrap-degraded state")?;
    let Some((active, reason, armed_at_raw)) = row else {
        return Ok(DiscoveryBootstrapDegradedStateRow::default());
    };
    Ok(DiscoveryBootstrapDegradedStateRow {
        active: active != 0,
        reason,
        armed_at: parse_optional_rfc3339_utc(
            armed_at_raw,
            "discovery_strategy_state.bootstrap_degraded_armed_at",
        )?,
    })
}

fn parse_optional_runtime_cursor(
    ts_raw: Option<String>,
    slot_raw: Option<i64>,
    signature: Option<String>,
    field_prefix: &str,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    match (ts_raw, slot_raw, signature) {
        (None, None, None) => Ok(None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Ok(Some(DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(&ts_raw, &format!("{field_prefix}_ts"))?,
            slot: slot_raw.max(0) as u64,
            signature,
        })),
        _ => Err(anyhow::anyhow!(
            "incomplete discovery runtime cursor payload for {field_prefix}"
        )),
    }
}

fn discovery_recent_raw_restore_state_query(
    conn: &Connection,
) -> Result<DiscoveryRecentRawRestoreStateRow> {
    let row = conn
        .query_row(
            "SELECT
                journal_available,
                journal_replayed,
                required_window_start,
                journal_covered_since,
                journal_covered_through_cursor_ts,
                journal_covered_through_cursor_slot,
                journal_covered_through_cursor_signature,
                gap_fill_replayed,
                gap_fill_covered_since,
                gap_fill_covered_through_cursor_ts,
                gap_fill_covered_through_cursor_slot,
                gap_fill_covered_through_cursor_signature,
                effective_covered_since,
                effective_covered_through_cursor_ts,
                effective_covered_through_cursor_slot,
                effective_covered_through_cursor_signature,
                artifact_runtime_cursor_ts,
                artifact_runtime_cursor_slot,
                artifact_runtime_cursor_signature,
                journal_covers_artifact_cursor,
                raw_coverage_satisfied,
                gap_fill_replayed_rows,
                replayed_rows,
                reason,
                replay_started_at,
                replay_completed_at,
                updated_at
             FROM discovery_recent_raw_restore_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<i64>>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<i64>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, i64>(19)?,
                    row.get::<_, i64>(20)?,
                    row.get::<_, i64>(21)?,
                    row.get::<_, i64>(22)?,
                    row.get::<_, Option<String>>(23)?,
                    row.get::<_, Option<String>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<String>>(26)?,
                ))
            },
        )
        .optional()
        .context("failed reading discovery recent raw restore state")?;
    let Some((
        journal_available,
        journal_replayed,
        required_window_start_raw,
        journal_covered_since_raw,
        journal_covered_through_cursor_ts_raw,
        journal_covered_through_cursor_slot_raw,
        journal_covered_through_cursor_signature,
        gap_fill_replayed,
        gap_fill_covered_since_raw,
        gap_fill_covered_through_cursor_ts_raw,
        gap_fill_covered_through_cursor_slot_raw,
        gap_fill_covered_through_cursor_signature,
        effective_covered_since_raw,
        effective_covered_through_cursor_ts_raw,
        effective_covered_through_cursor_slot_raw,
        effective_covered_through_cursor_signature,
        artifact_runtime_cursor_ts_raw,
        artifact_runtime_cursor_slot_raw,
        artifact_runtime_cursor_signature,
        journal_covers_artifact_cursor,
        raw_coverage_satisfied,
        gap_fill_replayed_rows,
        replayed_rows,
        reason,
        replay_started_at_raw,
        replay_completed_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(DiscoveryRecentRawRestoreStateRow::default());
    };

    Ok(DiscoveryRecentRawRestoreStateRow {
        journal_available: journal_available != 0,
        journal_replayed: journal_replayed != 0,
        required_window_start: parse_optional_rfc3339_utc(
            required_window_start_raw,
            "discovery_recent_raw_restore_state.required_window_start",
        )?,
        journal_covered_since: parse_optional_rfc3339_utc(
            journal_covered_since_raw,
            "discovery_recent_raw_restore_state.journal_covered_since",
        )?,
        journal_covered_through_cursor: parse_optional_runtime_cursor(
            journal_covered_through_cursor_ts_raw,
            journal_covered_through_cursor_slot_raw,
            journal_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.journal_covered_through_cursor",
        )?,
        gap_fill_replayed: gap_fill_replayed != 0,
        gap_fill_covered_since: parse_optional_rfc3339_utc(
            gap_fill_covered_since_raw,
            "discovery_recent_raw_restore_state.gap_fill_covered_since",
        )?,
        gap_fill_covered_through_cursor: parse_optional_runtime_cursor(
            gap_fill_covered_through_cursor_ts_raw,
            gap_fill_covered_through_cursor_slot_raw,
            gap_fill_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.gap_fill_covered_through_cursor",
        )?,
        effective_covered_since: parse_optional_rfc3339_utc(
            effective_covered_since_raw,
            "discovery_recent_raw_restore_state.effective_covered_since",
        )?,
        effective_covered_through_cursor: parse_optional_runtime_cursor(
            effective_covered_through_cursor_ts_raw,
            effective_covered_through_cursor_slot_raw,
            effective_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.effective_covered_through_cursor",
        )?,
        artifact_runtime_cursor: parse_optional_runtime_cursor(
            artifact_runtime_cursor_ts_raw,
            artifact_runtime_cursor_slot_raw,
            artifact_runtime_cursor_signature,
            "discovery_recent_raw_restore_state.artifact_runtime_cursor",
        )?,
        journal_covers_artifact_cursor: journal_covers_artifact_cursor != 0,
        raw_coverage_satisfied: raw_coverage_satisfied != 0,
        gap_fill_replayed_rows: gap_fill_replayed_rows.max(0) as usize,
        replayed_rows: replayed_rows.max(0) as usize,
        reason,
        replay_started_at: parse_optional_rfc3339_utc(
            replay_started_at_raw,
            "discovery_recent_raw_restore_state.replay_started_at",
        )?,
        replay_completed_at: parse_optional_rfc3339_utc(
            replay_completed_at_raw,
            "discovery_recent_raw_restore_state.replay_completed_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "discovery_recent_raw_restore_state.updated_at",
        )?,
    })
}

fn insert_trusted_wallet_metrics_snapshot_on_conn(
    conn: &Connection,
    snapshot_write: &TrustedWalletMetricsSnapshotWrite,
) -> Result<()> {
    conn.execute(
        "INSERT INTO trusted_wallet_metrics_snapshots(
            snapshot_id,
            source_snapshot_id,
            source_window_start,
            effective_window_start,
            created_at,
            source_kind,
            row_count,
            trust_state
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            &snapshot_write.snapshot_id,
            &snapshot_write.source_snapshot_id,
            snapshot_write
                .source_window_start
                .map(canonical_wallet_metrics_window_start),
            canonical_wallet_metrics_window_start(snapshot_write.effective_window_start),
            snapshot_write.created_at.to_rfc3339(),
            snapshot_write.source_kind.as_str(),
            snapshot_write.row_count as i64,
            snapshot_write.trust_state.as_str(),
        ],
    )
    .context("failed inserting trusted wallet_metrics snapshot metadata")?;
    Ok(())
}

pub(crate) fn upsert_wallet_activity_days_on_conn(
    conn: &Connection,
    rows: &[WalletActivityDayRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                last_seen = CASE
                    WHEN excluded.last_seen > wallet_activity_days.last_seen
                        THEN excluded.last_seen
                    ELSE wallet_activity_days.last_seen
                END",
        )
        .context("failed to prepare wallet_activity_days upsert statement")?;
    for row in rows {
        stmt.execute(params![
            &row.wallet_id,
            row.activity_day.format("%Y-%m-%d").to_string(),
            row.last_seen.to_rfc3339(),
        ])
        .context("failed to upsert wallet_activity_days row")?;
    }
    Ok(())
}

impl SqliteStore {
    fn startup_trusted_selection_gate_status_from_metadata(
        &self,
        bootstrap_required: bool,
        reason: Option<String>,
    ) -> Result<Option<StartupTrustedSelectionGateStatus>> {
        let Some(metadata) = self.latest_trusted_wallet_metrics_snapshot_metadata()? else {
            return Ok(None);
        };
        Ok(Some(StartupTrustedSelectionGateStatus {
            bootstrap_required,
            selection_state: Some(metadata.trust_state),
            startup_fail_closed: bootstrap_required
                || matches!(
                    metadata.trust_state,
                    TrustedSelectionState::Invalid | TrustedSelectionState::TrustedBridgedStale
                ),
            reason,
            active_snapshot_id: Some(metadata.snapshot_id),
            active_snapshot_window_start: Some(metadata.effective_window_start),
            last_bootstrap_source_kind: Some(metadata.source_kind),
            source_snapshot_window_start: metadata.source_window_start,
            legacy_bool_fallback_used: false,
        }))
    }

    pub fn discovery_trusted_selection_bootstrap_required(&self) -> Result<bool> {
        self.ensure_discovery_strategy_state_table()?;
        let required = self
            .conn
            .query_row(
                "SELECT trusted_selection_bootstrap_required
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed reading discovery trusted selection bootstrap requirement")?
            .unwrap_or(0);
        Ok(required != 0)
    }

    pub fn set_discovery_trusted_selection_bootstrap_required(
        &self,
        required: bool,
        reason: &str,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    trusted_selection_bootstrap_required =
                        excluded.trusted_selection_bootstrap_required,
                    trusted_selection_reason = excluded.trusted_selection_reason,
                    updated_at = excluded.updated_at",
                params![
                    if required { 1 } else { 0 },
                    reason,
                    Utc::now().to_rfc3339()
                ],
            )
        })
        .context("failed updating discovery trusted selection bootstrap requirement")?;
        Ok(())
    }

    pub fn discovery_publication_state(&self) -> Result<Option<DiscoveryPublicationStateRow>> {
        self.ensure_discovery_strategy_state_table()?;
        self.discovery_publication_state_query()
    }

    pub fn discovery_publication_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPublicationStateRow>> {
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(None);
        }
        self.discovery_publication_state_query()
    }

    fn discovery_publication_state_query(&self) -> Result<Option<DiscoveryPublicationStateRow>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    publication_wallet_ids_json,
                    updated_at
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery publication state")?;
        raw.map(
            |(
                runtime_mode_raw,
                reason,
                last_published_at_raw,
                last_published_window_start_raw,
                published_scoring_source,
                published_wallet_ids_raw,
                updated_at_raw,
            )| {
                Ok(DiscoveryPublicationStateRow {
                    runtime_mode: DiscoveryRuntimeMode::parse(&runtime_mode_raw)?,
                    reason,
                    last_published_at: parse_optional_rfc3339_utc(
                        last_published_at_raw,
                        "discovery_strategy_state.publication_last_published_at",
                    )?,
                    last_published_window_start: parse_optional_rfc3339_utc(
                        last_published_window_start_raw,
                        "discovery_strategy_state.publication_last_published_window_start",
                    )?,
                    published_scoring_source,
                    published_wallet_ids: parse_optional_wallet_ids_json(
                        published_wallet_ids_raw,
                        "discovery_strategy_state.publication_wallet_ids_json",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_strategy_state.updated_at",
                    )?,
                })
            },
        )
        .transpose()
    }

    pub fn set_discovery_publication_state(
        &self,
        update: &DiscoveryPublicationStateUpdate,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        let published_wallet_ids_json = update
            .published_wallet_ids
            .as_deref()
            .map(canonicalize_wallet_ids)
            .map(|wallet_ids| {
                serde_json::to_string(&wallet_ids)
                    .context("failed serializing discovery published wallet ids")
            })
            .transpose()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    publication_wallet_ids_json,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(id) DO UPDATE SET
                    publication_runtime_mode = excluded.publication_runtime_mode,
                    publication_reason = excluded.publication_reason,
                    publication_last_published_at = COALESCE(
                        excluded.publication_last_published_at,
                        discovery_strategy_state.publication_last_published_at
                    ),
                    publication_last_published_window_start = COALESCE(
                        excluded.publication_last_published_window_start,
                        discovery_strategy_state.publication_last_published_window_start
                    ),
                    publication_scoring_source = excluded.publication_scoring_source,
                    publication_wallet_ids_json = COALESCE(
                        excluded.publication_wallet_ids_json,
                        discovery_strategy_state.publication_wallet_ids_json
                    ),
                    updated_at = excluded.updated_at",
                params![
                    update.runtime_mode.as_str(),
                    &update.reason,
                    update.last_published_at.map(|ts| ts.to_rfc3339()),
                    update
                        .last_published_window_start
                        .map(canonical_wallet_metrics_window_start),
                    update.published_scoring_source.as_deref(),
                    published_wallet_ids_json.as_deref(),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery publication state")?;
        Ok(())
    }

    pub fn discovery_bootstrap_degraded_state(&self) -> Result<DiscoveryBootstrapDegradedStateRow> {
        self.ensure_discovery_strategy_state_table()?;
        discovery_bootstrap_degraded_state_query(&self.conn)
    }

    pub fn discovery_bootstrap_degraded_state_read_only(
        &self,
    ) -> Result<DiscoveryBootstrapDegradedStateRow> {
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(DiscoveryBootstrapDegradedStateRow::default());
        }
        discovery_bootstrap_degraded_state_query(&self.conn)
    }

    pub fn discovery_recent_raw_restore_state(&self) -> Result<DiscoveryRecentRawRestoreStateRow> {
        self.ensure_discovery_recent_raw_restore_state_table()?;
        discovery_recent_raw_restore_state_query(&self.conn)
    }

    pub fn discovery_recent_raw_restore_state_read_only(
        &self,
    ) -> Result<DiscoveryRecentRawRestoreStateRow> {
        if !self.sqlite_table_exists("discovery_recent_raw_restore_state")? {
            return Ok(DiscoveryRecentRawRestoreStateRow::default());
        }
        discovery_recent_raw_restore_state_query(&self.conn)
    }

    pub(crate) fn discovery_runtime_restore_dirty_tables(
        &self,
    ) -> Result<Vec<RuntimeArtifactRestoreDirtyTable>> {
        discovery_runtime_restore_dirty_tables_on_conn(&self.conn)
    }

    pub fn set_discovery_bootstrap_degraded_state(
        &self,
        active: bool,
        reason: Option<&str>,
        armed_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    bootstrap_degraded_active,
                    bootstrap_degraded_reason,
                    bootstrap_degraded_armed_at,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4)
                 ON CONFLICT(id) DO UPDATE SET
                    bootstrap_degraded_active = excluded.bootstrap_degraded_active,
                    bootstrap_degraded_reason = excluded.bootstrap_degraded_reason,
                    bootstrap_degraded_armed_at = excluded.bootstrap_degraded_armed_at,
                    updated_at = excluded.updated_at",
                params![
                    if active { 1 } else { 0 },
                    reason,
                    armed_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery bootstrap-degraded state")?;
        Ok(())
    }

    pub fn set_discovery_recent_raw_restore_state(
        &self,
        update: &DiscoveryRecentRawRestoreStateUpdate,
    ) -> Result<()> {
        self.ensure_discovery_recent_raw_restore_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_recent_raw_restore_state(
                    id,
                    journal_available,
                    journal_replayed,
                    required_window_start,
                    journal_covered_since,
                    journal_covered_through_cursor_ts,
                    journal_covered_through_cursor_slot,
                    journal_covered_through_cursor_signature,
                    gap_fill_replayed,
                    gap_fill_covered_since,
                    gap_fill_covered_through_cursor_ts,
                    gap_fill_covered_through_cursor_slot,
                    gap_fill_covered_through_cursor_signature,
                    effective_covered_since,
                    effective_covered_through_cursor_ts,
                    effective_covered_through_cursor_slot,
                    effective_covered_through_cursor_signature,
                    artifact_runtime_cursor_ts,
                    artifact_runtime_cursor_slot,
                    artifact_runtime_cursor_signature,
                    journal_covers_artifact_cursor,
                    raw_coverage_satisfied,
                    gap_fill_replayed_rows,
                    replayed_rows,
                    reason,
                    replay_started_at,
                    replay_completed_at,
                    updated_at
                 ) VALUES (
                    1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    journal_available = excluded.journal_available,
                    journal_replayed = excluded.journal_replayed,
                    required_window_start = excluded.required_window_start,
                    journal_covered_since = excluded.journal_covered_since,
                    journal_covered_through_cursor_ts =
                        excluded.journal_covered_through_cursor_ts,
                    journal_covered_through_cursor_slot =
                        excluded.journal_covered_through_cursor_slot,
                    journal_covered_through_cursor_signature =
                        excluded.journal_covered_through_cursor_signature,
                    gap_fill_replayed = excluded.gap_fill_replayed,
                    gap_fill_covered_since = excluded.gap_fill_covered_since,
                    gap_fill_covered_through_cursor_ts =
                        excluded.gap_fill_covered_through_cursor_ts,
                    gap_fill_covered_through_cursor_slot =
                        excluded.gap_fill_covered_through_cursor_slot,
                    gap_fill_covered_through_cursor_signature =
                        excluded.gap_fill_covered_through_cursor_signature,
                    effective_covered_since = excluded.effective_covered_since,
                    effective_covered_through_cursor_ts =
                        excluded.effective_covered_through_cursor_ts,
                    effective_covered_through_cursor_slot =
                        excluded.effective_covered_through_cursor_slot,
                    effective_covered_through_cursor_signature =
                        excluded.effective_covered_through_cursor_signature,
                    artifact_runtime_cursor_ts = excluded.artifact_runtime_cursor_ts,
                    artifact_runtime_cursor_slot = excluded.artifact_runtime_cursor_slot,
                    artifact_runtime_cursor_signature =
                        excluded.artifact_runtime_cursor_signature,
                    journal_covers_artifact_cursor =
                        excluded.journal_covers_artifact_cursor,
                    raw_coverage_satisfied = excluded.raw_coverage_satisfied,
                    gap_fill_replayed_rows = excluded.gap_fill_replayed_rows,
                    replayed_rows = excluded.replayed_rows,
                    reason = excluded.reason,
                    replay_started_at = excluded.replay_started_at,
                    replay_completed_at = excluded.replay_completed_at,
                    updated_at = excluded.updated_at",
                params![
                    if update.journal_available { 1 } else { 0 },
                    if update.journal_replayed { 1 } else { 0 },
                    update.required_window_start.map(|ts| ts.to_rfc3339()),
                    update.journal_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    if update.gap_fill_replayed { 1 } else { 0 },
                    update.gap_fill_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    update.effective_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    if update.journal_covers_artifact_cursor {
                        1
                    } else {
                        0
                    },
                    if update.raw_coverage_satisfied { 1 } else { 0 },
                    update.gap_fill_replayed_rows as i64,
                    update.replayed_rows as i64,
                    update.reason.as_deref(),
                    update.replay_started_at.map(|ts| ts.to_rfc3339()),
                    update.replay_completed_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery recent raw restore state")?;
        Ok(())
    }

    pub fn discovery_trusted_selection_state(
        &self,
    ) -> Result<Option<DiscoveryTrustedSelectionStateRow>> {
        self.ensure_discovery_strategy_state_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    trusted_selection_state,
                    active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at,
                    updated_at
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery trusted selection state")?;
        raw.map(
            |(
                bootstrap_required,
                reason,
                selection_state_raw,
                active_snapshot_id,
                active_snapshot_window_start_raw,
                last_bootstrap_source_kind_raw,
                last_bootstrap_at_raw,
                updated_at_raw,
            )| {
                Ok(DiscoveryTrustedSelectionStateRow {
                    bootstrap_required: bootstrap_required != 0,
                    reason,
                    selection_state: TrustedSelectionState::parse(&selection_state_raw)?,
                    active_snapshot_id,
                    active_snapshot_window_start: parse_optional_rfc3339_utc(
                        active_snapshot_window_start_raw,
                        "discovery_strategy_state.active_trusted_snapshot_window_start",
                    )?,
                    last_bootstrap_source_kind: match last_bootstrap_source_kind_raw {
                        Some(raw) => Some(TrustedSnapshotSourceKind::parse(&raw)?),
                        None => None,
                    },
                    last_bootstrap_at: parse_optional_rfc3339_utc(
                        last_bootstrap_at_raw,
                        "discovery_strategy_state.last_trusted_bootstrap_at",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_strategy_state.updated_at",
                    )?,
                })
            },
        )
        .transpose()
    }

    pub fn startup_trusted_selection_gate_status(
        &self,
    ) -> Result<StartupTrustedSelectionGateStatus> {
        let typed_state = self.discovery_trusted_selection_state()?;
        if let Some(typed_state) = typed_state {
            let legacy_bool_only_row = typed_state.selection_state
                == TrustedSelectionState::Invalid
                && typed_state.active_snapshot_id.is_none()
                && typed_state.active_snapshot_window_start.is_none()
                && typed_state.last_bootstrap_source_kind.is_none()
                && typed_state.last_bootstrap_at.is_none();
            if legacy_bool_only_row {
                if let Some(status) = self.startup_trusted_selection_gate_status_from_metadata(
                    typed_state.bootstrap_required,
                    Some(typed_state.reason.clone()),
                )? {
                    return Ok(status);
                }
                return Ok(StartupTrustedSelectionGateStatus {
                    bootstrap_required: typed_state.bootstrap_required,
                    selection_state: None,
                    startup_fail_closed: typed_state.bootstrap_required,
                    reason: Some(typed_state.reason),
                    active_snapshot_id: None,
                    active_snapshot_window_start: None,
                    last_bootstrap_source_kind: None,
                    source_snapshot_window_start: None,
                    legacy_bool_fallback_used: true,
                });
            }
            let source_snapshot_window_start = typed_state
                .active_snapshot_window_start
                .map(|window_start| {
                    self.trusted_wallet_metrics_snapshot_metadata_for_window(window_start)
                })
                .transpose()?
                .flatten()
                .and_then(|metadata| metadata.source_window_start);
            return Ok(StartupTrustedSelectionGateStatus {
                bootstrap_required: typed_state.bootstrap_required,
                selection_state: Some(typed_state.selection_state),
                startup_fail_closed: typed_state.bootstrap_required
                    || matches!(
                        typed_state.selection_state,
                        TrustedSelectionState::Invalid | TrustedSelectionState::TrustedBridgedStale
                    ),
                reason: Some(typed_state.reason),
                active_snapshot_id: typed_state.active_snapshot_id,
                active_snapshot_window_start: typed_state.active_snapshot_window_start,
                last_bootstrap_source_kind: typed_state.last_bootstrap_source_kind,
                source_snapshot_window_start,
                legacy_bool_fallback_used: false,
            });
        }

        let bootstrap_required = self.discovery_trusted_selection_bootstrap_required()?;
        if let Some(status) =
            self.startup_trusted_selection_gate_status_from_metadata(bootstrap_required, None)?
        {
            return Ok(status);
        }
        Ok(StartupTrustedSelectionGateStatus {
            bootstrap_required,
            selection_state: None,
            startup_fail_closed: bootstrap_required,
            reason: None,
            active_snapshot_id: None,
            active_snapshot_window_start: None,
            last_bootstrap_source_kind: None,
            source_snapshot_window_start: None,
            legacy_bool_fallback_used: true,
        })
    }

    pub fn set_discovery_trusted_selection_state(
        &self,
        update: &DiscoveryTrustedSelectionStateUpdate,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    trusted_selection_state,
                    active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    trusted_selection_bootstrap_required =
                        excluded.trusted_selection_bootstrap_required,
                    trusted_selection_reason = excluded.trusted_selection_reason,
                    trusted_selection_state = excluded.trusted_selection_state,
                    active_trusted_snapshot_id = excluded.active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start =
                        excluded.active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind =
                        excluded.last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at = excluded.last_trusted_bootstrap_at,
                    updated_at = excluded.updated_at",
                params![
                    if update.bootstrap_required { 1 } else { 0 },
                    &update.reason,
                    update.selection_state.as_str(),
                    &update.active_snapshot_id,
                    update
                        .active_snapshot_window_start
                        .map(canonical_wallet_metrics_window_start),
                    update
                        .last_bootstrap_source_kind
                        .map(TrustedSnapshotSourceKind::as_str),
                    update.last_bootstrap_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery trusted selection state")?;
        Ok(())
    }

    pub fn latest_trusted_wallet_metrics_snapshot_metadata(
        &self,
    ) -> Result<Option<TrustedWalletMetricsSnapshotRow>> {
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start,
                    effective_window_start,
                    created_at,
                    source_kind,
                    row_count,
                    trust_state
                 FROM trusted_wallet_metrics_snapshots
                 ORDER BY effective_window_start DESC, created_at DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading latest trusted wallet_metrics snapshot metadata")?;
        raw.map(
            |(
                snapshot_id,
                source_snapshot_id,
                source_window_start_raw,
                effective_window_start_raw,
                created_at_raw,
                source_kind_raw,
                row_count,
                trust_state_raw,
            )| {
                Ok(TrustedWalletMetricsSnapshotRow {
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start: parse_optional_rfc3339_utc(
                        source_window_start_raw,
                        "trusted_wallet_metrics_snapshots.source_window_start",
                    )?,
                    effective_window_start: parse_rfc3339_utc(
                        &effective_window_start_raw,
                        "trusted_wallet_metrics_snapshots.effective_window_start",
                    )?,
                    created_at: parse_rfc3339_utc(
                        &created_at_raw,
                        "trusted_wallet_metrics_snapshots.created_at",
                    )?,
                    source_kind: TrustedSnapshotSourceKind::parse(&source_kind_raw)?,
                    row_count: row_count.max(0) as usize,
                    trust_state: TrustedSelectionState::parse(&trust_state_raw)?,
                })
            },
        )
        .transpose()
    }

    pub fn trusted_wallet_metrics_snapshot_metadata_for_window(
        &self,
        effective_window_start: DateTime<Utc>,
    ) -> Result<Option<TrustedWalletMetricsSnapshotRow>> {
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start,
                    effective_window_start,
                    created_at,
                    source_kind,
                    row_count,
                    trust_state
                 FROM trusted_wallet_metrics_snapshots
                 WHERE effective_window_start = ?1",
                params![canonical_wallet_metrics_window_start(
                    effective_window_start
                )],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading trusted wallet_metrics snapshot metadata by window")?;
        raw.map(
            |(
                snapshot_id,
                source_snapshot_id,
                source_window_start_raw,
                effective_window_start_raw,
                created_at_raw,
                source_kind_raw,
                row_count,
                trust_state_raw,
            )| {
                Ok(TrustedWalletMetricsSnapshotRow {
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start: parse_optional_rfc3339_utc(
                        source_window_start_raw,
                        "trusted_wallet_metrics_snapshots.source_window_start",
                    )?,
                    effective_window_start: parse_rfc3339_utc(
                        &effective_window_start_raw,
                        "trusted_wallet_metrics_snapshots.effective_window_start",
                    )?,
                    created_at: parse_rfc3339_utc(
                        &created_at_raw,
                        "trusted_wallet_metrics_snapshots.created_at",
                    )?,
                    source_kind: TrustedSnapshotSourceKind::parse(&source_kind_raw)?,
                    row_count: row_count.max(0) as usize,
                    trust_state: TrustedSelectionState::parse(&trust_state_raw)?,
                })
            },
        )
        .transpose()
    }

    pub fn upsert_wallet_activity_days(&self, rows: &[WalletActivityDayRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.with_immediate_transaction_retry("wallet_activity_days upsert", |conn| {
            upsert_wallet_activity_days_on_conn(conn, rows)
        })
    }

    pub fn wallet_active_day_counts_since(
        &self,
        wallet_ids: &[String],
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        if wallet_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let day_start = window_start.date_naive();
        let mut counts = HashMap::new();
        for chunk in wallet_ids.chunks(900) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT wallet_id, COUNT(*)
                 FROM wallet_activity_days
                 WHERE (
                        activity_day > ?1
                        OR (activity_day = ?1 AND last_seen >= ?2)
                    )
                   AND wallet_id IN ({placeholders})
                 GROUP BY wallet_id"
            );
            let mut params = vec![
                rusqlite::types::Value::from(day_start.format("%Y-%m-%d").to_string()),
                rusqlite::types::Value::from(window_start.to_rfc3339()),
            ];
            params.extend(chunk.iter().cloned().map(rusqlite::types::Value::from));
            let mut stmt = self
                .conn
                .prepare(&sql)
                .context("failed to prepare wallet_activity_days count query")?;
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params))
                .context("failed querying wallet_activity_days counts")?;
            while let Some(row) = rows
                .next()
                .context("failed iterating wallet_activity_days counts")?
            {
                let wallet_id: String = row
                    .get(0)
                    .context("failed reading wallet_activity_days.wallet_id")?;
                let count: i64 = row
                    .get(1)
                    .context("failed reading wallet_activity_days count")?;
                counts.insert(wallet_id, count.max(0) as u32);
            }
        }

        Ok(counts)
    }

    pub fn backfill_wallet_activity_days_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<usize> {
        self.with_immediate_transaction_retry("wallet_activity_days backfill", |conn| {
            conn.execute(
                "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
                 SELECT wallet_id, substr(ts, 1, 10) AS activity_day, MAX(ts) AS last_seen
                 FROM observed_swaps
                 WHERE ts >= ?1
                 GROUP BY wallet_id, substr(ts, 1, 10)
                 ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                    last_seen = CASE
                        WHEN excluded.last_seen > wallet_activity_days.last_seen
                            THEN excluded.last_seen
                        ELSE wallet_activity_days.last_seen
                    END",
                params![window_start.to_rfc3339()],
            )
            .context("failed to backfill wallet_activity_days from observed_swaps")
        })
    }

    pub fn wallet_metrics_window_exists(&self, window_start: DateTime<Utc>) -> Result<bool> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                )",
                params![canonical, legacy_z],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_metrics window_start existence")?;
        Ok(exists != 0)
    }

    pub fn latest_wallet_metrics_window_start(&self) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                // Legacy `Z` and canonical `+00:00` differ only in the UTC suffix, so raw
                // RFC3339 string order remains chronological and only affects ties for the
                // same logical instant.
                "SELECT window_start
                 FROM wallet_metrics
                 ORDER BY window_start DESC
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .context("failed querying latest wallet_metrics window_start")?
            .flatten();
        raw.map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallet_metrics.window_start rfc3339 value: {raw}")
                })
        })
        .transpose()
    }

    pub fn load_latest_wallet_metric_snapshots(
        &self,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        let Some(window_start) = self.latest_wallet_metrics_window_start()? else {
            return Ok(Vec::new());
        };
        self.load_wallet_metric_snapshots_for_window(window_start)
    }

    pub fn load_wallet_metric_snapshots_for_window(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    wallet_metrics.wallet_id,
                    wallets.first_seen,
                    wallets.last_seen,
                    wallet_metrics.pnl,
                    wallet_metrics.win_rate,
                    wallet_metrics.trades,
                    wallet_metrics.closed_trades,
                    wallet_metrics.hold_median_seconds,
                    wallet_metrics.score,
                    wallet_metrics.buy_total,
                    wallet_metrics.tradable_ratio,
                    wallet_metrics.rug_ratio
                 FROM wallet_metrics
                 JOIN (
                    SELECT
                        wallet_id,
                        COALESCE(
                            MAX(CASE WHEN window_start = ?1 THEN id END),
                            MAX(id)
                        ) AS selected_id
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                    GROUP BY wallet_id
                 ) AS selected_wallet_metrics
                    ON selected_wallet_metrics.selected_id = wallet_metrics.id
                 JOIN wallets ON wallets.wallet_id = wallet_metrics.wallet_id
                 ORDER BY wallet_metrics.score DESC, wallet_metrics.wallet_id ASC",
            )
            .context("failed to prepare wallet_metrics snapshot query for requested window")?;
        let mut rows = stmt
            .query(params![canonical, legacy_z])
            .context("failed querying wallet_metrics snapshots for requested window")?;
        let mut snapshots = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_metrics snapshots for requested window")?
        {
            let first_seen_raw: String = row.get(1).context("failed reading wallets.first_seen")?;
            let last_seen_raw: String = row.get(2).context("failed reading wallets.last_seen")?;
            let first_seen = DateTime::parse_from_rfc3339(&first_seen_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallets.first_seen rfc3339 value: {first_seen_raw}")
                })?;
            let last_seen = DateTime::parse_from_rfc3339(&last_seen_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallets.last_seen rfc3339 value: {last_seen_raw}")
                })?;
            let trades: i64 = row.get(5).context("failed reading wallet_metrics.trades")?;
            let closed_trades: i64 = row
                .get(6)
                .context("failed reading wallet_metrics.closed_trades")?;
            let buy_total: i64 = row
                .get(9)
                .context("failed reading wallet_metrics.buy_total")?;
            if trades < 0 || closed_trades < 0 || buy_total < 0 {
                return Err(anyhow::anyhow!(
                    "invalid negative wallet_metrics counts in requested snapshot window"
                ));
            }
            snapshots.push(PersistedWalletMetricSnapshotRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_metrics.wallet_id")?,
                window_start,
                first_seen,
                last_seen,
                pnl: row.get(3).context("failed reading wallet_metrics.pnl")?,
                win_rate: row
                    .get(4)
                    .context("failed reading wallet_metrics.win_rate")?,
                trades: trades as u32,
                closed_trades: closed_trades as u32,
                hold_median_seconds: row
                    .get(7)
                    .context("failed reading wallet_metrics.hold_median_seconds")?,
                score: row.get(8).context("failed reading wallet_metrics.score")?,
                buy_total: buy_total as u32,
                tradable_ratio: row
                    .get(10)
                    .context("failed reading wallet_metrics.tradable_ratio")?,
                rug_ratio: row
                    .get(11)
                    .context("failed reading wallet_metrics.rug_ratio")?,
            });
        }

        Ok(snapshots)
    }

    pub fn wallet_metrics_row_count_for_window(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<usize> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);
        let count = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM (
                    SELECT wallet_id
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                    GROUP BY wallet_id
                 )",
                params![canonical, legacy_z],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting wallet_metrics rows for window")?;
        Ok(count.max(0) as usize)
    }

    pub fn export_discovery_runtime_artifact(
        &self,
        exported_at: DateTime<Utc>,
        export_gate: DiscoveryPublicationFreshnessGate,
    ) -> Result<DiscoveryRuntimeArtifact> {
        self.with_deferred_transaction("discovery runtime artifact export", |_conn| {
            let publication_state = self
                .discovery_publication_state_read_only()?
                .ok_or_else(|| anyhow::anyhow!("discovery runtime artifact export requires persisted publication truth"))?;
            let truth_detail =
                runtime_artifact_export_truth_detail(&publication_state, export_gate, exported_at);
            if !publication_state.has_complete_publication_truth() {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires complete publication truth ({truth_detail})"
                ));
            }
            if !publication_state.is_fresh_under_gate(export_gate, exported_at) {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires fresh publication truth under export gate ({truth_detail})"
                ));
            }
            let runtime_cursor: Option<(String, i64, String)> = self
                .conn
                .query_row(
                    "SELECT cursor_ts, cursor_slot, cursor_signature
                     FROM discovery_runtime_state
                     WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .context("failed reading discovery runtime cursor for artifact export")?;
            let Some((cursor_ts_raw, cursor_slot_raw, cursor_signature)) = runtime_cursor else {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires a persisted discovery runtime cursor ({truth_detail})"
                ));
            };
            let runtime_cursor = super::DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(
                    &cursor_ts_raw,
                    "discovery_runtime_state.cursor_ts",
                )?,
                slot: cursor_slot_raw.max(0) as u64,
                signature: cursor_signature,
            };
            let published_window_start = publication_state
                .last_published_window_start
                .expect("validated complete publication truth above");
            let artifact = DiscoveryRuntimeArtifact {
                format_version: DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
                exported_at,
                export_gate,
                publication_state,
                runtime_cursor,
                published_wallet_metrics_snapshot: self
                    .load_wallet_metric_snapshots_for_window(published_window_start)?,
            };
            validate_runtime_artifact_snapshot_shape(&artifact)?;
            Ok(artifact)
        })
    }

    pub fn restore_discovery_runtime_artifact(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        restored_at: DateTime<Utc>,
        bootstrap_degraded: bool,
    ) -> Result<()> {
        validate_runtime_artifact_snapshot_shape(artifact)?;
        self.ensure_discovery_strategy_state_table()?;
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        self.ensure_discovery_recent_raw_restore_state_table()?;
        let dirty_tables = self.discovery_runtime_restore_dirty_tables()?;
        if !dirty_tables.is_empty() {
            let detail = format_runtime_artifact_restore_dirty_tables(&dirty_tables);
            return Err(anyhow::anyhow!(
                "discovery runtime artifact restore requires an empty runtime db; found durable rows in {detail}"
            ));
        }
        self.with_immediate_transaction_retry("discovery runtime artifact restore", |conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_runtime_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    cursor_ts TEXT NOT NULL,
                    cursor_slot INTEGER NOT NULL,
                    cursor_signature TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS discovery_persisted_rebuild_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    phase TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    horizon_end TEXT NOT NULL,
                    metrics_window_start TEXT NOT NULL,
                    phase_cursor_ts TEXT,
                    phase_cursor_slot INTEGER,
                    phase_cursor_signature TEXT,
                    prepass_rows_processed INTEGER NOT NULL DEFAULT 0,
                    prepass_pages_processed INTEGER NOT NULL DEFAULT 0,
                    replay_rows_processed INTEGER NOT NULL DEFAULT 0,
                    replay_pages_processed INTEGER NOT NULL DEFAULT 0,
                    chunks_completed INTEGER NOT NULL DEFAULT 0,
                    state_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS discovery_recent_raw_restore_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    journal_available INTEGER NOT NULL DEFAULT 0,
                    journal_replayed INTEGER NOT NULL DEFAULT 0,
                    required_window_start TEXT,
                    journal_covered_since TEXT,
                    journal_covered_through_cursor_ts TEXT,
                    journal_covered_through_cursor_slot INTEGER,
                    journal_covered_through_cursor_signature TEXT,
                    gap_fill_replayed INTEGER NOT NULL DEFAULT 0,
                    gap_fill_covered_since TEXT,
                    gap_fill_covered_through_cursor_ts TEXT,
                    gap_fill_covered_through_cursor_slot INTEGER,
                    gap_fill_covered_through_cursor_signature TEXT,
                    effective_covered_since TEXT,
                    effective_covered_through_cursor_ts TEXT,
                    effective_covered_through_cursor_slot INTEGER,
                    effective_covered_through_cursor_signature TEXT,
                    artifact_runtime_cursor_ts TEXT,
                    artifact_runtime_cursor_slot INTEGER,
                    artifact_runtime_cursor_signature TEXT,
                    journal_covers_artifact_cursor INTEGER NOT NULL DEFAULT 0,
                    raw_coverage_satisfied INTEGER NOT NULL DEFAULT 0,
                    gap_fill_replayed_rows INTEGER NOT NULL DEFAULT 0,
                    replayed_rows INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    replay_started_at TEXT,
                    replay_completed_at TEXT,
                    updated_at TEXT NOT NULL
                );",
            )
            .context("failed ensuring discovery runtime restore tables exist")?;

            let dirty_tables = discovery_runtime_restore_dirty_tables_on_conn(conn)?;
            if !dirty_tables.is_empty() {
                let detail = format_runtime_artifact_restore_dirty_tables(&dirty_tables);
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact restore requires an empty runtime db; found durable rows in {detail}"
                ));
            }

            {
                let mut wallet_stmt = conn
                    .prepare_cached(
                        "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                         VALUES (?1, ?2, ?3, ?4)",
                    )
                    .context("failed to prepare runtime artifact wallet restore statement")?;
                for row in &artifact.published_wallet_metrics_snapshot {
                    let status = if artifact
                        .publication_state
                        .published_wallet_ids
                        .as_ref()
                        .is_some_and(|wallet_ids| wallet_ids.contains(&row.wallet_id))
                    {
                        "candidate"
                    } else {
                        "observed"
                    };
                    wallet_stmt
                        .execute(params![
                            &row.wallet_id,
                            row.first_seen.to_rfc3339(),
                            row.last_seen.to_rfc3339(),
                            status,
                        ])
                        .context("failed inserting runtime artifact wallet row")?;
                }
            }

            {
                let mut metric_stmt = conn
                    .prepare_cached(
                        "INSERT INTO wallet_metrics(
                            wallet_id,
                            window_start,
                            pnl,
                            win_rate,
                            trades,
                            closed_trades,
                            hold_median_seconds,
                            score,
                            buy_total,
                            tradable_ratio,
                            rug_ratio
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    )
                    .context("failed to prepare runtime artifact wallet_metrics restore statement")?;
                for row in &artifact.published_wallet_metrics_snapshot {
                    metric_stmt
                        .execute(params![
                            &row.wallet_id,
                            canonical_wallet_metrics_window_start(row.window_start),
                            row.pnl,
                            row.win_rate,
                            row.trades as i64,
                            row.closed_trades as i64,
                            row.hold_median_seconds,
                            row.score,
                            row.buy_total as i64,
                            row.tradable_ratio,
                            row.rug_ratio,
                        ])
                        .context("failed inserting runtime artifact wallet_metrics row")?;
                }
            }

            {
                let mut follow_stmt = conn
                    .prepare_cached(
                        "INSERT INTO followlist(wallet_id, added_at, reason, active)
                         VALUES (?1, ?2, ?3, 1)",
                    )
                    .context("failed to prepare runtime artifact followlist restore statement")?;
                for wallet_id in artifact
                    .publication_state
                    .published_wallet_ids
                    .as_ref()
                    .expect("validated complete publication truth above")
                {
                    follow_stmt
                        .execute(params![
                            wallet_id,
                            restored_at.to_rfc3339(),
                            "runtime_artifact_restore",
                        ])
                        .context("failed inserting runtime artifact followlist row")?;
                }
            }

            conn.execute(
                "INSERT INTO discovery_runtime_state(
                    id,
                    cursor_ts,
                    cursor_slot,
                    cursor_signature,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4)",
                params![
                    artifact.runtime_cursor.ts_utc.to_rfc3339(),
                    artifact.runtime_cursor.slot as i64,
                    artifact.runtime_cursor.signature.as_str(),
                    restored_at.to_rfc3339(),
                ],
            )
            .context("failed restoring discovery runtime cursor from artifact")?;

            let published_wallet_ids_json = serde_json::to_string(
                artifact
                    .publication_state
                    .published_wallet_ids
                    .as_ref()
                    .expect("validated complete publication truth above"),
            )
            .context("failed serializing runtime artifact published wallet ids")?;
            let published_window_start = artifact
                .publication_state
                .last_published_window_start
                .expect("validated complete publication truth above");
            let active_snapshot = TrustedWalletMetricsSnapshotWrite {
                snapshot_id: format!(
                    "wallet_metrics:{}:{}",
                    TrustedSnapshotSourceKind::DiscoveryRefresh.as_str(),
                    published_window_start.to_rfc3339()
                ),
                source_snapshot_id: None,
                source_window_start: Some(published_window_start),
                effective_window_start: published_window_start,
                created_at: artifact
                    .publication_state
                    .last_published_at
                    .unwrap_or(artifact.publication_state.updated_at),
                source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
                row_count: artifact.published_wallet_metrics_snapshot.len(),
                trust_state: TrustedSelectionState::TrustedCurrent,
            };
            insert_trusted_wallet_metrics_snapshot_on_conn(conn, &active_snapshot)?;
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    trusted_selection_state,
                    active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at,
                    bootstrap_degraded_active,
                    bootstrap_degraded_reason,
                    bootstrap_degraded_armed_at,
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    publication_wallet_ids_json,
                    updated_at
                 ) VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    ?7,
                    ?8,
                    ?9,
                    ?10,
                    ?11,
                    ?12,
                    ?13,
                    ?14,
                    ?15,
                    ?16,
                    ?17,
                    ?18
                 )",
                params![
                    1_i64,
                    0_i64,
                    "runtime_artifact_restore",
                    TrustedSelectionState::TrustedCurrent.as_str(),
                    active_snapshot.snapshot_id.as_str(),
                    canonical_wallet_metrics_window_start(active_snapshot.effective_window_start),
                    TrustedSnapshotSourceKind::DiscoveryRefresh.as_str(),
                    restored_at.to_rfc3339(),
                    if bootstrap_degraded { 1 } else { 0 },
                    bootstrap_degraded.then_some("runtime_artifact_restore_bootstrap_degraded"),
                    bootstrap_degraded.then_some(restored_at.to_rfc3339()),
                    artifact.publication_state.runtime_mode.as_str(),
                    &artifact.publication_state.reason,
                    artifact
                        .publication_state
                        .last_published_at
                        .map(|ts| ts.to_rfc3339()),
                    artifact
                        .publication_state
                        .last_published_window_start
                        .map(canonical_wallet_metrics_window_start),
                    artifact.publication_state.published_scoring_source.as_deref(),
                    published_wallet_ids_json.as_str(),
                    artifact.publication_state.updated_at.to_rfc3339(),
                ],
            )
            .context("failed restoring discovery publication state from artifact")?;
            conn.execute(
                "INSERT INTO discovery_recent_raw_restore_state(
                    id,
                    journal_available,
                    journal_replayed,
                    required_window_start,
                    journal_covered_since,
                    journal_covered_through_cursor_ts,
                    journal_covered_through_cursor_slot,
                    journal_covered_through_cursor_signature,
                    gap_fill_replayed,
                    gap_fill_covered_since,
                    gap_fill_covered_through_cursor_ts,
                    gap_fill_covered_through_cursor_slot,
                    gap_fill_covered_through_cursor_signature,
                    effective_covered_since,
                    effective_covered_through_cursor_ts,
                    effective_covered_through_cursor_slot,
                    effective_covered_through_cursor_signature,
                    artifact_runtime_cursor_ts,
                    artifact_runtime_cursor_slot,
                    artifact_runtime_cursor_signature,
                    journal_covers_artifact_cursor,
                    raw_coverage_satisfied,
                    gap_fill_replayed_rows,
                    replayed_rows,
                    reason,
                    replay_started_at,
                    replay_completed_at,
                    updated_at
                 ) VALUES (
                    1, 0, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?1, ?2, ?3, 0, 0, 0, 0, ?4, NULL, NULL, ?5
                 )",
                params![
                    artifact.runtime_cursor.ts_utc.to_rfc3339(),
                    artifact.runtime_cursor.slot as i64,
                    artifact.runtime_cursor.signature.as_str(),
                    "journal_replay_pending",
                    restored_at.to_rfc3339(),
                ],
            )
            .context("failed initializing discovery recent raw restore state from artifact")?;
            Ok(())
        })
    }

    pub fn clone_wallet_metrics_window(
        &self,
        source_window_start: DateTime<Utc>,
        target_window_start: DateTime<Utc>,
        expected_source_rows: usize,
    ) -> Result<usize> {
        self.clone_wallet_metrics_window_with_metadata(
            source_window_start,
            target_window_start,
            expected_source_rows,
            None,
        )
    }

    pub fn clone_wallet_metrics_window_with_metadata(
        &self,
        source_window_start: DateTime<Utc>,
        target_window_start: DateTime<Utc>,
        expected_source_rows: usize,
        snapshot_write: Option<&TrustedWalletMetricsSnapshotWrite>,
    ) -> Result<usize> {
        if snapshot_write.is_some() {
            self.ensure_trusted_wallet_metrics_snapshots_table()?;
        }
        let (source_canonical, source_legacy_z) =
            wallet_metrics_window_start_query_variants(source_window_start);
        let (target_canonical, target_legacy_z) =
            wallet_metrics_window_start_query_variants(target_window_start);
        self.with_immediate_transaction_retry("wallet_metrics clone-latest bridge", |conn| {
            let source_rows: i64 = conn
                .query_row(
                    "SELECT COUNT(*)
                     FROM (
                        SELECT wallet_id
                        FROM wallet_metrics
                        WHERE window_start IN (?1, ?2)
                        GROUP BY wallet_id
                     )",
                    params![&source_canonical, &source_legacy_z],
                    |row| row.get(0),
                )
                .context("failed counting source wallet_metrics rows for clone-latest bridge")?;
            let source_rows = source_rows.max(0) as usize;
            if source_rows != expected_source_rows {
                return Err(anyhow::anyhow!(
                    "wallet_metrics source window changed during clone-latest bootstrap: source_window_start={} expected_source_rows={} actual_source_rows={}",
                    source_window_start.to_rfc3339(),
                    expected_source_rows,
                    source_rows,
                ));
            }

            let target_exists = conn
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1
                        FROM wallet_metrics
                        WHERE window_start IN (?1, ?2)
                    )",
                    params![&target_canonical, &target_legacy_z],
                    |row| row.get::<_, i64>(0),
                )
                .context("failed checking target wallet_metrics window for clone-latest bridge")?;
            if target_exists != 0 {
                return Err(anyhow::anyhow!(
                    "target wallet_metrics bootstrap bucket already exists at {}",
                    target_window_start.to_rfc3339(),
                ));
            }

            conn.execute(
                "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 )
                 SELECT
                    source.wallet_id,
                    ?3,
                    source.pnl,
                    source.win_rate,
                    source.trades,
                    source.closed_trades,
                    source.hold_median_seconds,
                    source.score,
                    source.buy_total,
                    source.tradable_ratio,
                    source.rug_ratio
                 FROM wallet_metrics AS source
                 JOIN (
                    SELECT
                        wallet_id,
                        COALESCE(
                            MAX(CASE WHEN window_start = ?1 THEN id END),
                            MAX(id)
                        ) AS selected_id
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                    GROUP BY wallet_id
                 ) AS selected_wallet_metrics
                    ON selected_wallet_metrics.selected_id = source.id",
                params![
                    &source_canonical,
	                    &source_legacy_z,
	                    &target_canonical,
	                ],
	            )
	            .context("failed cloning wallet_metrics snapshot window")?;
            let inserted_rows = conn.changes() as usize;
            if let Some(snapshot_write) = snapshot_write {
                insert_trusted_wallet_metrics_snapshot_on_conn(conn, snapshot_write)?;
            }
            Ok(inserted_rows)
        })
    }

    pub fn upsert_wallet(
        &self,
        wallet_id: &str,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        status: &str,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(wallet_id) DO UPDATE SET
                    first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                    last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                    status = excluded.status",
                params![
                    wallet_id,
                    first_seen.to_rfc3339(),
                    last_seen.to_rfc3339(),
                    status,
                ],
            )
        })
            .context("failed to upsert wallet")?;
        Ok(())
    }

    pub fn insert_wallet_metric(&self, metric: &WalletMetricRow) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    &metric.wallet_id,
                    canonical_wallet_metrics_window_start(metric.window_start),
                    metric.pnl,
                    metric.win_rate,
                    metric.trades as i64,
                    metric.closed_trades as i64,
                    metric.hold_median_seconds,
                    metric.score,
                    metric.buy_total as i64,
                    metric.tradable_ratio,
                    metric.rug_ratio,
                ],
            )
        })
        .context("failed to insert wallet metric")?;
        Ok(())
    }

    pub fn persist_discovery_cycle(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        allow_followlist_activate: bool,
        allow_followlist_deactivate: bool,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        self.persist_discovery_cycle_with_snapshot_metadata(
            wallets,
            metrics,
            desired_wallets,
            allow_followlist_activate,
            allow_followlist_deactivate,
            now,
            reason,
            None,
        )
    }

    pub fn persist_discovery_cycle_with_snapshot_metadata(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        allow_followlist_activate: bool,
        allow_followlist_deactivate: bool,
        now: DateTime<Utc>,
        reason: &str,
        snapshot_write: Option<&TrustedWalletMetricsSnapshotWrite>,
    ) -> Result<FollowlistUpdateResult> {
        if snapshot_write.is_some() {
            self.ensure_trusted_wallet_metrics_snapshots_table()?;
        }
        self.with_immediate_transaction_retry("discovery write", |conn| {
            let retention_offset = DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS.saturating_sub(1);
            {
                let mut stmt = conn
                    .prepare_cached(
                    "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(wallet_id) DO UPDATE SET
                        first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                        last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                        status = excluded.status",
                    )
                    .context("failed to prepare discovery wallet upsert statement")?;
                for wallet in wallets {
                    stmt.execute(params![
                        &wallet.wallet_id,
                        wallet.first_seen.to_rfc3339(),
                        wallet.last_seen.to_rfc3339(),
                        &wallet.status,
                    ])
                    .context("failed to upsert wallet in discovery transaction")?;
                }
            }

            if !metrics.is_empty() {
                {
                    let mut stmt = conn
                        .prepare_cached(
                        "INSERT INTO wallet_metrics(
                            wallet_id,
                            window_start,
                            pnl,
                            win_rate,
                            trades,
                            closed_trades,
                            hold_median_seconds,
                            score,
                            buy_total,
                            tradable_ratio,
                            rug_ratio
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        )
                        .context("failed to prepare discovery wallet metric insert statement")?;
                    for metric in metrics {
                        stmt.execute(params![
                            &metric.wallet_id,
                            canonical_wallet_metrics_window_start(metric.window_start),
                            metric.pnl,
                            metric.win_rate,
                            metric.trades as i64,
                            metric.closed_trades as i64,
                            metric.hold_median_seconds,
                            metric.score,
                            metric.buy_total as i64,
                            metric.tradable_ratio,
                            metric.rug_ratio,
                        ])
                        .context("failed to insert wallet metric in discovery transaction")?;
                    }
                }

                conn.execute(
                    "DELETE FROM wallet_metrics
                     WHERE unixepoch(window_start) < (
                        SELECT cutoff_window_epoch
                        FROM (
                            SELECT unixepoch(window_start) AS cutoff_window_epoch
                            FROM wallet_metrics
                            GROUP BY unixepoch(window_start)
                            ORDER BY cutoff_window_epoch DESC
                            LIMIT 1 OFFSET ?1
                        )
                     )",
                    params![retention_offset],
                )
                .context("failed to apply wallet_metrics retention in discovery transaction")?;
                if let Some(snapshot_write) = snapshot_write {
                    insert_trusted_wallet_metrics_snapshot_on_conn(conn, snapshot_write)?;
                }
            }

            let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
            let active_wallets: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
                    .context("failed to prepare active followlist query in discovery transaction")?;
                let mut rows = stmt
                    .query([])
                    .context("failed querying active followlist in discovery transaction")?;
                let mut wallets = Vec::new();
                while let Some(row) = rows
                    .next()
                    .context("failed iterating active followlist in discovery transaction")?
                {
                    wallets.push(
                        row.get(0).context(
                            "failed reading followlist.wallet_id in discovery transaction",
                        )?,
                    );
                }
                wallets
            };

            let now_raw = now.to_rfc3339();
            let mut result = FollowlistUpdateResult::default();
            if allow_followlist_deactivate {
                let mut deactivate_stmt = conn
                    .prepare_cached(
                        "UPDATE followlist
                         SET active = 0, removed_at = ?1, reason = ?2
                         WHERE wallet_id = ?3 AND active = 1",
                    )
                    .context("failed to prepare followlist deactivate statement")?;
                for wallet_id in active_wallets.iter() {
                    if !desired.contains(wallet_id.as_str()) {
                        let changed = deactivate_stmt
                            .execute(params![&now_raw, reason, wallet_id])
                            .context(
                                "failed to deactivate follow wallet in discovery transaction",
                            )?;
                        if changed > 0 {
                            result.deactivated += 1;
                        }
                    }
                }
            }

            if allow_followlist_activate {
                let mut activate_stmt = conn
                    .prepare_cached(
                    "INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                    )
                    .context("failed to prepare followlist activate statement")?;
                for wallet_id in desired_wallets {
                    let changed = activate_stmt
                        .execute(params![wallet_id, &now_raw, reason])
                        .context("failed to activate follow wallet in discovery transaction")?;
                    if changed > 0 {
                        result.activated += 1;
                    }
                }
            }

            Ok(result)
        })
    }

    pub fn list_active_follow_wallets(&self) -> Result<HashSet<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
            .context("failed to prepare active followlist query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying active followlist entries")?;

        let mut wallets = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating active followlist entries")?
        {
            let wallet_id: String = row.get(0).context("failed reading followlist.wallet_id")?;
            wallets.insert(wallet_id);
        }
        Ok(wallets)
    }

    pub fn was_wallet_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> Result<bool> {
        let ts_raw = ts.to_rfc3339();
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM followlist
                 WHERE wallet_id = ?1
                   AND added_at <= ?2
                   AND (removed_at IS NULL OR ?2 < removed_at)
                 LIMIT 1",
                params![wallet_id, ts_raw],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking temporal followlist membership")?;
        Ok(exists.is_some())
    }

    pub fn deactivate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let changed = self
            .conn
            .execute(
                "UPDATE followlist
                 SET active = 0, removed_at = ?1, reason = ?2
                 WHERE wallet_id = ?3 AND active = 1",
                params![now.to_rfc3339(), reason, wallet_id],
            )
            .context("failed to deactivate follow wallet")?;
        Ok(changed > 0)
    }

    pub fn activate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let changed = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                    params![wallet_id, now.to_rfc3339(), reason],
                )
            })
            .context("failed to activate follow wallet")?;
        Ok(changed > 0)
    }

    pub fn recent_copy_signal_counts_for_wallets_by_status(
        &self,
        since: DateTime<Utc>,
        wallet_ids: &[String],
        status: &str,
    ) -> Result<Vec<WalletRecentActivityCountRow>> {
        if wallet_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT wallet_id, COUNT(*), MAX(ts)
             FROM copy_signals
             WHERE status = ?1
               AND ts >= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id
             ORDER BY wallet_id ASC"
        );
        let mut params = vec![
            rusqlite::types::Value::from(status.to_string()),
            rusqlite::types::Value::from(since.to_rfc3339()),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare recent copy_signals wallet activity query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying recent copy_signals wallet activity")?;

        let mut summaries = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent copy_signals wallet activity rows")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading recent copy_signals wallet_id")?;
            let row_count_raw: i64 = row
                .get(1)
                .context("failed reading recent copy_signals row_count")?;
            let latest_ts_raw: String = row
                .get(2)
                .context("failed reading recent copy_signals latest_ts")?;
            summaries.push(WalletRecentActivityCountRow {
                wallet_id,
                row_count: row_count_raw.max(0) as usize,
                latest_ts: parse_rfc3339_utc(&latest_ts_raw, "recent copy_signals latest_ts")?,
            });
        }
        Ok(summaries)
    }

    pub fn append_discovery_wallet_freshness_capture(
        &self,
        capture: &DiscoveryWalletFreshnessCaptureWrite,
    ) -> Result<DiscoveryWalletFreshnessCaptureRow> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        let published_wallet_ids_json = serde_json::to_string(&capture.published_wallet_ids)
            .context("failed serializing discovery wallet freshness published wallet ids")?;
        let active_follow_wallet_ids_json = serde_json::to_string(
            &capture.active_follow_wallet_ids,
        )
        .context("failed serializing discovery wallet freshness active follow wallet ids")?;
        let current_raw_top_wallet_ids_json = serde_json::to_string(
            &capture.current_raw_top_wallet_ids,
        )
        .context("failed serializing discovery wallet freshness current raw top wallet ids")?;
        let capture_id = self
            .execute_with_retry_result(|conn| {
                conn.execute(
                    "INSERT INTO discovery_wallet_freshness_history(
                        captured_at,
                        recent_cycles,
                        verdict,
                        reason,
                        publication_age_seconds,
                        raw_truth_sufficient,
                        raw_truth_reason,
                        shadow_signal_verdict,
                        shadow_signal_reason,
                        published_wallet_ids_json,
                        active_follow_wallet_ids_json,
                        current_raw_top_wallet_ids_json,
                        audit_json,
                        shadow_signal_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        capture.captured_at.to_rfc3339(),
                        capture.recent_cycles.max(1) as i64,
                        capture.verdict,
                        capture.reason,
                        capture
                            .publication_age_seconds
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        if capture.raw_truth_sufficient { 1 } else { 0 },
                        capture.raw_truth_reason,
                        capture.shadow_signal_verdict,
                        capture.shadow_signal_reason,
                        published_wallet_ids_json,
                        active_follow_wallet_ids_json,
                        current_raw_top_wallet_ids_json,
                        capture.audit_json,
                        capture.shadow_signal_json,
                    ],
                )?;
                Ok(conn.last_insert_rowid())
            })
            .context("failed appending discovery wallet freshness capture")?;
        self.load_discovery_wallet_freshness_capture(capture_id)?
            .ok_or_else(|| anyhow::anyhow!("wallet freshness capture disappeared after insert"))
    }

    pub fn list_discovery_wallet_freshness_captures(
        &self,
        limit: usize,
    ) -> Result<Vec<DiscoveryWalletFreshnessCaptureRow>> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        let query_limit = limit.max(1).min(i64::MAX as usize) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    capture_id,
                    captured_at,
                    recent_cycles,
                    verdict,
                    reason,
                    publication_age_seconds,
                    raw_truth_sufficient,
                    raw_truth_reason,
                    shadow_signal_verdict,
                    shadow_signal_reason,
                    published_wallet_ids_json,
                    active_follow_wallet_ids_json,
                    current_raw_top_wallet_ids_json,
                    audit_json,
                    shadow_signal_json
                 FROM discovery_wallet_freshness_history
                 ORDER BY captured_at DESC, capture_id DESC
                 LIMIT ?1",
            )
            .context("failed to prepare discovery wallet freshness history query")?;
        let mut rows = stmt
            .query(params![query_limit])
            .context("failed querying discovery wallet freshness history")?;
        let mut captures = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating discovery wallet freshness history rows")?
        {
            captures.push(read_discovery_wallet_freshness_capture_row(row)?);
        }
        Ok(captures)
    }

    fn load_discovery_wallet_freshness_capture(
        &self,
        capture_id: i64,
    ) -> Result<Option<DiscoveryWalletFreshnessCaptureRow>> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        self.conn
            .query_row(
                "SELECT
                    capture_id,
                    captured_at,
                    recent_cycles,
                    verdict,
                    reason,
                    publication_age_seconds,
                    raw_truth_sufficient,
                    raw_truth_reason,
                    shadow_signal_verdict,
                    shadow_signal_reason,
                    published_wallet_ids_json,
                    active_follow_wallet_ids_json,
                    current_raw_top_wallet_ids_json,
                    audit_json,
                    shadow_signal_json
                 FROM discovery_wallet_freshness_history
                 WHERE capture_id = ?1",
                params![capture_id],
                read_discovery_wallet_freshness_capture_row,
            )
            .optional()
            .context("failed loading discovery wallet freshness capture")
    }

    fn ensure_trusted_wallet_metrics_snapshots_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS trusted_wallet_metrics_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    source_snapshot_id TEXT,
                    source_window_start TEXT,
                    effective_window_start TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    trust_state TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_trusted_wallet_metrics_snapshots_effective_window_start
                ON trusted_wallet_metrics_snapshots(effective_window_start);
                CREATE INDEX IF NOT EXISTS
                    idx_trusted_wallet_metrics_snapshots_trust_state_effective_window_start
                ON trusted_wallet_metrics_snapshots(trust_state, effective_window_start DESC);
                CREATE INDEX IF NOT EXISTS
                    idx_trusted_wallet_metrics_snapshots_created_at
                ON trusted_wallet_metrics_snapshots(created_at DESC);",
            )
            .context("failed to ensure trusted wallet_metrics snapshots table exists")?;
        Ok(())
    }

    fn ensure_discovery_wallet_freshness_history_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_wallet_freshness_history (
                    capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    recent_cycles INTEGER NOT NULL,
                    verdict TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    publication_age_seconds INTEGER,
                    raw_truth_sufficient INTEGER NOT NULL,
                    raw_truth_reason TEXT NOT NULL,
                    shadow_signal_verdict TEXT NOT NULL,
                    shadow_signal_reason TEXT NOT NULL,
                    published_wallet_ids_json TEXT NOT NULL,
                    active_follow_wallet_ids_json TEXT NOT NULL,
                    current_raw_top_wallet_ids_json TEXT NOT NULL,
                    audit_json TEXT NOT NULL,
                    shadow_signal_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS
                    idx_discovery_wallet_freshness_history_captured_at
                ON discovery_wallet_freshness_history(captured_at DESC, capture_id DESC);",
            )
            .context("failed to ensure discovery_wallet_freshness_history table exists")?;
        Ok(())
    }

    fn ensure_discovery_recent_raw_restore_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_recent_raw_restore_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    journal_available INTEGER NOT NULL DEFAULT 0,
                    journal_replayed INTEGER NOT NULL DEFAULT 0,
                    required_window_start TEXT,
                    journal_covered_since TEXT,
                    journal_covered_through_cursor_ts TEXT,
                    journal_covered_through_cursor_slot INTEGER,
                    journal_covered_through_cursor_signature TEXT,
                    gap_fill_replayed INTEGER NOT NULL DEFAULT 0,
                    gap_fill_covered_since TEXT,
                    gap_fill_covered_through_cursor_ts TEXT,
                    gap_fill_covered_through_cursor_slot INTEGER,
                    gap_fill_covered_through_cursor_signature TEXT,
                    effective_covered_since TEXT,
                    effective_covered_through_cursor_ts TEXT,
                    effective_covered_through_cursor_slot INTEGER,
                    effective_covered_through_cursor_signature TEXT,
                    artifact_runtime_cursor_ts TEXT,
                    artifact_runtime_cursor_slot INTEGER,
                    artifact_runtime_cursor_signature TEXT,
                    journal_covers_artifact_cursor INTEGER NOT NULL DEFAULT 0,
                    raw_coverage_satisfied INTEGER NOT NULL DEFAULT 0,
                    gap_fill_replayed_rows INTEGER NOT NULL DEFAULT 0,
                    replayed_rows INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    replay_started_at TEXT,
                    replay_completed_at TEXT,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_recent_raw_restore_state table exists")?;
        let columns: HashSet<String> = {
            let mut stmt = self
                .conn
                .prepare("PRAGMA table_info(discovery_recent_raw_restore_state)")
                .context(
                    "failed to prepare discovery_recent_raw_restore_state column introspection",
                )?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .context("failed querying discovery_recent_raw_restore_state columns")?
                .collect::<rusqlite::Result<HashSet<String>>>()
                .context("failed collecting discovery_recent_raw_restore_state columns")?;
            columns
        };
        if !columns.contains("gap_fill_replayed") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_replayed INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context("failed adding discovery_recent_raw_restore_state.gap_fill_replayed")?;
        }
        if !columns.contains("gap_fill_covered_since") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_since TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_since",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_ts") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_ts TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_ts",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_slot") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_slot INTEGER",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_slot",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_signature") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_signature TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_signature",
                )?;
        }
        if !columns.contains("effective_covered_since") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_since TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_since",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_ts") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_ts TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_ts",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_slot") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_slot INTEGER",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_slot",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_signature") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_signature TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_signature",
                )?;
        }
        if !columns.contains("gap_fill_replayed_rows") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_replayed_rows INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_replayed_rows",
                )?;
        }
        Ok(())
    }

    fn ensure_discovery_strategy_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_strategy_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    trusted_selection_bootstrap_required INTEGER NOT NULL DEFAULT 0,
                    trusted_selection_reason TEXT NOT NULL DEFAULT '',
                    trusted_selection_state TEXT NOT NULL DEFAULT 'invalid',
                    active_trusted_snapshot_id TEXT,
                    active_trusted_snapshot_window_start TEXT,
                    last_trusted_bootstrap_source_kind TEXT,
                    last_trusted_bootstrap_at TEXT,
                    bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0,
                    bootstrap_degraded_reason TEXT,
                    bootstrap_degraded_armed_at TEXT,
                    publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
                    publication_reason TEXT NOT NULL DEFAULT '',
                    publication_last_published_at TEXT,
                    publication_last_published_window_start TEXT,
                    publication_scoring_source TEXT,
                    publication_wallet_ids_json TEXT,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_strategy_state table exists")?;
        let columns: HashSet<String> = {
            let mut stmt = self
                .conn
                .prepare("PRAGMA table_info(discovery_strategy_state)")
                .context("failed to prepare discovery_strategy_state column introspection")?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .context("failed querying discovery_strategy_state columns")?
                .collect::<rusqlite::Result<HashSet<String>>>()
                .context("failed collecting discovery_strategy_state columns")?;
            columns
        };
        if !columns.contains("trusted_selection_state") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN trusted_selection_state TEXT NOT NULL DEFAULT 'invalid'",
                    [],
                )
                .context("failed adding discovery_strategy_state.trusted_selection_state")?;
        }
        if !columns.contains("active_trusted_snapshot_id") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN active_trusted_snapshot_id TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.active_trusted_snapshot_id")?;
        }
        if !columns.contains("active_trusted_snapshot_window_start") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN active_trusted_snapshot_window_start TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.active_trusted_snapshot_window_start",
                )?;
        }
        if !columns.contains("last_trusted_bootstrap_source_kind") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN last_trusted_bootstrap_source_kind TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.last_trusted_bootstrap_source_kind",
                )?;
        }
        if !columns.contains("last_trusted_bootstrap_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN last_trusted_bootstrap_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.last_trusted_bootstrap_at")?;
        }
        if !columns.contains("bootstrap_degraded_active") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_active")?;
        }
        if !columns.contains("bootstrap_degraded_reason") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_reason TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_reason")?;
        }
        if !columns.contains("bootstrap_degraded_armed_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_armed_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_armed_at")?;
        }
        if !columns.contains("publication_runtime_mode") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed'",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_runtime_mode")?;
        }
        if !columns.contains("publication_reason") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_reason TEXT NOT NULL DEFAULT ''",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_reason")?;
        }
        if !columns.contains("publication_last_published_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_last_published_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_last_published_at")?;
        }
        if !columns.contains("publication_last_published_window_start") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_last_published_window_start TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.publication_last_published_window_start",
                )?;
        }
        if !columns.contains("publication_scoring_source") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_scoring_source TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_scoring_source")?;
        }
        if !columns.contains("publication_wallet_ids_json") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_wallet_ids_json TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_wallet_ids_json")?;
        }
        Ok(())
    }
}
