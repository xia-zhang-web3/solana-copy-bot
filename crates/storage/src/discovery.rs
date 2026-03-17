use super::{
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode,
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate,
    FollowlistUpdateResult, PersistedWalletMetricSnapshotRow, SqliteStore,
    StartupTrustedSelectionGateStatus, TrustedSelectionState, TrustedSnapshotSourceKind,
    TrustedWalletMetricsSnapshotRow, TrustedWalletMetricsSnapshotWrite, WalletActivityDayRow,
    WalletMetricRow, WalletUpsertRow, DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};

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
        let raw = self
            .conn
            .query_row(
                "SELECT
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
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
                        row.get::<_, String>(5)?,
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
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
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
                    updated_at = excluded.updated_at",
                params![
                    update.runtime_mode.as_str(),
                    &update.reason,
                    update.last_published_at.map(|ts| ts.to_rfc3339()),
                    update
                        .last_published_window_start
                        .map(canonical_wallet_metrics_window_start),
                    update.published_scoring_source.as_deref(),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery publication state")?;
        Ok(())
    }

    pub fn recent_published_follow_wallets(
        &self,
        now: DateTime<Utc>,
        max_age: chrono::Duration,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
    ) -> Result<Option<HashSet<String>>> {
        let Some(state) = self.discovery_publication_state()? else {
            return Ok(None);
        };
        if state.runtime_mode == DiscoveryRuntimeMode::FailClosed {
            return Ok(None);
        }
        let Some(last_published_at) = state.last_published_at else {
            return Ok(None);
        };
        if now.signed_duration_since(last_published_at) > max_age {
            return Ok(None);
        }
        if !state.has_valid_recent_published_universe(
            now,
            scoring_window_days,
            metric_snapshot_interval_seconds,
        ) {
            return Ok(None);
        }
        let active_wallets = self.list_active_follow_wallets()?;
        if active_wallets.is_empty() {
            return Ok(None);
        }
        Ok(Some(active_wallets))
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
                    publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
                    publication_reason TEXT NOT NULL DEFAULT '',
                    publication_last_published_at TEXT,
                    publication_last_published_window_start TEXT,
                    publication_scoring_source TEXT,
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
        Ok(())
    }
}
