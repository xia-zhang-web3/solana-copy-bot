use super::{ExecutionDevnetDressRehearsalRow, ExecutionDevnetDressRehearsalWrite, SqliteStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, Row};

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {field_name} rfc3339 timestamp: {raw}"))
}

fn parse_bool(raw: i64, field_name: &str) -> Result<bool> {
    match raw {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(anyhow!("{field_name} must be 0 or 1, got {other}")),
    }
}

fn parse_optional_bool(raw: Option<i64>, field_name: &str) -> Result<Option<bool>> {
    raw.map(|value| parse_bool(value, field_name)).transpose()
}

fn parse_string_vec_json(raw: &str, field_name: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).with_context(|| format!("invalid {field_name} json"))
}

fn read_execution_devnet_dress_rehearsal_row(
    row: &Row<'_>,
) -> Result<ExecutionDevnetDressRehearsalRow> {
    let rehearsed_at_raw: String = row
        .get(1)
        .context("failed reading execution_devnet_dress_rehearsal_history.rehearsed_at")?;
    let blockers_json: String = row
        .get(31)
        .context("failed reading execution_devnet_dress_rehearsal_history.blockers_json")?;
    let warnings_json: String = row
        .get(32)
        .context("failed reading execution_devnet_dress_rehearsal_history.warnings_json")?;

    Ok(ExecutionDevnetDressRehearsalRow {
        rehearsal_id: row
            .get(0)
            .context("failed reading execution_devnet_dress_rehearsal_history.rehearsal_id")?,
        rehearsed_at: parse_rfc3339_utc(
            rehearsed_at_raw.as_str(),
            "execution_devnet_dress_rehearsal_history.rehearsed_at",
        )?,
        target_environment: row
            .get(2)
            .context("failed reading execution_devnet_dress_rehearsal_history.target_environment")?,
        config_env: row
            .get(3)
            .context("failed reading execution_devnet_dress_rehearsal_history.config_env")?,
        execution_mode: row
            .get(4)
            .context("failed reading execution_devnet_dress_rehearsal_history.execution_mode")?,
        execution_enabled: parse_bool(
            row.get(5)
                .context("failed reading execution_devnet_dress_rehearsal_history.execution_enabled")?,
            "execution_devnet_dress_rehearsal_history.execution_enabled",
        )?,
        route: row
            .get(6)
            .context("failed reading execution_devnet_dress_rehearsal_history.route")?,
        token: row
            .get(7)
            .context("failed reading execution_devnet_dress_rehearsal_history.token")?,
        side: row
            .get(8)
            .context("failed reading execution_devnet_dress_rehearsal_history.side")?,
        notional_sol: row
            .get(9)
            .context("failed reading execution_devnet_dress_rehearsal_history.notional_sol")?,
        readiness_verdict: row
            .get(10)
            .context("failed reading execution_devnet_dress_rehearsal_history.readiness_verdict")?,
        readiness_reason: row
            .get(11)
            .context("failed reading execution_devnet_dress_rehearsal_history.readiness_reason")?,
        dry_run_verdict: row
            .get(12)
            .context("failed reading execution_devnet_dress_rehearsal_history.dry_run_verdict")?,
        dry_run_reason: row
            .get(13)
            .context("failed reading execution_devnet_dress_rehearsal_history.dry_run_reason")?,
        tiny_live_policy_verdict: row.get(14).context(
            "failed reading execution_devnet_dress_rehearsal_history.tiny_live_policy_verdict",
        )?,
        tiny_live_policy_reason: row.get(15).context(
            "failed reading execution_devnet_dress_rehearsal_history.tiny_live_policy_reason",
        )?,
        tiny_live_policy_bounded: parse_bool(
            row.get(16).context(
                "failed reading execution_devnet_dress_rehearsal_history.tiny_live_policy_bounded",
            )?,
            "execution_devnet_dress_rehearsal_history.tiny_live_policy_bounded",
        )?,
        signer_pubkey_configured: parse_bool(
            row.get(17).context(
                "failed reading execution_devnet_dress_rehearsal_history.signer_pubkey_configured",
            )?,
            "execution_devnet_dress_rehearsal_history.signer_pubkey_configured",
        )?,
        config_valid: parse_bool(
            row.get(18)
                .context("failed reading execution_devnet_dress_rehearsal_history.config_valid")?,
            "execution_devnet_dress_rehearsal_history.config_valid",
        )?,
        connectivity_valid: parse_bool(
            row.get(19).context(
                "failed reading execution_devnet_dress_rehearsal_history.connectivity_valid",
            )?,
            "execution_devnet_dress_rehearsal_history.connectivity_valid",
        )?,
        adapter_contract_valid: parse_bool(
            row.get(20).context(
                "failed reading execution_devnet_dress_rehearsal_history.adapter_contract_valid",
            )?,
            "execution_devnet_dress_rehearsal_history.adapter_contract_valid",
        )?,
        policy_contract_valid: parse_bool(
            row.get(21).context(
                "failed reading execution_devnet_dress_rehearsal_history.policy_contract_valid",
            )?,
            "execution_devnet_dress_rehearsal_history.policy_contract_valid",
        )?,
        route_contract_valid: parse_bool(
            row.get(22).context(
                "failed reading execution_devnet_dress_rehearsal_history.route_contract_valid",
            )?,
            "execution_devnet_dress_rehearsal_history.route_contract_valid",
        )?,
        ready_for_dry_run: parse_bool(
            row.get(23).context(
                "failed reading execution_devnet_dress_rehearsal_history.ready_for_dry_run",
            )?,
            "execution_devnet_dress_rehearsal_history.ready_for_dry_run",
        )?,
        would_be_admissible_for_later_tiny_live: parse_bool(
            row.get(24).context(
                "failed reading execution_devnet_dress_rehearsal_history.would_be_admissible_for_later_tiny_live",
            )?,
            "execution_devnet_dress_rehearsal_history.would_be_admissible_for_later_tiny_live",
        )?,
        rpc_preconditions_valid: parse_bool(
            row.get(25).context(
                "failed reading execution_devnet_dress_rehearsal_history.rpc_preconditions_valid",
            )?,
            "execution_devnet_dress_rehearsal_history.rpc_preconditions_valid",
        )?,
        adapter_result_classification: row.get(26).context(
            "failed reading execution_devnet_dress_rehearsal_history.adapter_result_classification",
        )?,
        adapter_accepted: parse_optional_bool(
            row.get(27).context(
                "failed reading execution_devnet_dress_rehearsal_history.adapter_accepted",
            )?,
            "execution_devnet_dress_rehearsal_history.adapter_accepted",
        )?,
        policy_echo_present: parse_bool(
            row.get(28).context(
                "failed reading execution_devnet_dress_rehearsal_history.policy_echo_present",
            )?,
            "execution_devnet_dress_rehearsal_history.policy_echo_present",
        )?,
        route_echo_present: parse_bool(
            row.get(29).context(
                "failed reading execution_devnet_dress_rehearsal_history.route_echo_present",
            )?,
            "execution_devnet_dress_rehearsal_history.route_echo_present",
        )?,
        contract_version_echo_present: parse_bool(
            row.get(30).context(
                "failed reading execution_devnet_dress_rehearsal_history.contract_version_echo_present",
            )?,
            "execution_devnet_dress_rehearsal_history.contract_version_echo_present",
        )?,
        verdict: row
            .get(33)
            .context("failed reading execution_devnet_dress_rehearsal_history.verdict")?,
        reason: row
            .get(34)
            .context("failed reading execution_devnet_dress_rehearsal_history.reason")?,
        blockers: parse_string_vec_json(
            blockers_json.as_str(),
            "execution_devnet_dress_rehearsal_history.blockers_json",
        )?,
        warnings: parse_string_vec_json(
            warnings_json.as_str(),
            "execution_devnet_dress_rehearsal_history.warnings_json",
        )?,
        rehearsal_json: row
            .get(35)
            .context("failed reading execution_devnet_dress_rehearsal_history.rehearsal_json")?,
    })
}

impl SqliteStore {
    pub fn append_execution_devnet_dress_rehearsal(
        &self,
        rehearsal: &ExecutionDevnetDressRehearsalWrite,
    ) -> Result<ExecutionDevnetDressRehearsalRow> {
        self.ensure_execution_devnet_dress_rehearsal_history_table()?;
        let blockers_json = serde_json::to_string(&rehearsal.blockers)
            .context("failed serializing execution devnet dress rehearsal blockers")?;
        let warnings_json = serde_json::to_string(&rehearsal.warnings)
            .context("failed serializing execution devnet dress rehearsal warnings")?;
        let rehearsal_id = self.conn.execute(
            "INSERT INTO execution_devnet_dress_rehearsal_history(
                rehearsed_at,
                target_environment,
                config_env,
                execution_mode,
                execution_enabled,
                route,
                token,
                side,
                notional_sol,
                readiness_verdict,
                readiness_reason,
                dry_run_verdict,
                dry_run_reason,
                tiny_live_policy_verdict,
                tiny_live_policy_reason,
                tiny_live_policy_bounded,
                signer_pubkey_configured,
                config_valid,
                connectivity_valid,
                adapter_contract_valid,
                policy_contract_valid,
                route_contract_valid,
                ready_for_dry_run,
                would_be_admissible_for_later_tiny_live,
                rpc_preconditions_valid,
                adapter_result_classification,
                adapter_accepted,
                policy_echo_present,
                route_echo_present,
                contract_version_echo_present,
                blockers_json,
                warnings_json,
                verdict,
                reason,
                rehearsal_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35)",
            params![
                rehearsal.rehearsed_at.to_rfc3339(),
                rehearsal.target_environment,
                rehearsal.config_env,
                rehearsal.execution_mode,
                i64::from(rehearsal.execution_enabled),
                rehearsal.route,
                rehearsal.token,
                rehearsal.side,
                rehearsal.notional_sol,
                rehearsal.readiness_verdict,
                rehearsal.readiness_reason,
                rehearsal.dry_run_verdict,
                rehearsal.dry_run_reason,
                rehearsal.tiny_live_policy_verdict,
                rehearsal.tiny_live_policy_reason,
                i64::from(rehearsal.tiny_live_policy_bounded),
                i64::from(rehearsal.signer_pubkey_configured),
                i64::from(rehearsal.config_valid),
                i64::from(rehearsal.connectivity_valid),
                i64::from(rehearsal.adapter_contract_valid),
                i64::from(rehearsal.policy_contract_valid),
                i64::from(rehearsal.route_contract_valid),
                i64::from(rehearsal.ready_for_dry_run),
                i64::from(rehearsal.would_be_admissible_for_later_tiny_live),
                i64::from(rehearsal.rpc_preconditions_valid),
                rehearsal.adapter_result_classification,
                rehearsal.adapter_accepted.map(i64::from),
                i64::from(rehearsal.policy_echo_present),
                i64::from(rehearsal.route_echo_present),
                i64::from(rehearsal.contract_version_echo_present),
                blockers_json,
                warnings_json,
                rehearsal.verdict,
                rehearsal.reason,
                rehearsal.rehearsal_json,
            ],
        )
        .context("failed inserting execution_devnet_dress_rehearsal_history row")?;
        let rehearsal_id = self.conn.last_insert_rowid().max(rehearsal_id as i64);
        self.get_execution_devnet_dress_rehearsal(rehearsal_id)?
            .ok_or_else(|| anyhow!("inserted execution devnet dress rehearsal row not found"))
    }

    pub fn list_execution_devnet_dress_rehearsals(
        &self,
        limit: usize,
    ) -> Result<Vec<ExecutionDevnetDressRehearsalRow>> {
        self.ensure_execution_devnet_dress_rehearsal_history_table()?;
        let limit = limit.max(1) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    rehearsal_id,
                    rehearsed_at,
                    target_environment,
                    config_env,
                    execution_mode,
                    execution_enabled,
                    route,
                    token,
                    side,
                    notional_sol,
                    readiness_verdict,
                    readiness_reason,
                    dry_run_verdict,
                    dry_run_reason,
                    tiny_live_policy_verdict,
                    tiny_live_policy_reason,
                    tiny_live_policy_bounded,
                    signer_pubkey_configured,
                    config_valid,
                    connectivity_valid,
                    adapter_contract_valid,
                    policy_contract_valid,
                    route_contract_valid,
                    ready_for_dry_run,
                    would_be_admissible_for_later_tiny_live,
                    rpc_preconditions_valid,
                    adapter_result_classification,
                    adapter_accepted,
                    policy_echo_present,
                    route_echo_present,
                    contract_version_echo_present,
                    blockers_json,
                    warnings_json,
                    verdict,
                    reason,
                    rehearsal_json
                 FROM execution_devnet_dress_rehearsal_history
                 ORDER BY rehearsed_at DESC, rehearsal_id DESC
                 LIMIT ?1",
            )
            .context("failed preparing execution devnet dress rehearsal history query")?;
        let rows = stmt
            .query_map(params![limit], |row| {
                read_execution_devnet_dress_rehearsal_row(row).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::other(error.to_string())),
                    )
                })
            })
            .context("failed querying execution devnet dress rehearsal history")?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.context("failed decoding execution devnet dress rehearsal row")?);
        }
        Ok(result)
    }

    fn get_execution_devnet_dress_rehearsal(
        &self,
        rehearsal_id: i64,
    ) -> Result<Option<ExecutionDevnetDressRehearsalRow>> {
        self.ensure_execution_devnet_dress_rehearsal_history_table()?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    rehearsal_id,
                    rehearsed_at,
                    target_environment,
                    config_env,
                    execution_mode,
                    execution_enabled,
                    route,
                    token,
                    side,
                    notional_sol,
                    readiness_verdict,
                    readiness_reason,
                    dry_run_verdict,
                    dry_run_reason,
                    tiny_live_policy_verdict,
                    tiny_live_policy_reason,
                    tiny_live_policy_bounded,
                    signer_pubkey_configured,
                    config_valid,
                    connectivity_valid,
                    adapter_contract_valid,
                    policy_contract_valid,
                    route_contract_valid,
                    ready_for_dry_run,
                    would_be_admissible_for_later_tiny_live,
                    rpc_preconditions_valid,
                    adapter_result_classification,
                    adapter_accepted,
                    policy_echo_present,
                    route_echo_present,
                    contract_version_echo_present,
                    blockers_json,
                    warnings_json,
                    verdict,
                    reason,
                    rehearsal_json
                 FROM execution_devnet_dress_rehearsal_history
                 WHERE rehearsal_id = ?1",
            )
            .context("failed preparing execution devnet dress rehearsal row query")?;
        stmt.query_row(params![rehearsal_id], |row| {
            read_execution_devnet_dress_rehearsal_row(row).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::other(error.to_string())),
                )
            })
        })
        .optional()
        .context("failed loading execution devnet dress rehearsal row")
    }

    fn ensure_execution_devnet_dress_rehearsal_history_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS execution_devnet_dress_rehearsal_history (
                    rehearsal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rehearsed_at TEXT NOT NULL,
                    target_environment TEXT NOT NULL,
                    config_env TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    execution_enabled INTEGER NOT NULL,
                    route TEXT NOT NULL,
                    token TEXT NOT NULL,
                    side TEXT NOT NULL,
                    notional_sol REAL NOT NULL,
                    readiness_verdict TEXT NOT NULL,
                    readiness_reason TEXT NOT NULL,
                    dry_run_verdict TEXT,
                    dry_run_reason TEXT,
                    tiny_live_policy_verdict TEXT NOT NULL,
                    tiny_live_policy_reason TEXT NOT NULL,
                    tiny_live_policy_bounded INTEGER NOT NULL,
                    signer_pubkey_configured INTEGER NOT NULL,
                    config_valid INTEGER NOT NULL,
                    connectivity_valid INTEGER NOT NULL,
                    adapter_contract_valid INTEGER NOT NULL,
                    policy_contract_valid INTEGER NOT NULL,
                    route_contract_valid INTEGER NOT NULL,
                    ready_for_dry_run INTEGER NOT NULL,
                    would_be_admissible_for_later_tiny_live INTEGER NOT NULL,
                    rpc_preconditions_valid INTEGER NOT NULL,
                    adapter_result_classification TEXT,
                    adapter_accepted INTEGER,
                    policy_echo_present INTEGER NOT NULL,
                    route_echo_present INTEGER NOT NULL,
                    contract_version_echo_present INTEGER NOT NULL,
                    blockers_json TEXT NOT NULL,
                    warnings_json TEXT NOT NULL,
                    verdict TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    rehearsal_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_execution_devnet_dress_rehearsal_history_rehearsed_at
                ON execution_devnet_dress_rehearsal_history(rehearsed_at DESC, rehearsal_id DESC);",
            )
            .context("failed to ensure execution_devnet_dress_rehearsal_history table exists")?;
        Ok(())
    }
}
