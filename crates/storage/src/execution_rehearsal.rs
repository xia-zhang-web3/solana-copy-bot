use super::{ExecutionDryRunRehearsalRow, ExecutionDryRunRehearsalWrite, SqliteStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Row};

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {field_name} rfc3339 timestamp: {raw}"))
}

fn parse_optional_u64(raw: Option<i64>, field_name: &str) -> Result<Option<u64>> {
    match raw {
        Some(value) if value < 0 => Err(anyhow!("{} must be non-negative", field_name)),
        Some(value) => Ok(Some(value as u64)),
        None => Ok(None),
    }
}

fn parse_bool(raw: i64, field_name: &str) -> Result<bool> {
    match raw {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(anyhow!("{} must be 0 or 1, got {}", field_name, other)),
    }
}

fn parse_optional_bool(raw: Option<i64>, field_name: &str) -> Result<Option<bool>> {
    raw.map(|value| parse_bool(value, field_name)).transpose()
}

fn parse_string_vec_json(raw: &str, field_name: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).with_context(|| format!("invalid {field_name} json"))
}

fn read_execution_dry_run_rehearsal_row(row: &Row<'_>) -> Result<ExecutionDryRunRehearsalRow> {
    let rehearsed_at_raw: String = row
        .get(1)
        .context("failed reading execution_dry_run_rehearsal_history.rehearsed_at")?;
    let blockers_json: String = row
        .get(31)
        .context("failed reading execution_dry_run_rehearsal_history.blockers_json")?;
    let warnings_json: String = row
        .get(32)
        .context("failed reading execution_dry_run_rehearsal_history.warnings_json")?;

    Ok(ExecutionDryRunRehearsalRow {
        rehearsal_id: row
            .get(0)
            .context("failed reading execution_dry_run_rehearsal_history.rehearsal_id")?,
        rehearsed_at: parse_rfc3339_utc(
            rehearsed_at_raw.as_str(),
            "execution_dry_run_rehearsal_history.rehearsed_at",
        )?,
        execution_mode: row
            .get(2)
            .context("failed reading execution_dry_run_rehearsal_history.execution_mode")?,
        execution_enabled: parse_bool(
            row.get(3)
                .context("failed reading execution_dry_run_rehearsal_history.execution_enabled")?,
            "execution_dry_run_rehearsal_history.execution_enabled",
        )?,
        route: row
            .get(4)
            .context("failed reading execution_dry_run_rehearsal_history.route")?,
        token: row
            .get(5)
            .context("failed reading execution_dry_run_rehearsal_history.token")?,
        notional_sol: row
            .get(6)
            .context("failed reading execution_dry_run_rehearsal_history.notional_sol")?,
        signer_pubkey_configured: parse_bool(
            row.get(7).context(
                "failed reading execution_dry_run_rehearsal_history.signer_pubkey_configured",
            )?,
            "execution_dry_run_rehearsal_history.signer_pubkey_configured",
        )?,
        config_valid: parse_bool(
            row.get(8)
                .context("failed reading execution_dry_run_rehearsal_history.config_valid")?,
            "execution_dry_run_rehearsal_history.config_valid",
        )?,
        connectivity_valid: parse_bool(
            row.get(9).context(
                "failed reading execution_dry_run_rehearsal_history.connectivity_valid",
            )?,
            "execution_dry_run_rehearsal_history.connectivity_valid",
        )?,
        adapter_contract_valid: parse_bool(
            row.get(10).context(
                "failed reading execution_dry_run_rehearsal_history.adapter_contract_valid",
            )?,
            "execution_dry_run_rehearsal_history.adapter_contract_valid",
        )?,
        policy_contract_valid: parse_bool(
            row.get(11).context(
                "failed reading execution_dry_run_rehearsal_history.policy_contract_valid",
            )?,
            "execution_dry_run_rehearsal_history.policy_contract_valid",
        )?,
        route_contract_valid: parse_bool(
            row.get(12).context(
                "failed reading execution_dry_run_rehearsal_history.route_contract_valid",
            )?,
            "execution_dry_run_rehearsal_history.route_contract_valid",
        )?,
        ready_for_dry_run: parse_bool(
            row.get(13).context(
                "failed reading execution_dry_run_rehearsal_history.ready_for_dry_run",
            )?,
            "execution_dry_run_rehearsal_history.ready_for_dry_run",
        )?,
        would_be_admissible_for_later_tiny_live: parse_bool(
            row.get(14).context(
                "failed reading execution_dry_run_rehearsal_history.would_be_admissible_for_later_tiny_live",
            )?,
            "execution_dry_run_rehearsal_history.would_be_admissible_for_later_tiny_live",
        )?,
        rpc_preconditions_valid: parse_bool(
            row.get(15).context(
                "failed reading execution_dry_run_rehearsal_history.rpc_preconditions_valid",
            )?,
            "execution_dry_run_rehearsal_history.rpc_preconditions_valid",
        )?,
        rpc_slot: parse_optional_u64(
            row.get(16)
                .context("failed reading execution_dry_run_rehearsal_history.rpc_slot")?,
            "execution_dry_run_rehearsal_history.rpc_slot",
        )?,
        rpc_blockhash: row
            .get(17)
            .context("failed reading execution_dry_run_rehearsal_history.rpc_blockhash")?,
        rpc_signer_balance_lamports: parse_optional_u64(
            row.get(18).context(
                "failed reading execution_dry_run_rehearsal_history.rpc_signer_balance_lamports",
            )?,
            "execution_dry_run_rehearsal_history.rpc_signer_balance_lamports",
        )?,
        adapter_result_classification: row
            .get(19)
            .context("failed reading execution_dry_run_rehearsal_history.adapter_result_classification")?,
        adapter_accepted: parse_optional_bool(
            row.get(20).context(
                "failed reading execution_dry_run_rehearsal_history.adapter_accepted",
            )?,
            "execution_dry_run_rehearsal_history.adapter_accepted",
        )?,
        adapter_detail: row
            .get(21)
            .context("failed reading execution_dry_run_rehearsal_history.adapter_detail")?,
        policy_echo_present: parse_bool(
            row.get(22).context(
                "failed reading execution_dry_run_rehearsal_history.policy_echo_present",
            )?,
            "execution_dry_run_rehearsal_history.policy_echo_present",
        )?,
        route_echo_present: parse_bool(
            row.get(23).context(
                "failed reading execution_dry_run_rehearsal_history.route_echo_present",
            )?,
            "execution_dry_run_rehearsal_history.route_echo_present",
        )?,
        contract_version_echo_present: parse_bool(
            row.get(24).context(
                "failed reading execution_dry_run_rehearsal_history.contract_version_echo_present",
            )?,
            "execution_dry_run_rehearsal_history.contract_version_echo_present",
        )?,
        response_slippage_bps: row.get(25).context(
            "failed reading execution_dry_run_rehearsal_history.response_slippage_bps",
        )?,
        response_tip_lamports: parse_optional_u64(
            row.get(26).context(
                "failed reading execution_dry_run_rehearsal_history.response_tip_lamports",
            )?,
            "execution_dry_run_rehearsal_history.response_tip_lamports",
        )?,
        response_compute_unit_limit: parse_optional_u64(
            row.get(27).context(
                "failed reading execution_dry_run_rehearsal_history.response_compute_unit_limit",
            )?,
            "execution_dry_run_rehearsal_history.response_compute_unit_limit",
        )?,
        response_compute_unit_price_micro_lamports: parse_optional_u64(
            row.get(28).context(
                "failed reading execution_dry_run_rehearsal_history.response_compute_unit_price_micro_lamports",
            )?,
            "execution_dry_run_rehearsal_history.response_compute_unit_price_micro_lamports",
        )?,
        verdict: row
            .get(29)
            .context("failed reading execution_dry_run_rehearsal_history.verdict")?,
        reason: row
            .get(30)
            .context("failed reading execution_dry_run_rehearsal_history.reason")?,
        blockers: parse_string_vec_json(
            blockers_json.as_str(),
            "execution_dry_run_rehearsal_history.blockers_json",
        )?,
        warnings: parse_string_vec_json(
            warnings_json.as_str(),
            "execution_dry_run_rehearsal_history.warnings_json",
        )?,
        rehearsal_json: row
            .get(33)
            .context("failed reading execution_dry_run_rehearsal_history.rehearsal_json")?,
    })
}

impl SqliteStore {
    pub fn append_execution_dry_run_rehearsal(
        &self,
        rehearsal: &ExecutionDryRunRehearsalWrite,
    ) -> Result<ExecutionDryRunRehearsalRow> {
        self.ensure_execution_dry_run_rehearsal_history_table()?;
        let blockers_json = serde_json::to_string(&rehearsal.blockers)
            .context("failed serializing execution dry-run rehearsal blockers")?;
        let warnings_json = serde_json::to_string(&rehearsal.warnings)
            .context("failed serializing execution dry-run rehearsal warnings")?;
        let rehearsal_id = self
            .execute_with_retry_result(|conn| {
                conn.execute(
                    "INSERT INTO execution_dry_run_rehearsal_history(
                        rehearsed_at,
                        execution_mode,
                        execution_enabled,
                        route,
                        token,
                        notional_sol,
                        signer_pubkey_configured,
                        config_valid,
                        connectivity_valid,
                        adapter_contract_valid,
                        policy_contract_valid,
                        route_contract_valid,
                        ready_for_dry_run,
                        would_be_admissible_for_later_tiny_live,
                        rpc_preconditions_valid,
                        rpc_slot,
                        rpc_blockhash,
                        rpc_signer_balance_lamports,
                        adapter_result_classification,
                        adapter_accepted,
                        adapter_detail,
                        policy_echo_present,
                        route_echo_present,
                        contract_version_echo_present,
                        response_slippage_bps,
                        response_tip_lamports,
                        response_compute_unit_limit,
                        response_compute_unit_price_micro_lamports,
                        verdict,
                        reason,
                        blockers_json,
                        warnings_json,
                        rehearsal_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33)",
                    params![
                        rehearsal.rehearsed_at.to_rfc3339(),
                        rehearsal.execution_mode,
                        if rehearsal.execution_enabled { 1 } else { 0 },
                        rehearsal.route,
                        rehearsal.token,
                        rehearsal.notional_sol,
                        if rehearsal.signer_pubkey_configured { 1 } else { 0 },
                        if rehearsal.config_valid { 1 } else { 0 },
                        if rehearsal.connectivity_valid { 1 } else { 0 },
                        if rehearsal.adapter_contract_valid { 1 } else { 0 },
                        if rehearsal.policy_contract_valid { 1 } else { 0 },
                        if rehearsal.route_contract_valid { 1 } else { 0 },
                        if rehearsal.ready_for_dry_run { 1 } else { 0 },
                        if rehearsal.would_be_admissible_for_later_tiny_live { 1 } else { 0 },
                        if rehearsal.rpc_preconditions_valid { 1 } else { 0 },
                        rehearsal
                            .rpc_slot
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        rehearsal.rpc_blockhash,
                        rehearsal
                            .rpc_signer_balance_lamports
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        rehearsal.adapter_result_classification,
                        rehearsal.adapter_accepted.map(|value| if value { 1 } else { 0 }),
                        rehearsal.adapter_detail,
                        if rehearsal.policy_echo_present { 1 } else { 0 },
                        if rehearsal.route_echo_present { 1 } else { 0 },
                        if rehearsal.contract_version_echo_present { 1 } else { 0 },
                        rehearsal.response_slippage_bps,
                        rehearsal
                            .response_tip_lamports
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        rehearsal
                            .response_compute_unit_limit
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        rehearsal
                            .response_compute_unit_price_micro_lamports
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        rehearsal.verdict,
                        rehearsal.reason,
                        blockers_json,
                        warnings_json,
                        rehearsal.rehearsal_json,
                    ],
                )?;
                Ok(conn.last_insert_rowid())
            })
            .context("failed appending execution dry-run rehearsal")?;
        self.load_execution_dry_run_rehearsal(rehearsal_id)?
            .ok_or_else(|| anyhow!("execution dry-run rehearsal disappeared after insert"))
    }

    pub fn list_execution_dry_run_rehearsals(
        &self,
        limit: usize,
    ) -> Result<Vec<ExecutionDryRunRehearsalRow>> {
        self.ensure_execution_dry_run_rehearsal_history_table()?;
        let query_limit = limit.max(1).min(i64::MAX as usize) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    rehearsal_id,
                    rehearsed_at,
                    execution_mode,
                    execution_enabled,
                    route,
                    token,
                    notional_sol,
                    signer_pubkey_configured,
                    config_valid,
                    connectivity_valid,
                    adapter_contract_valid,
                    policy_contract_valid,
                    route_contract_valid,
                    ready_for_dry_run,
                    would_be_admissible_for_later_tiny_live,
                    rpc_preconditions_valid,
                    rpc_slot,
                    rpc_blockhash,
                    rpc_signer_balance_lamports,
                    adapter_result_classification,
                    adapter_accepted,
                    adapter_detail,
                    policy_echo_present,
                    route_echo_present,
                    contract_version_echo_present,
                    response_slippage_bps,
                    response_tip_lamports,
                    response_compute_unit_limit,
                    response_compute_unit_price_micro_lamports,
                    verdict,
                    reason,
                    blockers_json,
                    warnings_json,
                    rehearsal_json
                 FROM execution_dry_run_rehearsal_history
                 ORDER BY rehearsed_at DESC, rehearsal_id DESC
                 LIMIT ?1",
            )
            .context("failed to prepare execution dry-run rehearsal history query")?;
        let mut rows = stmt
            .query(params![query_limit])
            .context("failed querying execution dry-run rehearsal history")?;
        let mut rehearsals = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating execution dry-run rehearsal history rows")?
        {
            rehearsals.push(read_execution_dry_run_rehearsal_row(row)?);
        }
        Ok(rehearsals)
    }

    fn load_execution_dry_run_rehearsal(
        &self,
        rehearsal_id: i64,
    ) -> Result<Option<ExecutionDryRunRehearsalRow>> {
        self.ensure_execution_dry_run_rehearsal_history_table()?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    rehearsal_id,
                    rehearsed_at,
                    execution_mode,
                    execution_enabled,
                    route,
                    token,
                    notional_sol,
                    signer_pubkey_configured,
                    config_valid,
                    connectivity_valid,
                    adapter_contract_valid,
                    policy_contract_valid,
                    route_contract_valid,
                    ready_for_dry_run,
                    would_be_admissible_for_later_tiny_live,
                    rpc_preconditions_valid,
                    rpc_slot,
                    rpc_blockhash,
                    rpc_signer_balance_lamports,
                    adapter_result_classification,
                    adapter_accepted,
                    adapter_detail,
                    policy_echo_present,
                    route_echo_present,
                    contract_version_echo_present,
                    response_slippage_bps,
                    response_tip_lamports,
                    response_compute_unit_limit,
                    response_compute_unit_price_micro_lamports,
                    verdict,
                    reason,
                    blockers_json,
                    warnings_json,
                    rehearsal_json
                 FROM execution_dry_run_rehearsal_history
                 WHERE rehearsal_id = ?1",
            )
            .context("failed to prepare execution dry-run rehearsal load query")?;
        let mut rows = stmt
            .query(params![rehearsal_id])
            .context("failed querying execution dry-run rehearsal")?;
        match rows
            .next()
            .context("failed iterating execution dry-run rehearsal rows")?
        {
            Some(row) => Ok(Some(read_execution_dry_run_rehearsal_row(row)?)),
            None => Ok(None),
        }
    }

    fn ensure_execution_dry_run_rehearsal_history_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS execution_dry_run_rehearsal_history (
                    rehearsal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rehearsed_at TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    execution_enabled INTEGER NOT NULL,
                    route TEXT NOT NULL,
                    token TEXT NOT NULL,
                    notional_sol REAL NOT NULL,
                    signer_pubkey_configured INTEGER NOT NULL,
                    config_valid INTEGER NOT NULL,
                    connectivity_valid INTEGER NOT NULL,
                    adapter_contract_valid INTEGER NOT NULL,
                    policy_contract_valid INTEGER NOT NULL,
                    route_contract_valid INTEGER NOT NULL,
                    ready_for_dry_run INTEGER NOT NULL,
                    would_be_admissible_for_later_tiny_live INTEGER NOT NULL,
                    rpc_preconditions_valid INTEGER NOT NULL,
                    rpc_slot INTEGER,
                    rpc_blockhash TEXT,
                    rpc_signer_balance_lamports INTEGER,
                    adapter_result_classification TEXT NOT NULL,
                    adapter_accepted INTEGER,
                    adapter_detail TEXT NOT NULL,
                    policy_echo_present INTEGER NOT NULL,
                    route_echo_present INTEGER NOT NULL,
                    contract_version_echo_present INTEGER NOT NULL,
                    response_slippage_bps REAL,
                    response_tip_lamports INTEGER,
                    response_compute_unit_limit INTEGER,
                    response_compute_unit_price_micro_lamports INTEGER,
                    verdict TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    blockers_json TEXT NOT NULL,
                    warnings_json TEXT NOT NULL,
                    rehearsal_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS
                    idx_execution_dry_run_rehearsal_history_rehearsed_at
                ON execution_dry_run_rehearsal_history(rehearsed_at DESC, rehearsal_id DESC);",
            )
            .context("failed to ensure execution_dry_run_rehearsal_history table exists")?;
        Ok(())
    }
}
