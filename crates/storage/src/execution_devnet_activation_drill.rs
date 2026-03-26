use super::{ExecutionDevnetActivationDrillRow, ExecutionDevnetActivationDrillWrite, SqliteStore};
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

fn parse_string_vec_json(raw: &str, field_name: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).with_context(|| format!("invalid {field_name} json"))
}

fn read_execution_devnet_activation_drill_row(
    row: &Row<'_>,
) -> Result<ExecutionDevnetActivationDrillRow> {
    let drilled_at_raw: String = row
        .get(1)
        .context("failed reading execution_devnet_activation_drill_history.drilled_at")?;
    let blockers_json: String = row
        .get(28)
        .context("failed reading execution_devnet_activation_drill_history.blockers_json")?;
    let warnings_json: String = row
        .get(29)
        .context("failed reading execution_devnet_activation_drill_history.warnings_json")?;

    Ok(ExecutionDevnetActivationDrillRow {
        drill_id: row
            .get(0)
            .context("failed reading execution_devnet_activation_drill_history.drill_id")?,
        drilled_at: parse_rfc3339_utc(
            drilled_at_raw.as_str(),
            "execution_devnet_activation_drill_history.drilled_at",
        )?,
        target_environment: row.get(2).context(
            "failed reading execution_devnet_activation_drill_history.target_environment",
        )?,
        config_env: row
            .get(3)
            .context("failed reading execution_devnet_activation_drill_history.config_env")?,
        source_config_path: row.get(4).context(
            "failed reading execution_devnet_activation_drill_history.source_config_path",
        )?,
        execution_enabled_source: parse_bool(
            row.get(5).context(
                "failed reading execution_devnet_activation_drill_history.execution_enabled_source",
            )?,
            "execution_devnet_activation_drill_history.execution_enabled_source",
        )?,
        route: row
            .get(6)
            .context("failed reading execution_devnet_activation_drill_history.route")?,
        token: row
            .get(7)
            .context("failed reading execution_devnet_activation_drill_history.token")?,
        side: row
            .get(8)
            .context("failed reading execution_devnet_activation_drill_history.side")?,
        notional_sol: row
            .get(9)
            .context("failed reading execution_devnet_activation_drill_history.notional_sol")?,
        launch_dossier_verdict: row.get(10).context(
            "failed reading execution_devnet_activation_drill_history.launch_dossier_verdict",
        )?,
        launch_dossier_reason: row.get(11).context(
            "failed reading execution_devnet_activation_drill_history.launch_dossier_reason",
        )?,
        pre_activation_gate_verdict: row.get(12).context(
            "failed reading execution_devnet_activation_drill_history.pre_activation_gate_verdict",
        )?,
        pre_activation_gate_reason: row.get(13).context(
            "failed reading execution_devnet_activation_drill_history.pre_activation_gate_reason",
        )?,
        tiny_live_policy_verdict: row.get(14).context(
            "failed reading execution_devnet_activation_drill_history.tiny_live_policy_verdict",
        )?,
        tiny_live_guardrail_verdict: row.get(15).context(
            "failed reading execution_devnet_activation_drill_history.tiny_live_guardrail_verdict",
        )?,
        tiny_live_policy_bounded: parse_bool(
            row.get(16).context(
                "failed reading execution_devnet_activation_drill_history.tiny_live_policy_bounded",
            )?,
            "execution_devnet_activation_drill_history.tiny_live_policy_bounded",
        )?,
        tiny_live_guardrails_bounded: parse_bool(
            row.get(17).context(
                "failed reading execution_devnet_activation_drill_history.tiny_live_guardrails_bounded",
            )?,
            "execution_devnet_activation_drill_history.tiny_live_guardrails_bounded",
        )?,
        activation_overlay_change_count: row.get(18).context(
            "failed reading execution_devnet_activation_drill_history.activation_overlay_change_count",
        )?,
        rollback_overlay_change_count: row.get(19).context(
            "failed reading execution_devnet_activation_drill_history.rollback_overlay_change_count",
        )?,
        activation_drill_verdict: row.get(20).context(
            "failed reading execution_devnet_activation_drill_history.activation_drill_verdict",
        )?,
        activation_drill_reason: row.get(21).context(
            "failed reading execution_devnet_activation_drill_history.activation_drill_reason",
        )?,
        activation_rehearsal_verdict: row.get(22).context(
            "failed reading execution_devnet_activation_drill_history.activation_rehearsal_verdict",
        )?,
        activation_rehearsal_reason: row.get(23).context(
            "failed reading execution_devnet_activation_drill_history.activation_rehearsal_reason",
        )?,
        rollback_drill_verdict: row.get(24).context(
            "failed reading execution_devnet_activation_drill_history.rollback_drill_verdict",
        )?,
        rollback_drill_reason: row.get(25).context(
            "failed reading execution_devnet_activation_drill_history.rollback_drill_reason",
        )?,
        activated_config_policy_bounded: parse_bool(
            row.get(26).context(
                "failed reading execution_devnet_activation_drill_history.activated_config_policy_bounded",
            )?,
            "execution_devnet_activation_drill_history.activated_config_policy_bounded",
        )?,
        activated_config_guardrails_bounded: parse_bool(
            row.get(27).context(
                "failed reading execution_devnet_activation_drill_history.activated_config_guardrails_bounded",
            )?,
            "execution_devnet_activation_drill_history.activated_config_guardrails_bounded",
        )?,
        rollback_restores_safe_mode: parse_bool(
            row.get(30).context(
                "failed reading execution_devnet_activation_drill_history.rollback_restores_safe_mode",
            )?,
            "execution_devnet_activation_drill_history.rollback_restores_safe_mode",
        )?,
        blockers: parse_string_vec_json(
            blockers_json.as_str(),
            "execution_devnet_activation_drill_history.blockers_json",
        )?,
        warnings: parse_string_vec_json(
            warnings_json.as_str(),
            "execution_devnet_activation_drill_history.warnings_json",
        )?,
        drill_json: row
            .get(31)
            .context("failed reading execution_devnet_activation_drill_history.drill_json")?,
    })
}

impl SqliteStore {
    pub fn append_execution_devnet_activation_drill(
        &self,
        drill: &ExecutionDevnetActivationDrillWrite,
    ) -> Result<ExecutionDevnetActivationDrillRow> {
        self.ensure_execution_devnet_activation_drill_history_table()?;
        let blockers_json = serde_json::to_string(&drill.blockers)
            .context("failed serializing execution devnet activation drill blockers")?;
        let warnings_json = serde_json::to_string(&drill.warnings)
            .context("failed serializing execution devnet activation drill warnings")?;
        self.conn.execute(
            "INSERT INTO execution_devnet_activation_drill_history(
                drilled_at,
                target_environment,
                config_env,
                source_config_path,
                execution_enabled_source,
                route,
                token,
                side,
                notional_sol,
                launch_dossier_verdict,
                launch_dossier_reason,
                pre_activation_gate_verdict,
                pre_activation_gate_reason,
                tiny_live_policy_verdict,
                tiny_live_guardrail_verdict,
                tiny_live_policy_bounded,
                tiny_live_guardrails_bounded,
                activation_overlay_change_count,
                rollback_overlay_change_count,
                activation_drill_verdict,
                activation_drill_reason,
                activation_rehearsal_verdict,
                activation_rehearsal_reason,
                rollback_drill_verdict,
                rollback_drill_reason,
                activated_config_policy_bounded,
                activated_config_guardrails_bounded,
                blockers_json,
                warnings_json,
                rollback_restores_safe_mode,
                drill_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31)",
            params![
                drill.drilled_at.to_rfc3339(),
                drill.target_environment,
                drill.config_env,
                drill.source_config_path,
                i64::from(drill.execution_enabled_source),
                drill.route,
                drill.token,
                drill.side,
                drill.notional_sol,
                drill.launch_dossier_verdict,
                drill.launch_dossier_reason,
                drill.pre_activation_gate_verdict,
                drill.pre_activation_gate_reason,
                drill.tiny_live_policy_verdict,
                drill.tiny_live_guardrail_verdict,
                i64::from(drill.tiny_live_policy_bounded),
                i64::from(drill.tiny_live_guardrails_bounded),
                drill.activation_overlay_change_count as i64,
                drill.rollback_overlay_change_count as i64,
                drill.activation_drill_verdict,
                drill.activation_drill_reason,
                drill.activation_rehearsal_verdict,
                drill.activation_rehearsal_reason,
                drill.rollback_drill_verdict,
                drill.rollback_drill_reason,
                i64::from(drill.activated_config_policy_bounded),
                i64::from(drill.activated_config_guardrails_bounded),
                blockers_json,
                warnings_json,
                i64::from(drill.rollback_restores_safe_mode),
                drill.drill_json,
            ],
        )
        .context("failed inserting execution_devnet_activation_drill_history row")?;
        let drill_id = self.conn.last_insert_rowid();
        self.get_execution_devnet_activation_drill(drill_id)?
            .ok_or_else(|| anyhow!("inserted execution devnet activation drill row not found"))
    }

    pub fn list_execution_devnet_activation_drills(
        &self,
        limit: usize,
    ) -> Result<Vec<ExecutionDevnetActivationDrillRow>> {
        self.ensure_execution_devnet_activation_drill_history_table()?;
        let limit = limit.max(1) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    drill_id,
                    drilled_at,
                    target_environment,
                    config_env,
                    source_config_path,
                    execution_enabled_source,
                    route,
                    token,
                    side,
                    notional_sol,
                    launch_dossier_verdict,
                    launch_dossier_reason,
                    pre_activation_gate_verdict,
                    pre_activation_gate_reason,
                    tiny_live_policy_verdict,
                    tiny_live_guardrail_verdict,
                    tiny_live_policy_bounded,
                    tiny_live_guardrails_bounded,
                    activation_overlay_change_count,
                    rollback_overlay_change_count,
                    activation_drill_verdict,
                    activation_drill_reason,
                    activation_rehearsal_verdict,
                    activation_rehearsal_reason,
                    rollback_drill_verdict,
                    rollback_drill_reason,
                    activated_config_policy_bounded,
                    activated_config_guardrails_bounded,
                    blockers_json,
                    warnings_json,
                    rollback_restores_safe_mode,
                    drill_json
                 FROM execution_devnet_activation_drill_history
                 ORDER BY drilled_at DESC, drill_id DESC
                 LIMIT ?1",
            )
            .context("failed preparing execution devnet activation drill history query")?;
        let rows = stmt
            .query_map(params![limit], |row| {
                read_execution_devnet_activation_drill_row(row).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::other(error.to_string())),
                    )
                })
            })
            .context("failed querying execution devnet activation drill history")?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.context("failed decoding execution devnet activation drill row")?);
        }
        Ok(result)
    }

    fn get_execution_devnet_activation_drill(
        &self,
        drill_id: i64,
    ) -> Result<Option<ExecutionDevnetActivationDrillRow>> {
        self.ensure_execution_devnet_activation_drill_history_table()?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    drill_id,
                    drilled_at,
                    target_environment,
                    config_env,
                    source_config_path,
                    execution_enabled_source,
                    route,
                    token,
                    side,
                    notional_sol,
                    launch_dossier_verdict,
                    launch_dossier_reason,
                    pre_activation_gate_verdict,
                    pre_activation_gate_reason,
                    tiny_live_policy_verdict,
                    tiny_live_guardrail_verdict,
                    tiny_live_policy_bounded,
                    tiny_live_guardrails_bounded,
                    activation_overlay_change_count,
                    rollback_overlay_change_count,
                    activation_drill_verdict,
                    activation_drill_reason,
                    activation_rehearsal_verdict,
                    activation_rehearsal_reason,
                    rollback_drill_verdict,
                    rollback_drill_reason,
                    activated_config_policy_bounded,
                    activated_config_guardrails_bounded,
                    blockers_json,
                    warnings_json,
                    rollback_restores_safe_mode,
                    drill_json
                 FROM execution_devnet_activation_drill_history
                 WHERE drill_id = ?1",
            )
            .context("failed preparing execution devnet activation drill row query")?;
        stmt.query_row(params![drill_id], |row| {
            read_execution_devnet_activation_drill_row(row).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::other(error.to_string())),
                )
            })
        })
        .optional()
        .context("failed loading execution devnet activation drill row")
    }

    fn ensure_execution_devnet_activation_drill_history_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS execution_devnet_activation_drill_history (
                    drill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    drilled_at TEXT NOT NULL,
                    target_environment TEXT NOT NULL,
                    config_env TEXT NOT NULL,
                    source_config_path TEXT NOT NULL,
                    execution_enabled_source INTEGER NOT NULL,
                    route TEXT NOT NULL,
                    token TEXT NOT NULL,
                    side TEXT NOT NULL,
                    notional_sol REAL NOT NULL,
                    launch_dossier_verdict TEXT NOT NULL,
                    launch_dossier_reason TEXT NOT NULL,
                    pre_activation_gate_verdict TEXT NOT NULL,
                    pre_activation_gate_reason TEXT NOT NULL,
                    tiny_live_policy_verdict TEXT NOT NULL,
                    tiny_live_guardrail_verdict TEXT NOT NULL,
                    tiny_live_policy_bounded INTEGER NOT NULL,
                    tiny_live_guardrails_bounded INTEGER NOT NULL,
                    activation_overlay_change_count INTEGER NOT NULL,
                    rollback_overlay_change_count INTEGER NOT NULL,
                    activation_drill_verdict TEXT NOT NULL,
                    activation_drill_reason TEXT NOT NULL,
                    activation_rehearsal_verdict TEXT,
                    activation_rehearsal_reason TEXT,
                    rollback_drill_verdict TEXT NOT NULL,
                    rollback_drill_reason TEXT NOT NULL,
                    activated_config_policy_bounded INTEGER NOT NULL,
                    activated_config_guardrails_bounded INTEGER NOT NULL,
                    blockers_json TEXT NOT NULL,
                    warnings_json TEXT NOT NULL,
                    rollback_restores_safe_mode INTEGER NOT NULL,
                    drill_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_execution_devnet_activation_drill_history_drilled_at
                ON execution_devnet_activation_drill_history(drilled_at DESC, drill_id DESC);",
            )
            .context("failed to ensure execution_devnet_activation_drill_history table exists")?;
        Ok(())
    }
}
