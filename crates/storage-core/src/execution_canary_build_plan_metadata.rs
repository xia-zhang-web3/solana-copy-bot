use crate::{
    observed_timestamp::parse_rfc3339_utc, schema::column_exists, schema::ensure_column,
    ExecutionCanaryBuildPlanMetadata, ExecutionCanaryBuildPlanMetadataRecordOutcome,
    SqliteDiscoveryStore,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_build_plan_metadata(
        &self,
        metadata: &ExecutionCanaryBuildPlanMetadata,
    ) -> Result<ExecutionCanaryBuildPlanMetadataRecordOutcome> {
        validate_required(
            "execution canary build metadata order_id",
            &metadata.order_id,
        )?;
        validate_required(
            "execution canary build metadata signal_id",
            &metadata.signal_id,
        )?;
        validate_required(
            "execution canary build metadata client_order_id",
            &metadata.client_order_id,
        )?;
        ensure_execution_canary_build_plan_metadata_table(self)?;
        let priority_fee_lamports = optional_u64_to_i64(
            "execution_canary_build_plan_metadata.priority_fee_lamports",
            metadata.priority_fee_lamports,
        )?;
        let inserted = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO execution_canary_build_plan_metadata(
                        order_id,
                        signal_id,
                        client_order_id,
                        recorded_ts,
                        quote_source,
                        quote_event_id,
                        quote_status,
                        quote_in_amount_raw,
                        quote_out_amount_raw,
                        quote_price_sol,
                        price_impact_pct,
                        route_plan_json,
                        priority_fee_source,
                        priority_fee_status,
                        priority_fee_lamports,
                        priority_fee_json,
                        slippage_bps,
                        decision_status,
                        decision_reason
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                        ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                        ?17, ?18, ?19
                    )",
                    params![
                        &metadata.order_id,
                        &metadata.signal_id,
                        &metadata.client_order_id,
                        metadata.recorded_ts.to_rfc3339(),
                        metadata.quote_source.as_deref(),
                        metadata.quote_event_id.as_deref(),
                        metadata.quote_status.as_deref(),
                        metadata.quote_in_amount_raw.as_deref(),
                        metadata.quote_out_amount_raw.as_deref(),
                        metadata.quote_price_sol,
                        metadata.price_impact_pct,
                        metadata.route_plan_json.as_deref(),
                        metadata.priority_fee_source.as_deref(),
                        metadata.priority_fee_status.as_deref(),
                        priority_fee_lamports,
                        metadata.priority_fee_json.as_deref(),
                        metadata.slippage_bps,
                        metadata.decision_status.as_deref(),
                        metadata.decision_reason.as_deref(),
                    ],
                )
            })
            .context("failed recording execution canary build plan metadata")?;
        if inserted > 0 {
            Ok(ExecutionCanaryBuildPlanMetadataRecordOutcome::Inserted)
        } else {
            Ok(ExecutionCanaryBuildPlanMetadataRecordOutcome::Existing)
        }
    }

    pub fn load_execution_canary_build_plan_metadata(
        &self,
        order_id: &str,
    ) -> Result<Option<ExecutionCanaryBuildPlanMetadata>> {
        if !self.sqlite_table_exists("execution_canary_build_plan_metadata")? {
            return Ok(None);
        }
        self.conn
            .query_row(
                &format!(
                    "SELECT {}
                     FROM execution_canary_build_plan_metadata
                     WHERE order_id = ?1
                     LIMIT 1",
                    build_plan_metadata_columns(self)?
                ),
                params![order_id],
                execution_canary_build_plan_metadata_from_row,
            )
            .optional()
            .context("failed loading execution canary build plan metadata")
    }

    pub fn latest_execution_canary_build_plan_metadata(
        &self,
    ) -> Result<Option<ExecutionCanaryBuildPlanMetadata>> {
        if !self.sqlite_table_exists("execution_canary_build_plan_metadata")? {
            return Ok(None);
        }
        self.conn
            .query_row(
                &format!(
                    "SELECT {}
                     FROM execution_canary_build_plan_metadata
                     ORDER BY recorded_ts DESC, order_id DESC
                     LIMIT 1",
                    build_plan_metadata_columns(self)?
                ),
                [],
                execution_canary_build_plan_metadata_from_row,
            )
            .optional()
            .context("failed loading latest execution canary build plan metadata")
    }
}

fn ensure_execution_canary_build_plan_metadata_table(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .execute_with_retry_result(|conn| conn.execute_batch(EXECUTION_CANARY_BUILD_PLAN_SCHEMA))
        .context("failed ensuring execution canary build plan metadata table")?;
    ensure_column(
        store,
        "execution_canary_build_plan_metadata",
        "slippage_bps",
        "REAL",
    )?;
    ensure_column(
        store,
        "execution_canary_build_plan_metadata",
        "decision_status",
        "TEXT",
    )?;
    ensure_column(
        store,
        "execution_canary_build_plan_metadata",
        "decision_reason",
        "TEXT",
    )?;
    Ok(())
}

fn build_plan_metadata_columns(store: &SqliteDiscoveryStore) -> Result<String> {
    Ok(format!(
        "{BUILD_PLAN_METADATA_BASE_COLUMNS},
         {},
         {},
         {}",
        optional_column_expr(store, "slippage_bps")?,
        optional_column_expr(store, "decision_status")?,
        optional_column_expr(store, "decision_reason")?,
    ))
}

fn optional_column_expr(store: &SqliteDiscoveryStore, column: &str) -> Result<String> {
    if column_exists(store, "execution_canary_build_plan_metadata", column)? {
        Ok(column.to_string())
    } else {
        Ok(format!("NULL AS {column}"))
    }
}

const BUILD_PLAN_METADATA_BASE_COLUMNS: &str = "
    order_id,
    signal_id,
    client_order_id,
    recorded_ts,
    quote_source,
    quote_event_id,
    quote_status,
    quote_in_amount_raw,
    quote_out_amount_raw,
    quote_price_sol,
    price_impact_pct,
    route_plan_json,
    priority_fee_source,
    priority_fee_status,
    priority_fee_lamports,
    priority_fee_json";

fn execution_canary_build_plan_metadata_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionCanaryBuildPlanMetadata> {
    read_execution_canary_build_plan_metadata_row(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        )
    })
}

fn read_execution_canary_build_plan_metadata_row(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionCanaryBuildPlanMetadata> {
    let recorded_ts_raw: String = row.get(3).context("failed reading recorded_ts")?;
    let priority_fee_lamports = optional_i64_to_u64(
        "execution_canary_build_plan_metadata.priority_fee_lamports",
        row.get(14)
            .context("failed reading priority_fee_lamports")?,
    )?;
    Ok(ExecutionCanaryBuildPlanMetadata {
        order_id: row.get(0).context("failed reading order_id")?,
        signal_id: row.get(1).context("failed reading signal_id")?,
        client_order_id: row.get(2).context("failed reading client_order_id")?,
        recorded_ts: parse_rfc3339_utc(
            &recorded_ts_raw,
            "execution_canary_build_plan_metadata.recorded_ts",
        )?,
        quote_source: row.get(4).context("failed reading quote_source")?,
        quote_event_id: row.get(5).context("failed reading quote_event_id")?,
        quote_status: row.get(6).context("failed reading quote_status")?,
        quote_in_amount_raw: row.get(7).context("failed reading quote_in_amount_raw")?,
        quote_out_amount_raw: row.get(8).context("failed reading quote_out_amount_raw")?,
        quote_price_sol: row.get(9).context("failed reading quote_price_sol")?,
        price_impact_pct: row.get(10).context("failed reading price_impact_pct")?,
        route_plan_json: row.get(11).context("failed reading route_plan_json")?,
        priority_fee_source: row.get(12).context("failed reading priority_fee_source")?,
        priority_fee_status: row.get(13).context("failed reading priority_fee_status")?,
        priority_fee_lamports,
        priority_fee_json: row.get(15).context("failed reading priority_fee_json")?,
        slippage_bps: row.get(16).context("failed reading slippage_bps")?,
        decision_status: row.get(17).context("failed reading decision_status")?,
        decision_reason: row.get(18).context("failed reading decision_reason")?,
    })
}

fn optional_u64_to_i64(field: &str, value: Option<u64>) -> Result<Option<i64>> {
    value
        .map(|raw| i64::try_from(raw).with_context(|| format!("{field} exceeds i64: {raw}")))
        .transpose()
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).with_context(|| format!("{field} is negative or invalid: {raw}"))
        })
        .transpose()
}

fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must be non-empty"));
    }
    Ok(())
}

const EXECUTION_CANARY_BUILD_PLAN_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS execution_canary_build_plan_metadata (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    recorded_ts TEXT NOT NULL,
    quote_source TEXT,
    quote_event_id TEXT,
    quote_status TEXT,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_price_sol REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    priority_fee_source TEXT,
    priority_fee_status TEXT,
    priority_fee_lamports INTEGER,
    priority_fee_json TEXT,
    slippage_bps REAL,
    decision_status TEXT,
    decision_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_execution_canary_build_plan_signal
    ON execution_canary_build_plan_metadata(signal_id);
CREATE INDEX IF NOT EXISTS idx_execution_canary_build_plan_recorded_ts
    ON execution_canary_build_plan_metadata(recorded_ts);
";
