use crate::{
    execution_quote_canary_route_samples::ensure_execution_quote_canary_provider_samples_table,
    observed_timestamp::parse_rfc3339_utc, schema::column_exists,
    ExecutionQuoteCanaryProviderSampleInsert, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn load_execution_quote_canary_provider_sample(
        &self,
        event_id: &str,
        provider: &str,
    ) -> Result<Option<ExecutionQuoteCanaryProviderSampleInsert>> {
        ensure_execution_quote_canary_provider_samples_table(self)?;
        self.conn
            .query_row(
                &format!(
                    "SELECT
                        event_id,
                        provider,
                        side,
                        quote_status,
                        request_ts,
                        quote_latency_ms,
                        quote_in_amount_raw,
                        quote_out_amount_raw,
                        {},
                        quote_price_sol,
                        shadow_price_sol,
                        slippage_bps,
                        price_impact_pct,
                        route_plan_json,
                        decision_status,
                        decision_reason,
                        error
                     FROM execution_quote_canary_provider_samples
                     WHERE event_id = ?1
                       AND provider = ?2
                     LIMIT 1",
                    quote_response_json_expr(self)?
                ),
                params![event_id, provider],
                provider_sample_from_row,
            )
            .optional()
            .context("failed loading execution quote canary provider sample")
    }
}

fn provider_sample_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionQuoteCanaryProviderSampleInsert> {
    read_provider_sample_row(row).map_err(|error| {
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

fn read_provider_sample_row(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionQuoteCanaryProviderSampleInsert> {
    let request_ts_raw: String = row.get(4).context("failed reading request_ts")?;
    Ok(ExecutionQuoteCanaryProviderSampleInsert {
        event_id: row.get(0).context("failed reading event_id")?,
        provider: row.get(1).context("failed reading provider")?,
        side: row.get(2).context("failed reading side")?,
        quote_status: row.get(3).context("failed reading quote_status")?,
        request_ts: parse_rfc3339_utc(
            &request_ts_raw,
            "execution_quote_canary_provider_samples.request_ts",
        )?,
        quote_latency_ms: optional_i64_to_u64(
            "execution_quote_canary_provider_samples.quote_latency_ms",
            row.get(5).context("failed reading quote_latency_ms")?,
        )?,
        quote_in_amount_raw: row.get(6).context("failed reading quote_in_amount_raw")?,
        quote_out_amount_raw: row.get(7).context("failed reading quote_out_amount_raw")?,
        quote_response_json: row.get(8).context("failed reading quote_response_json")?,
        quote_price_sol: row.get(9).context("failed reading quote_price_sol")?,
        shadow_price_sol: row.get(10).context("failed reading shadow_price_sol")?,
        slippage_bps: row.get(11).context("failed reading slippage_bps")?,
        price_impact_pct: row.get(12).context("failed reading price_impact_pct")?,
        route_plan_json: row.get(13).context("failed reading route_plan_json")?,
        decision_status: row.get(14).context("failed reading decision_status")?,
        decision_reason: row.get(15).context("failed reading decision_reason")?,
        error: row.get(16).context("failed reading error")?,
    })
}

fn quote_response_json_expr(store: &SqliteDiscoveryStore) -> Result<String> {
    if column_exists(
        store,
        "execution_quote_canary_provider_samples",
        "quote_response_json",
    )? {
        Ok("quote_response_json".to_string())
    } else {
        Ok("NULL AS quote_response_json".to_string())
    }
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).with_context(|| format!("{field} is negative or invalid: {raw}"))
        })
        .transpose()
}
