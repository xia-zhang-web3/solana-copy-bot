use crate::types::{
    ExecutionQuoteCanaryProviderSelectionEvent, ExecutionQuoteCanaryProviderSelectionSummary,
};
use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, SqliteDiscoveryStore, PROVIDER_GENERIC_METIS,
    PROVIDER_GENERIC_PUBLIC, PROVIDER_PUMP_FUN_PAID,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use serde_json::Value;

const SELECTED_GENERIC_METIS: &str = "generic_metis";
const SELECTED_GENERIC_PUBLIC: &str = "generic_public";
const SELECTED_PUMP_FUN_PAID: &str = "pump_fun_paid";
const SELECTED_UNRESOLVED: &str = "unresolved";

impl SqliteDiscoveryStore {
    pub fn execution_quote_canary_provider_selection_summary(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionQuoteCanaryProviderSelectionSummary> {
        ensure_execution_quote_canary_tables(self)?;
        let latest = self.execution_quote_canary_provider_selection_events(since, limit)?;
        Ok(summarize_selection(as_of, since, limit, latest))
    }

    fn execution_quote_canary_provider_selection_events(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ExecutionQuoteCanaryProviderSelectionEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    event.event_id,
                    event.side,
                    event.token,
                    event.request_ts,
                    COALESCE(generic.quote_status, event.quote_status),
                    public.quote_status,
                    pump.quote_status,
                    pump.quote_response_json,
                    generic.slippage_bps,
                    public.slippage_bps,
                    COALESCE(generic.error, event.error),
                    public.error,
                    pump.error
                 FROM execution_quote_canary_events AS event
                 LEFT JOIN execution_quote_canary_provider_samples AS generic
                    ON generic.event_id = event.event_id
                   AND generic.provider = ?2
                 LEFT JOIN execution_quote_canary_provider_samples AS public
                    ON public.event_id = event.event_id
                   AND public.provider = ?3
                 LEFT JOIN execution_quote_canary_provider_samples AS pump
                    ON pump.event_id = event.event_id
                   AND pump.provider = ?4
                 WHERE event.request_ts >= ?1
                 ORDER BY event.request_ts DESC, event.event_id DESC
                 LIMIT ?5",
            )
            .context("failed to prepare provider selection query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    PROVIDER_GENERIC_METIS,
                    PROVIDER_GENERIC_PUBLIC,
                    PROVIDER_PUMP_FUN_PAID,
                    i64::from(limit.max(1)),
                ],
                read_selection_event,
            )
            .context("failed querying provider selection rows")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading provider selection rows")
    }
}

fn summarize_selection(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    latest: Vec<ExecutionQuoteCanaryProviderSelectionEvent>,
) -> ExecutionQuoteCanaryProviderSelectionSummary {
    let mut summary = ExecutionQuoteCanaryProviderSelectionSummary {
        as_of,
        since,
        limit,
        total_events: latest.len() as u64,
        selected_generic_metis_events: 0,
        selected_generic_public_events: 0,
        selected_pump_fun_paid_events: 0,
        unresolved_events: 0,
        latest,
    };
    for event in &summary.latest {
        match event.selected_provider.as_str() {
            SELECTED_GENERIC_METIS => summary.selected_generic_metis_events += 1,
            SELECTED_GENERIC_PUBLIC => summary.selected_generic_public_events += 1,
            SELECTED_PUMP_FUN_PAID => summary.selected_pump_fun_paid_events += 1,
            _ => summary.unresolved_events += 1,
        }
    }
    summary
}

fn read_selection_event(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionQuoteCanaryProviderSelectionEvent> {
    read_selection_event_result(row).map_err(|error| {
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

fn read_selection_event_result(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionQuoteCanaryProviderSelectionEvent> {
    let request_ts_raw: String = row.get(3).context("failed reading request_ts")?;
    let generic_status: Option<String> = row.get(4).context("failed reading generic status")?;
    let public_status: Option<String> = row.get(5).context("failed reading public status")?;
    let pump_status: Option<String> = row.get(6).context("failed reading pump status")?;
    let pump_response_json: Option<String> =
        row.get(7).context("failed reading pump response JSON")?;
    let generic_slippage_bps: Option<f64> =
        row.get(8).context("failed reading generic slippage bps")?;
    let public_slippage_bps: Option<f64> =
        row.get(9).context("failed reading public slippage bps")?;
    let pump_is_completed = pump_response_json
        .as_deref()
        .and_then(|raw| pump_fun_is_completed(raw).ok());
    let (selected_provider, selected_reason) = select_provider(
        &generic_status,
        generic_slippage_bps,
        &public_status,
        public_slippage_bps,
        &pump_status,
        pump_is_completed,
    );
    Ok(ExecutionQuoteCanaryProviderSelectionEvent {
        event_id: row.get(0).context("failed reading event_id")?,
        side: row.get(1).context("failed reading side")?,
        token: row.get(2).context("failed reading token")?,
        request_ts: parse_rfc3339_utc(&request_ts_raw, "execution_quote_canary_events.request_ts")?,
        selected_provider,
        selected_reason,
        generic_metis_status: generic_status,
        generic_public_status: public_status,
        pump_fun_paid_status: pump_status,
        pump_fun_paid_is_completed: pump_is_completed,
        generic_metis_error: row.get(10).context("failed reading generic error")?,
        generic_public_error: row.get(11).context("failed reading public error")?,
        pump_fun_paid_error: row.get(12).context("failed reading pump error")?,
    })
}

fn select_provider(
    generic_status: &Option<String>,
    generic_slippage_bps: Option<f64>,
    public_status: &Option<String>,
    public_slippage_bps: Option<f64>,
    pump_status: &Option<String>,
    pump_is_completed: Option<bool>,
) -> (String, String) {
    if pump_status.as_deref() == Some("ok") && pump_is_completed == Some(false) {
        return (
            SELECTED_PUMP_FUN_PAID.to_string(),
            "pump_fun_bonding_curve_quote_ok".to_string(),
        );
    }
    if provider_has_finite_slippage(generic_status, generic_slippage_bps)
        || provider_has_finite_slippage(public_status, public_slippage_bps)
    {
        if provider_has_finite_slippage(public_status, public_slippage_bps)
            && (!provider_has_finite_slippage(generic_status, generic_slippage_bps)
                || public_slippage_bps.expect("checked finite")
                    < generic_slippage_bps.expect("checked finite"))
        {
            return (
                SELECTED_GENERIC_PUBLIC.to_string(),
                "public_generic_best_slippage".to_string(),
            );
        }
        return (
            SELECTED_GENERIC_METIS.to_string(),
            "paid_generic_best_slippage".to_string(),
        );
    }
    if generic_status.as_deref() == Some("ok") {
        return (
            SELECTED_GENERIC_METIS.to_string(),
            "paid_generic_quote_ok".to_string(),
        );
    }
    if public_status.as_deref() == Some("ok") {
        return (
            SELECTED_GENERIC_PUBLIC.to_string(),
            "public_generic_fallback_ok".to_string(),
        );
    }
    (
        SELECTED_UNRESOLVED.to_string(),
        "no_usable_quote".to_string(),
    )
}

fn provider_has_finite_slippage(status: &Option<String>, slippage_bps: Option<f64>) -> bool {
    status.as_deref() == Some("ok") && slippage_bps.is_some_and(f64::is_finite)
}

fn pump_fun_is_completed(raw: &str) -> Result<bool> {
    let value: Value = serde_json::from_str(raw).context("invalid pump.fun quote response JSON")?;
    value
        .pointer("/quote/meta/isCompleted")
        .and_then(Value::as_bool)
        .context("missing pump.fun quote meta.isCompleted")
}
