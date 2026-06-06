use crate::types::{
    ExecutionQuoteCanaryPublicPaidComparisonEvent, ExecutionQuoteCanaryPublicPaidComparisonSummary,
};
use crate::{
    observed_timestamp::parse_rfc3339_utc, ExecutionQuoteCanaryProviderComparisonEvent,
    ExecutionQuoteCanaryProviderComparisonSummary, ExecutionQuoteCanaryProviderSampleInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

pub const PROVIDER_GENERIC_METIS: &str = "generic_metis";
pub const PROVIDER_GENERIC_PUBLIC: &str = "generic_public";
pub const PROVIDER_PUMP_FUN_PAID: &str = "pump_fun_paid";

pub(crate) fn ensure_execution_quote_canary_provider_samples_table(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    store
        .conn
        .execute_batch(EXECUTION_QUOTE_CANARY_PROVIDER_SAMPLES_SCHEMA)
        .context("failed ensuring execution quote canary provider samples schema")
}

impl SqliteDiscoveryStore {
    pub fn record_execution_quote_canary_provider_sample(
        &self,
        sample: &ExecutionQuoteCanaryProviderSampleInsert,
    ) -> Result<ExecutionQuoteCanaryRecordOutcome> {
        ensure_execution_quote_canary_provider_samples_table(self)?;
        let quote_latency_ms = optional_u64_to_i64(
            "execution_quote_canary_provider_samples.quote_latency_ms",
            sample.quote_latency_ms,
        )?;
        let inserted = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO execution_quote_canary_provider_samples(
                        event_id,
                        provider,
                        side,
                        quote_status,
                        request_ts,
                        quote_latency_ms,
                        quote_in_amount_raw,
                        quote_out_amount_raw,
                        quote_response_json,
                        quote_price_sol,
                        shadow_price_sol,
                        slippage_bps,
                        price_impact_pct,
                        route_plan_json,
                        decision_status,
                        decision_reason,
                        error
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17
                    )",
                    params![
                        &sample.event_id,
                        &sample.provider,
                        &sample.side,
                        &sample.quote_status,
                        sample.request_ts.to_rfc3339(),
                        quote_latency_ms,
                        sample.quote_in_amount_raw.as_deref(),
                        sample.quote_out_amount_raw.as_deref(),
                        sample.quote_response_json.as_deref(),
                        sample.quote_price_sol,
                        sample.shadow_price_sol,
                        sample.slippage_bps,
                        sample.price_impact_pct,
                        sample.route_plan_json.as_deref(),
                        sample.decision_status.as_deref(),
                        sample.decision_reason.as_deref(),
                        sample.error.as_deref(),
                    ],
                )
            })
            .context("failed recording execution quote canary provider sample")?;
        if inserted > 0 {
            Ok(ExecutionQuoteCanaryRecordOutcome::Inserted)
        } else {
            Ok(ExecutionQuoteCanaryRecordOutcome::Existing)
        }
    }

    pub fn execution_quote_canary_provider_comparison_summary(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionQuoteCanaryProviderComparisonSummary> {
        ensure_execution_quote_canary_provider_samples_table(self)?;
        let events = self.execution_quote_canary_provider_comparison_events(since, limit)?;
        Ok(summarize_provider_comparison(as_of, since, limit, events))
    }

    pub fn execution_quote_canary_public_paid_comparison_summary(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionQuoteCanaryPublicPaidComparisonSummary> {
        ensure_execution_quote_canary_provider_samples_table(self)?;
        let events = self.execution_quote_canary_public_paid_comparison_events(since, limit)?;
        Ok(summarize_public_paid_comparison(
            as_of, since, limit, events,
        ))
    }

    fn execution_quote_canary_provider_comparison_events(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ExecutionQuoteCanaryProviderComparisonEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    event.event_id,
                    event.side,
                    event.token,
                    event.request_ts,
                    generic.quote_status,
                    pump.quote_status,
                    generic.quote_latency_ms,
                    pump.quote_latency_ms,
                    generic.slippage_bps,
                    pump.slippage_bps,
                    generic.quote_price_sol,
                    pump.quote_price_sol,
                    COALESCE(pump.shadow_price_sol, generic.shadow_price_sol, event.shadow_price_sol),
                    generic.error,
                    pump.error
                 FROM execution_quote_canary_events AS event
                 LEFT JOIN execution_quote_canary_provider_samples AS generic
                    ON generic.event_id = event.event_id
                   AND generic.provider = ?2
                 LEFT JOIN execution_quote_canary_provider_samples AS pump
                    ON pump.event_id = event.event_id
                   AND pump.provider = ?3
                 WHERE event.request_ts >= ?1
                   AND (generic.event_id IS NOT NULL OR pump.event_id IS NOT NULL)
                 ORDER BY event.request_ts DESC, event.event_id DESC
                 LIMIT ?4",
            )
            .context("failed to prepare provider comparison query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    PROVIDER_GENERIC_METIS,
                    PROVIDER_PUMP_FUN_PAID,
                    i64::from(limit.max(1)),
                ],
                read_provider_comparison_event,
            )
            .context("failed querying provider comparison rows")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading provider comparison rows")
    }

    fn execution_quote_canary_public_paid_comparison_events(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ExecutionQuoteCanaryPublicPaidComparisonEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    event.event_id,
                    event.side,
                    event.token,
                    event.request_ts,
                    public.quote_status,
                    paid.quote_status,
                    public.quote_latency_ms,
                    paid.quote_latency_ms,
                    public.slippage_bps,
                    paid.slippage_bps,
                    public.quote_price_sol,
                    paid.quote_price_sol,
                    COALESCE(paid.shadow_price_sol, public.shadow_price_sol, event.shadow_price_sol),
                    public.error,
                    paid.error
                 FROM execution_quote_canary_events AS event
                 LEFT JOIN execution_quote_canary_provider_samples AS public
                    ON public.event_id = event.event_id
                   AND public.provider = ?2
                 LEFT JOIN execution_quote_canary_provider_samples AS paid
                    ON paid.event_id = event.event_id
                   AND paid.provider = ?3
                 WHERE event.request_ts >= ?1
                   AND (public.event_id IS NOT NULL OR paid.event_id IS NOT NULL)
                 ORDER BY event.request_ts DESC, event.event_id DESC
                 LIMIT ?4",
            )
            .context("failed to prepare public/paid generic comparison query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    PROVIDER_GENERIC_PUBLIC,
                    PROVIDER_GENERIC_METIS,
                    i64::from(limit.max(1)),
                ],
                read_public_paid_comparison_event,
            )
            .context("failed querying public/paid generic comparison rows")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading public/paid generic comparison rows")
    }
}

fn summarize_provider_comparison(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    latest: Vec<ExecutionQuoteCanaryProviderComparisonEvent>,
) -> ExecutionQuoteCanaryProviderComparisonSummary {
    let mut summary = ExecutionQuoteCanaryProviderComparisonSummary {
        as_of,
        since,
        limit,
        total_events: latest.len() as u64,
        paired_events: 0,
        both_ok_events: 0,
        generic_only_ok_events: 0,
        pump_fun_only_ok_events: 0,
        both_error_events: 0,
        pump_fun_better_slippage_events: 0,
        generic_better_slippage_events: 0,
        equal_slippage_events: 0,
        avg_generic_latency_ms: 0.0,
        avg_pump_fun_latency_ms: 0.0,
        avg_generic_slippage_bps: 0.0,
        avg_pump_fun_slippage_bps: 0.0,
        avg_pump_fun_minus_generic_slippage_bps: 0.0,
        latest,
    };
    let mut generic_latency = Avg::default();
    let mut pump_latency = Avg::default();
    let mut generic_slippage = Avg::default();
    let mut pump_slippage = Avg::default();
    let mut slippage_delta = Avg::default();
    for event in &summary.latest {
        if event.generic_status.is_some() && event.pump_fun_status.is_some() {
            summary.paired_events += 1;
        }
        let generic_ok = event.generic_status.as_deref() == Some("ok");
        let pump_ok = event.pump_fun_status.as_deref() == Some("ok");
        match (generic_ok, pump_ok) {
            (true, true) => summary.both_ok_events += 1,
            (true, false) => summary.generic_only_ok_events += 1,
            (false, true) => summary.pump_fun_only_ok_events += 1,
            (false, false) => summary.both_error_events += 1,
        }
        generic_latency.record(event.generic_latency_ms.map(|value| value as f64));
        pump_latency.record(event.pump_fun_latency_ms.map(|value| value as f64));
        generic_slippage.record(event.generic_slippage_bps);
        pump_slippage.record(event.pump_fun_slippage_bps);
        if let Some(delta) = event.slippage_delta_bps {
            slippage_delta.record(Some(delta));
            if delta < 0.0 {
                summary.pump_fun_better_slippage_events += 1;
            } else if delta > 0.0 {
                summary.generic_better_slippage_events += 1;
            } else {
                summary.equal_slippage_events += 1;
            }
        }
    }
    summary.avg_generic_latency_ms = generic_latency.avg;
    summary.avg_pump_fun_latency_ms = pump_latency.avg;
    summary.avg_generic_slippage_bps = generic_slippage.avg;
    summary.avg_pump_fun_slippage_bps = pump_slippage.avg;
    summary.avg_pump_fun_minus_generic_slippage_bps = slippage_delta.avg;
    summary
}

fn summarize_public_paid_comparison(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    latest: Vec<ExecutionQuoteCanaryPublicPaidComparisonEvent>,
) -> ExecutionQuoteCanaryPublicPaidComparisonSummary {
    let mut summary = ExecutionQuoteCanaryPublicPaidComparisonSummary {
        as_of,
        since,
        limit,
        total_events: latest.len() as u64,
        paired_events: 0,
        both_ok_events: 0,
        public_only_ok_events: 0,
        paid_only_ok_events: 0,
        both_error_events: 0,
        paid_better_slippage_events: 0,
        public_better_slippage_events: 0,
        equal_slippage_events: 0,
        avg_public_latency_ms: 0.0,
        avg_paid_latency_ms: 0.0,
        avg_public_slippage_bps: 0.0,
        avg_paid_slippage_bps: 0.0,
        avg_paid_minus_public_slippage_bps: 0.0,
        latest,
    };
    let mut public_latency = Avg::default();
    let mut paid_latency = Avg::default();
    let mut public_slippage = Avg::default();
    let mut paid_slippage = Avg::default();
    let mut slippage_delta = Avg::default();
    for event in &summary.latest {
        if event.public_status.is_some() && event.paid_status.is_some() {
            summary.paired_events += 1;
        }
        let public_ok = event.public_status.as_deref() == Some("ok");
        let paid_ok = event.paid_status.as_deref() == Some("ok");
        match (public_ok, paid_ok) {
            (true, true) => summary.both_ok_events += 1,
            (true, false) => summary.public_only_ok_events += 1,
            (false, true) => summary.paid_only_ok_events += 1,
            (false, false) => summary.both_error_events += 1,
        }
        public_latency.record(event.public_latency_ms.map(|value| value as f64));
        paid_latency.record(event.paid_latency_ms.map(|value| value as f64));
        public_slippage.record(event.public_slippage_bps);
        paid_slippage.record(event.paid_slippage_bps);
        if let Some(delta) = event.slippage_delta_bps {
            slippage_delta.record(Some(delta));
            if delta < 0.0 {
                summary.paid_better_slippage_events += 1;
            } else if delta > 0.0 {
                summary.public_better_slippage_events += 1;
            } else {
                summary.equal_slippage_events += 1;
            }
        }
    }
    summary.avg_public_latency_ms = public_latency.avg;
    summary.avg_paid_latency_ms = paid_latency.avg;
    summary.avg_public_slippage_bps = public_slippage.avg;
    summary.avg_paid_slippage_bps = paid_slippage.avg;
    summary.avg_paid_minus_public_slippage_bps = slippage_delta.avg;
    summary
}

fn read_provider_comparison_event(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionQuoteCanaryProviderComparisonEvent> {
    read_provider_comparison_event_result(row).map_err(|error| {
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

fn read_provider_comparison_event_result(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionQuoteCanaryProviderComparisonEvent> {
    let request_ts_raw: String = row.get(3).context("failed reading request_ts")?;
    let generic_latency = optional_i64_to_u64("generic.quote_latency_ms", row.get(6)?)?;
    let pump_latency = optional_i64_to_u64("pump.quote_latency_ms", row.get(7)?)?;
    let generic_slippage: Option<f64> = row.get(8)?;
    let pump_slippage: Option<f64> = row.get(9)?;
    let slippage_delta_bps = match (generic_slippage, pump_slippage) {
        (Some(generic), Some(pump)) if generic.is_finite() && pump.is_finite() => {
            Some(pump - generic)
        }
        _ => None,
    };
    let latency_delta_ms = match (generic_latency, pump_latency) {
        (Some(generic), Some(pump)) => i64::try_from(pump)
            .ok()
            .zip(i64::try_from(generic).ok())
            .map(|(pump, generic)| pump - generic),
        _ => None,
    };
    Ok(ExecutionQuoteCanaryProviderComparisonEvent {
        event_id: row.get(0).context("failed reading event_id")?,
        side: row.get(1).context("failed reading side")?,
        token: row.get(2).context("failed reading token")?,
        request_ts: parse_rfc3339_utc(&request_ts_raw, "provider.request_ts")?,
        generic_status: row.get(4).context("failed reading generic quote_status")?,
        pump_fun_status: row.get(5).context("failed reading pump quote_status")?,
        generic_latency_ms: generic_latency,
        pump_fun_latency_ms: pump_latency,
        generic_slippage_bps: generic_slippage,
        pump_fun_slippage_bps: pump_slippage,
        slippage_delta_bps,
        latency_delta_ms,
        generic_quote_price_sol: row.get(10).context("failed reading generic quote price")?,
        pump_fun_quote_price_sol: row.get(11).context("failed reading pump quote price")?,
        shadow_price_sol: row.get(12).context("failed reading shadow price")?,
        better_provider: better_provider(slippage_delta_bps),
        generic_error: row.get(13).context("failed reading generic error")?,
        pump_fun_error: row.get(14).context("failed reading pump error")?,
    })
}

fn read_public_paid_comparison_event(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionQuoteCanaryPublicPaidComparisonEvent> {
    read_public_paid_comparison_event_result(row).map_err(|error| {
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

fn read_public_paid_comparison_event_result(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionQuoteCanaryPublicPaidComparisonEvent> {
    let request_ts_raw: String = row.get(3).context("failed reading request_ts")?;
    let public_latency = optional_i64_to_u64("public.quote_latency_ms", row.get(6)?)?;
    let paid_latency = optional_i64_to_u64("paid.quote_latency_ms", row.get(7)?)?;
    let public_slippage: Option<f64> = row.get(8)?;
    let paid_slippage: Option<f64> = row.get(9)?;
    let slippage_delta_bps = match (public_slippage, paid_slippage) {
        (Some(public), Some(paid)) if public.is_finite() && paid.is_finite() => Some(paid - public),
        _ => None,
    };
    let latency_delta_ms = match (public_latency, paid_latency) {
        (Some(public), Some(paid)) => i64::try_from(paid)
            .ok()
            .zip(i64::try_from(public).ok())
            .map(|(paid, public)| paid - public),
        _ => None,
    };
    Ok(ExecutionQuoteCanaryPublicPaidComparisonEvent {
        event_id: row.get(0).context("failed reading event_id")?,
        side: row.get(1).context("failed reading side")?,
        token: row.get(2).context("failed reading token")?,
        request_ts: parse_rfc3339_utc(&request_ts_raw, "provider.request_ts")?,
        public_status: row.get(4).context("failed reading public quote_status")?,
        paid_status: row.get(5).context("failed reading paid quote_status")?,
        public_latency_ms: public_latency,
        paid_latency_ms: paid_latency,
        public_slippage_bps: public_slippage,
        paid_slippage_bps: paid_slippage,
        slippage_delta_bps,
        latency_delta_ms,
        public_quote_price_sol: row.get(10).context("failed reading public quote price")?,
        paid_quote_price_sol: row.get(11).context("failed reading paid quote price")?,
        shadow_price_sol: row.get(12).context("failed reading shadow price")?,
        better_provider: better_generic_provider(slippage_delta_bps),
        public_error: row.get(13).context("failed reading public error")?,
        paid_error: row.get(14).context("failed reading paid error")?,
    })
}

fn better_provider(slippage_delta_bps: Option<f64>) -> Option<String> {
    let delta = slippage_delta_bps?;
    if delta < 0.0 {
        Some(PROVIDER_PUMP_FUN_PAID.to_string())
    } else if delta > 0.0 {
        Some(PROVIDER_GENERIC_METIS.to_string())
    } else {
        Some("equal".to_string())
    }
}

fn better_generic_provider(slippage_delta_bps: Option<f64>) -> Option<String> {
    let delta = slippage_delta_bps?;
    if delta < 0.0 {
        Some(PROVIDER_GENERIC_METIS.to_string())
    } else if delta > 0.0 {
        Some(PROVIDER_GENERIC_PUBLIC.to_string())
    } else {
        Some("equal".to_string())
    }
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

#[derive(Default)]
struct Avg {
    samples: u64,
    avg: f64,
}

impl Avg {
    fn record(&mut self, value: Option<f64>) {
        let Some(value) = value.filter(|value| value.is_finite()) else {
            return;
        };
        self.samples += 1;
        self.avg += (value - self.avg) / self.samples as f64;
    }
}

const EXECUTION_QUOTE_CANARY_PROVIDER_SAMPLES_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS execution_quote_canary_provider_samples (
    event_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
    quote_status TEXT NOT NULL,
    request_ts TEXT NOT NULL,
    quote_latency_ms INTEGER,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_response_json TEXT,
    quote_price_sol REAL,
    shadow_price_sol REAL,
    slippage_bps REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    decision_status TEXT,
    decision_reason TEXT,
    error TEXT,
    PRIMARY KEY(event_id, provider)
);
CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_provider_samples_request
    ON execution_quote_canary_provider_samples(provider, request_ts);
";
