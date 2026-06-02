use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryBuildPlanMetadata,
    ExecutionCanaryOrder, ExecutionCanaryReadinessLatestOrder, ExecutionCanaryReadinessSummary,
    ExecutionCanaryReadinessWindowSummary, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::BTreeMap;

const READINESS_NO_ORDER: &str = "no_order";
const READINESS_MISSING_METADATA: &str = "missing_build_metadata";
const READINESS_WOULD_ENTER: &str = "would_enter";
const READINESS_WOULD_SKIP: &str = "would_skip";
const READINESS_UNKNOWN: &str = "unknown";

const QUOTE_STATUS_OK: &str = "ok";
const QUOTE_DECISION_WOULD_EXECUTE: &str = "would_execute";
const QUOTE_DECISION_WOULD_SKIP: &str = "would_skip";
const QUOTE_DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

impl SqliteDiscoveryStore {
    pub fn execution_canary_readiness_summary(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<ExecutionCanaryReadinessSummary> {
        let report = self.execution_canary_status_report(as_of)?;
        let latest = match report.latest_order.as_ref() {
            Some(order) => {
                let metadata = self.load_execution_canary_build_plan_metadata(&order.order_id)?;
                Some(readiness_latest_order(order, metadata.as_ref()))
            }
            None => None,
        };
        let (readiness_status, readiness_reason) = classify_readiness(latest.as_ref());

        Ok(ExecutionCanaryReadinessSummary {
            as_of,
            readiness_status,
            readiness_reason,
            total_orders: report.total,
            active_orders: report.active_count(),
            failed_orders: report.failed,
            submit_disabled_orders: report.submit_disabled,
            latest,
        })
    }

    pub fn execution_canary_readiness_window_summary(
        &self,
        as_of: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionCanaryReadinessWindowSummary> {
        let orders = self.recent_execution_canary_orders(limit)?;
        let mut window = WindowAccumulator::new(as_of, limit);

        for order in orders {
            let metadata = self.load_execution_canary_build_plan_metadata(&order.order_id)?;
            let latest = readiness_latest_order(&order, metadata.as_ref());
            let (readiness_status, readiness_reason) = classify_readiness(Some(&latest));
            window.record(&latest, &readiness_status, &readiness_reason);
        }

        Ok(window.finish())
    }

    fn recent_execution_canary_orders(&self, limit: u32) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    order_id,
                    signal_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    client_order_id,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt
                 FROM orders
                 WHERE order_id LIKE 'exec-canary:%'
                 ORDER BY submit_ts DESC, order_id DESC
                 LIMIT ?1",
            )
            .context("failed to prepare execution canary readiness window query")?;
        let rows = stmt
            .query_map(params![i64::from(limit)], execution_canary_order_from_row)
            .context("failed querying execution canary readiness window")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution canary readiness window")
    }
}

fn readiness_latest_order(
    order: &ExecutionCanaryOrder,
    metadata: Option<&ExecutionCanaryBuildPlanMetadata>,
) -> ExecutionCanaryReadinessLatestOrder {
    ExecutionCanaryReadinessLatestOrder {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        order_status: order.status.clone(),
        route: order.route.clone(),
        submit_ts: order.submit_ts,
        metadata_recorded_ts: metadata.map(|row| row.recorded_ts),
        quote_source: metadata.and_then(|row| row.quote_source.clone()),
        quote_event_id: metadata.and_then(|row| row.quote_event_id.clone()),
        quote_status: metadata.and_then(|row| row.quote_status.clone()),
        quote_in_amount_raw: metadata.and_then(|row| row.quote_in_amount_raw.clone()),
        quote_out_amount_raw: metadata.and_then(|row| row.quote_out_amount_raw.clone()),
        quote_price_sol: metadata.and_then(|row| row.quote_price_sol),
        slippage_bps: metadata.and_then(|row| row.slippage_bps),
        price_impact_pct: metadata.and_then(|row| row.price_impact_pct),
        route_plan_json: metadata.and_then(|row| row.route_plan_json.clone()),
        priority_fee_source: metadata.and_then(|row| row.priority_fee_source.clone()),
        priority_fee_status: metadata.and_then(|row| row.priority_fee_status.clone()),
        priority_fee_lamports: metadata.and_then(|row| row.priority_fee_lamports),
        decision_status: metadata.and_then(|row| row.decision_status.clone()),
        decision_reason: metadata.and_then(|row| row.decision_reason.clone()),
    }
}

fn classify_readiness(latest: Option<&ExecutionCanaryReadinessLatestOrder>) -> (String, String) {
    let Some(latest) = latest else {
        return (
            READINESS_NO_ORDER.to_string(),
            "no canary order has been recorded".to_string(),
        );
    };
    if latest.metadata_recorded_ts.is_none() {
        return (
            READINESS_MISSING_METADATA.to_string(),
            "latest canary order has no persisted build metadata".to_string(),
        );
    }

    match latest.decision_status.as_deref() {
        Some(QUOTE_DECISION_WOULD_EXECUTE) => (
            READINESS_WOULD_ENTER.to_string(),
            latest
                .decision_reason
                .clone()
                .unwrap_or_else(|| "quote decision would execute".to_string()),
        ),
        Some(QUOTE_DECISION_WOULD_SKIP) => (
            READINESS_WOULD_SKIP.to_string(),
            latest
                .decision_reason
                .clone()
                .unwrap_or_else(|| "quote decision would skip".to_string()),
        ),
        Some(QUOTE_DECISION_WOULD_FORCE_EXIT) => (
            READINESS_WOULD_SKIP.to_string(),
            "force-exit quote decision is not a BUY entry readiness signal".to_string(),
        ),
        Some(other) if !other.trim().is_empty() => (
            READINESS_UNKNOWN.to_string(),
            format!("unsupported quote decision status: {other}"),
        ),
        _ => classify_without_quote_decision(latest),
    }
}

fn classify_without_quote_decision(
    latest: &ExecutionCanaryReadinessLatestOrder,
) -> (String, String) {
    match latest.quote_status.as_deref() {
        Some(QUOTE_STATUS_OK) => (
            READINESS_UNKNOWN.to_string(),
            "quote status is ok but no quote decision was persisted".to_string(),
        ),
        Some(status) => (
            READINESS_WOULD_SKIP.to_string(),
            format!("quote status is not ok: {status}"),
        ),
        None => (
            READINESS_UNKNOWN.to_string(),
            "quote status is missing from build metadata".to_string(),
        ),
    }
}

struct WindowAccumulator {
    as_of: DateTime<Utc>,
    limit: u32,
    total_orders: u64,
    metadata_orders: u64,
    missing_metadata_orders: u64,
    would_enter_orders: u64,
    would_skip_orders: u64,
    unknown_orders: u64,
    latest_metadata_age_seconds: Option<i64>,
    latest_route: Option<String>,
    latest_price_impact_pct: Option<f64>,
    latest_priority_fee_lamports: Option<u64>,
    decision_reasons: BTreeMap<String, u64>,
    provider_errors: BTreeMap<String, u64>,
    routes: BTreeMap<String, u64>,
}

impl WindowAccumulator {
    fn new(as_of: DateTime<Utc>, limit: u32) -> Self {
        Self {
            as_of,
            limit,
            total_orders: 0,
            metadata_orders: 0,
            missing_metadata_orders: 0,
            would_enter_orders: 0,
            would_skip_orders: 0,
            unknown_orders: 0,
            latest_metadata_age_seconds: None,
            latest_route: None,
            latest_price_impact_pct: None,
            latest_priority_fee_lamports: None,
            decision_reasons: BTreeMap::new(),
            provider_errors: BTreeMap::new(),
            routes: BTreeMap::new(),
        }
    }

    fn record(
        &mut self,
        latest: &ExecutionCanaryReadinessLatestOrder,
        readiness_status: &str,
        readiness_reason: &str,
    ) {
        self.total_orders += 1;
        if self.total_orders == 1 {
            self.record_latest_fields(latest);
        }

        increment(&mut self.routes, latest.route.as_str());
        if latest.metadata_recorded_ts.is_some() {
            self.metadata_orders += 1;
        } else {
            self.missing_metadata_orders += 1;
        }

        match readiness_status {
            READINESS_WOULD_ENTER => self.would_enter_orders += 1,
            READINESS_WOULD_SKIP => {
                self.would_skip_orders += 1;
                increment(&mut self.decision_reasons, readiness_reason);
            }
            _ => self.unknown_orders += 1,
        }
        record_provider_error(
            &mut self.provider_errors,
            "quote",
            latest.quote_status.as_deref(),
        );
        record_provider_error(
            &mut self.provider_errors,
            "priority_fee",
            latest.priority_fee_status.as_deref(),
        );
    }

    fn record_latest_fields(&mut self, latest: &ExecutionCanaryReadinessLatestOrder) {
        self.latest_route = Some(latest.route.clone());
        self.latest_price_impact_pct = latest.price_impact_pct;
        self.latest_priority_fee_lamports = latest.priority_fee_lamports;
        self.latest_metadata_age_seconds = latest.metadata_recorded_ts.map(|recorded_ts| {
            self.as_of
                .signed_duration_since(recorded_ts)
                .num_seconds()
                .max(0)
        });
    }

    fn finish(self) -> ExecutionCanaryReadinessWindowSummary {
        ExecutionCanaryReadinessWindowSummary {
            as_of: self.as_of,
            limit: self.limit,
            total_orders: self.total_orders,
            metadata_orders: self.metadata_orders,
            missing_metadata_orders: self.missing_metadata_orders,
            would_enter_orders: self.would_enter_orders,
            would_skip_orders: self.would_skip_orders,
            unknown_orders: self.unknown_orders,
            latest_metadata_age_seconds: self.latest_metadata_age_seconds,
            latest_route: self.latest_route,
            latest_price_impact_pct: self.latest_price_impact_pct,
            latest_priority_fee_lamports: self.latest_priority_fee_lamports,
            decision_reasons: into_counts(self.decision_reasons),
            provider_errors: into_counts(self.provider_errors),
            routes: into_counts(self.routes),
        }
    }
}

fn record_provider_error(counts: &mut BTreeMap<String, u64>, prefix: &str, status: Option<&str>) {
    let Some(status) = status else {
        return;
    };
    if status == QUOTE_STATUS_OK || status.trim().is_empty() {
        return;
    }
    increment(counts, &format!("{prefix}:{status}"));
}

fn increment(counts: &mut BTreeMap<String, u64>, key: &str) {
    *counts.entry(key.to_string()).or_insert(0) += 1;
}

fn into_counts(counts: BTreeMap<String, u64>) -> Vec<crate::ExecutionCanaryReadinessCount> {
    counts
        .into_iter()
        .map(|(key, count)| crate::ExecutionCanaryReadinessCount { key, count })
        .collect()
}
