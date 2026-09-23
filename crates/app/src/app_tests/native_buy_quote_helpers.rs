//! Test-only quote operands for native BUY; production quote paths remain unchanged.
use crate::execution_build_plan_refresh::{
    apply_fresh_quote, refresh_tiny_buy_build_plan_metadata_inner,
};
use crate::execution_canary_route::NativeBuyGuard;
use crate::execution_quote_canary::{
    buy_quote_price_and_slippage, ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary,
    QuoteEventBundle,
};
use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, quote_canary_slippage_limit_bps, PriorityFeeSample, QuoteSample,
    SIDE_BUY,
};
use crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS;
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{native_buy, SqliteStore};

pub(crate) async fn refresh_tiny_buy_build_plan_metadata_with_external_quote(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    signal: &CopySignalRow,
    metadata: ExecutionBuildPlanMetadata,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
) -> Result<ExecutionBuildPlanMetadata> {
    refresh_tiny_buy_build_plan_metadata_inner(http, config, signal, metadata, Some(quote)).await
}

pub(crate) fn refresh_tiny_buy_build_plan_metadata_with_mock_external_quote(
    config: &ExecutionConfig,
    metadata: ExecutionBuildPlanMetadata,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
) -> ExecutionBuildPlanMetadata {
    apply_fresh_quote(
        metadata,
        quote,
        quote_canary_slippage_limit_bps(config, SIDE_BUY),
        QUOTE_SOURCE_GENERIC_METIS,
    )
}

impl ExecutionQuoteCanaryRunner {
    pub(crate) fn build_entry_quote_event_with_mock_external_quote(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        quote: QuoteSample,
        priority: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        let (mut event, token_decimals) =
            self.initial_entry_quote_event(store, signal, now, priority)?;
        let decimals = token_decimals.ok_or_else(|| anyhow::anyhow!("source_decimals_missing"))?;
        apply_quote_sample_to_event(&mut event, quote);
        (event.quote_price_sol, event.slippage_bps) =
            buy_quote_price_and_slippage(&event, decimals);
        Ok(QuoteEventBundle::event_only(event))
    }

    pub(crate) async fn build_entry_quote_event_with_external_quote(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
        quote: QuoteSample,
    ) -> Result<QuoteEventBundle> {
        self.build_entry_quote_event_inner(store, signal, now, priority_fee_sample, Some(quote))
            .await
    }

    pub(crate) fn process_native_buy_signal_with_mock_external_quote(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        guard: &NativeBuyGuard,
        now: DateTime<Utc>,
        quote: QuoteSample,
        priority: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        ensure!(
            self.is_enabled() && signal.status == native_buy::STATUS,
            "native_buy_quote_disabled"
        );
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        if !guard.check(store)? {
            return Ok(summary);
        }
        summary.entry_candidates = 1;
        let bundle = self.build_entry_quote_event_with_mock_external_quote(
            store, signal, now, quote, priority,
        )?;
        self.record_native_buy_bundle(store, guard, bundle, &mut summary)?;
        Ok(summary)
    }
}
