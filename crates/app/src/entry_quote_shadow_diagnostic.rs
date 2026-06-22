use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, duration_ms_between, load_matching_observed_entry_leg,
    observed_token_decimals, price_sol_per_token, quote_canary_slippage_limit_bps,
    quote_slippage_bps_for_buy, raw_amount_to_ui, short_error, sol_to_lamports_raw,
    QUOTE_STATUS_ERROR, QUOTE_STATUS_OK, QUOTE_STATUS_SKIPPED, SIDE_BUY, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};

const COPY_SIGNAL_STATUS_SHADOW_RECORDED: &str = "shadow_recorded";

#[derive(Debug, Clone)]
pub(crate) struct EntryQuoteShadowDiagnostic {
    config: ExecutionConfig,
    http: reqwest::Client,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct EntryQuoteShadowDiagnosticSummary {
    pub(crate) checked: u64,
    pub(crate) inserted: u64,
    pub(crate) existing: u64,
    pub(crate) quote_ok: u64,
    pub(crate) quote_error: u64,
    pub(crate) last_event_id: Option<String>,
}

impl EntryQuoteShadowDiagnostic {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn process_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<EntryQuoteShadowDiagnosticSummary> {
        if !self.config.entry_quote_shadow_diagnostic_enabled {
            return Ok(EntryQuoteShadowDiagnosticSummary::default());
        }
        let max_age_seconds = self
            .config
            .entry_quote_shadow_diagnostic_max_signal_age_seconds
            .max(1);
        let since = now - Duration::seconds(max_age_seconds.min(i64::MAX as u64) as i64);
        let candidates = store
            .list_entry_quote_shadow_diagnostic_candidates(
                COPY_SIGNAL_STATUS_SHADOW_RECORDED,
                since,
                self.config.entry_quote_shadow_diagnostic_batch_limit.max(1),
            )
            .context("failed loading entry quote shadow diagnostic candidates")?;
        let mut summary = EntryQuoteShadowDiagnosticSummary::default();
        for signal in candidates {
            summary.checked += 1;
            let event_id = entry_quote_shadow_diagnostic_event_id(&signal.signal_id);
            let event = self
                .quote_signal_event(store, signal, event_id.clone(), now)
                .await;
            match store.record_execution_quote_canary_event(&event)? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.existing += 1,
            }
            if event.quote_status == QUOTE_STATUS_OK {
                summary.quote_ok += 1;
            } else {
                summary.quote_error += 1;
            }
            summary.last_event_id = Some(event_id);
        }
        Ok(summary)
    }

    async fn quote_signal_event(
        &self,
        store: &SqliteStore,
        signal: CopySignalRow,
        event_id: String,
        now: DateTime<Utc>,
    ) -> ExecutionQuoteCanaryEventInsert {
        let observed = match load_matching_observed_entry_leg(store, &signal) {
            Ok(value) => value,
            Err(error) => {
                let mut event = self.base_event(&signal, None, event_id, now);
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
                return event;
            }
        };
        let mut event = self.base_event(&signal, observed.as_ref(), event_id, now);
        let amount = match sol_to_lamports_raw(self.config.quote_canary_buy_size_sol) {
            Ok(value) => value,
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
                return event;
            }
        };
        match self
            .quote_signal(
                &signal,
                &amount,
                observed.as_ref().and_then(observed_token_decimals),
            )
            .await
        {
            Ok(quote) => apply_quote_to_entry_event(&mut event, quote),
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        event
    }

    fn base_event(
        &self,
        signal: &CopySignalRow,
        observed: Option<&copybot_storage_core::ExecutionCanaryObservedLeg>,
        event_id: String,
        now: DateTime<Utc>,
    ) -> ExecutionQuoteCanaryEventInsert {
        ExecutionQuoteCanaryEventInsert {
            event_id,
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: SIDE_BUY.to_string(),
            quote_status: QUOTE_STATUS_SKIPPED.to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: duration_ms_between(signal.ts, now),
            quote_latency_ms: None,
            leader_notional_sol: observed
                .map(|value| value.sol_notional)
                .or(Some(signal.notional_sol)),
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_response_json: None,
            quote_price_sol: None,
            shadow_price_sol: observed
                .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty)),
            slippage_bps: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            decision_status: None,
            decision_reason: None,
            error: None,
        }
    }

    async fn quote_signal(
        &self,
        signal: &CopySignalRow,
        amount: &str,
        known_decimals: Option<u8>,
    ) -> Result<EntryQuote> {
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
        let sample = fetch_quote_sample(
            &self.http,
            &self.config,
            SOL_MINT,
            &signal.token,
            amount,
            limit_bps,
        )
        .await?;
        let decimals =
            resolve_spl_token_decimals(&self.http, &self.config, &signal.token, known_decimals)
                .await;
        Ok(EntryQuote {
            sample,
            token_decimals: decimals,
        })
    }
}

impl EntryQuoteShadowDiagnosticSummary {
    pub(crate) fn has_activity(&self) -> bool {
        self.checked > 0 || self.inserted > 0 || self.existing > 0
    }
}

pub(crate) fn entry_quote_shadow_diagnostic_event_id(signal_id: &str) -> String {
    format!("quote:entry-shadow-diag:{signal_id}")
}

struct EntryQuote {
    sample: crate::execution_quote_canary_helpers::QuoteSample,
    token_decimals: Option<u8>,
}

fn apply_quote_to_entry_event(event: &mut ExecutionQuoteCanaryEventInsert, quote: EntryQuote) {
    apply_quote_sample_to_event(event, quote.sample);
    if let Some(decimals) = quote.token_decimals {
        let quote_in_sol = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), 9);
        let quote_out_tokens = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), decimals);
        event.quote_price_sol = quote_in_sol
            .and_then(|sol| quote_out_tokens.and_then(|qty| price_sol_per_token(sol, qty)));
        event.slippage_bps =
            quote_slippage_bps_for_buy(event.quote_price_sol, event.shadow_price_sol);
    }
}
