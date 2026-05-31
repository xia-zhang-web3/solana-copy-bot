use crate::execution_quote_canary_helpers::*;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionQuoteCanaryEventInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};
use serde_json::{json, Value};
use std::time::{Duration as StdDuration, Instant};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionQuoteCanaryTickSummary {
    pub entry_candidates: usize,
    pub entry_inserted: usize,
    pub entry_existing: usize,
    pub entry_errors: usize,
    pub close_candidates: usize,
    pub close_inserted: usize,
    pub close_existing: usize,
    pub close_errors: usize,
    pub last_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionQuoteCanaryRunner {
    config: ExecutionConfig,
    http: reqwest::Client,
}

impl ExecutionQuoteCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.quote_canary_enabled
    }

    pub(crate) async fn process_tick(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        let priority_fee_sample = if self.config.priority_fee_canary_enabled {
            Some(self.fetch_priority_fee_sample().await)
        } else {
            None
        };
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        self.process_entry_candidates(
            store,
            copy_signal_status,
            now,
            since,
            batch_limit,
            priority_fee_sample.as_ref(),
            &mut summary,
        )
        .await?;
        self.process_close_candidates(
            store,
            now,
            since,
            batch_limit,
            priority_fee_sample.as_ref(),
            &mut summary,
        )
        .await?;
        Ok(summary)
    }

    async fn process_entry_candidates(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: Option<&PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let signals = store
            .list_execution_quote_canary_entry_candidates(copy_signal_status, since, batch_limit)
            .context("failed loading execution quote canary entry candidates")?;
        summary.entry_candidates = signals.len();
        for signal in signals {
            let event = match self
                .build_entry_quote_event(store, &signal, now, priority_fee_sample)
                .await
            {
                Ok(event) => event,
                Err(error) => entry_error_event(&signal, now, &error),
            };
            if event.quote_status == QUOTE_STATUS_ERROR {
                summary.entry_errors += 1;
            }
            summary.last_event_id = Some(event.event_id.clone());
            match store
                .record_execution_quote_canary_event(&event)
                .with_context(|| {
                    format!(
                        "failed recording execution entry quote canary event {}",
                        event.event_id
                    )
                })? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.entry_inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.entry_existing += 1,
            }
        }
        Ok(())
    }

    async fn process_close_candidates(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: Option<&PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let closes = store
            .list_execution_quote_canary_close_candidates(since, batch_limit)
            .context("failed loading execution quote canary close candidates")?;
        summary.close_candidates = closes.len();
        for close in closes {
            let event = match self
                .build_close_quote_event(&close, now, priority_fee_sample)
                .await
            {
                Ok(event) => event,
                Err(error) => close_error_event(&close, now, &error),
            };
            if event.quote_status == QUOTE_STATUS_ERROR {
                summary.close_errors += 1;
            }
            summary.last_event_id = Some(event.event_id.clone());
            match store
                .record_execution_quote_canary_event(&event)
                .with_context(|| {
                    format!(
                        "failed recording execution close quote canary event {}",
                        event.event_id
                    )
                })? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.close_inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.close_existing += 1,
            }
        }
        Ok(())
    }

    async fn build_entry_quote_event(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryEventInsert> {
        let observed = load_matching_observed_entry_leg(store, signal)?;
        let mut event = ExecutionQuoteCanaryEventInsert {
            event_id: entry_quote_event_id(&signal.signal_id),
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
                .as_ref()
                .map(|value| value.sol_notional)
                .or(Some(signal.notional_sol)),
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_price_sol: None,
            shadow_price_sol: observed
                .as_ref()
                .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty)),
            slippage_bps: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            error: None,
        };
        attach_priority_fee(&mut event, priority_fee_sample);

        let amount = match sol_to_lamports_raw(self.config.quote_canary_buy_size_sol) {
            Ok(value) => value,
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
                return Ok(event);
            }
        };
        match self.fetch_quote(SOL_MINT, &signal.token, &amount).await {
            Ok(quote) => {
                apply_quote_sample_to_event(&mut event, quote);
                if let Some(decimals) = observed.as_ref().and_then(|value| value.token_decimals) {
                    let quote_in_sol = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), 9);
                    let quote_out_tokens =
                        raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), decimals);
                    event.quote_price_sol = quote_in_sol.and_then(|input| {
                        quote_out_tokens.and_then(|out| price_sol_per_token(input, out))
                    });
                    event.slippage_bps =
                        quote_slippage_bps_for_buy(event.quote_price_sol, event.shadow_price_sol);
                }
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        Ok(event)
    }

    async fn build_close_quote_event(
        &self,
        close: &ExecutionCanaryCloseCandidate,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryEventInsert> {
        let mut event = close_quote_event(close, now);
        attach_priority_fee(&mut event, priority_fee_sample);
        let Some(amount) = close
            .qty_raw
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            event.error = Some("missing exact close qty_raw".to_string());
            return Ok(event);
        };
        let Some(decimals) = close.qty_decimals else {
            event.error = Some("missing exact close qty_decimals".to_string());
            return Ok(event);
        };
        match self.fetch_quote(&close.token, SOL_MINT, amount).await {
            Ok(quote) => {
                apply_quote_sample_to_event(&mut event, quote);
                let quote_out_sol = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), 9);
                let quote_in_tokens =
                    raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), decimals);
                event.quote_price_sol = quote_out_sol.and_then(|out| {
                    quote_in_tokens.and_then(|input| price_sol_per_token(out, input))
                });
                event.slippage_bps =
                    quote_slippage_bps_for_sell(event.quote_price_sol, event.shadow_price_sol);
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        Ok(event)
    }

    async fn fetch_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_raw: &str,
    ) -> Result<QuoteSample> {
        let url = quote_url(&self.config.quote_canary_base_url)?;
        let timeout = StdDuration::from_millis(self.config.quote_canary_timeout_ms.max(1));
        let slippage_bps = self.config.quote_canary_slippage_bps.to_string();
        let started = Instant::now();
        let mut request = self
            .http
            .get(url)
            .query(&[
                ("inputMint", input_mint),
                ("outputMint", output_mint),
                ("amount", amount_raw),
                ("slippageBps", slippage_bps.as_str()),
                ("swapMode", "ExactIn"),
            ])
            .timeout(timeout);
        let api_key = self.config.quote_canary_api_key.trim();
        if !api_key.is_empty() {
            request = request.header("x-api-key", api_key);
        }
        let response = request
            .send()
            .await
            .context("quote canary request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "quote canary returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ));
        }
        quote_sample_from_json(
            response
                .json()
                .await
                .context("quote canary response JSON decode failed")?,
            started,
        )
    }

    async fn fetch_priority_fee_sample(&self) -> PriorityFeeSample {
        match self.fetch_priority_fee_sample_inner().await {
            Ok(sample) => sample,
            Err(error) => PriorityFeeSample {
                status: QUOTE_STATUS_ERROR.to_string(),
                lamports: None,
                json: None,
                error: Some(short_error(&error)),
            },
        }
    }

    async fn fetch_priority_fee_sample_inner(&self) -> Result<PriorityFeeSample> {
        let rpc_url = self.config.priority_fee_canary_rpc_url.trim();
        if rpc_url.is_empty() {
            return Err(anyhow!("priority fee canary RPC URL is empty"));
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "qn_estimatePriorityFees",
            "params": {
                "last_n_blocks": self.config.priority_fee_canary_last_n_blocks,
                "account": self.config.priority_fee_canary_account.trim(),
                "api_version": 2
            }
        });
        let response = self
            .http
            .post(rpc_url)
            .json(&body)
            .timeout(StdDuration::from_millis(
                self.config.priority_fee_canary_timeout_ms.max(1),
            ))
            .send()
            .await
            .context("priority fee canary request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "priority fee canary returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ));
        }
        let value: Value = response
            .json()
            .await
            .context("priority fee canary response JSON decode failed")?;
        if let Some(error) = value.get("error") {
            return Err(anyhow!("priority fee canary RPC error: {error}"));
        }
        let result = value.get("result").unwrap_or(&value);
        Ok(PriorityFeeSample {
            status: QUOTE_STATUS_OK.to_string(),
            lamports: priority_fee_lamports(result),
            json: Some(result.to_string()),
            error: None,
        })
    }
}

fn close_quote_event(
    close: &ExecutionCanaryCloseCandidate,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: close_quote_event_id(close.id),
        signal_id: Some(close.signal_id.clone()),
        shadow_closed_trade_id: Some(close.id),
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts: Some(close.closed_ts),
        decision_delay_ms: duration_ms_between(close.closed_ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(close.exit_value_sol),
        quote_in_amount_raw: close.qty_raw.clone(),
        quote_out_amount_raw: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(close.exit_value_sol, close.qty),
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        error: None,
    }
}

fn quote_sample_from_json(value: Value, started: Instant) -> Result<QuoteSample> {
    let in_amount = string_field(&value, "inAmount")
        .ok_or_else(|| anyhow!("quote canary response missing inAmount"))?;
    let out_amount = string_field(&value, "outAmount")
        .ok_or_else(|| anyhow!("quote canary response missing outAmount"))?;
    Ok(QuoteSample {
        in_amount,
        out_amount,
        price_impact_pct: numeric_field(&value, "priceImpactPct"),
        route_plan_json: value
            .get("routePlan")
            .map(|route| route.to_string())
            .filter(|raw| !raw.is_empty()),
        latency_ms: elapsed_ms(started),
    })
}
