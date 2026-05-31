use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionCanaryObservedLeg, ExecutionDryRunRecordOutcome,
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration as StdDuration, Instant};
use tracing::info;

const CANARY_COPY_SIGNAL_STATUS: &str = "shadow_recorded";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUOTE_STATUS_OK: &str = "ok";
const QUOTE_STATUS_ERROR: &str = "error";
const QUOTE_STATUS_SKIPPED: &str = "skipped";
const SIDE_BUY: &str = "buy";
const SIDE_SELL: &str = "sell";

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionCanaryTickSummary {
    pub enabled: bool,
    pub dry_run: bool,
    pub route: String,
    pub wallet_pubkey: String,
    pub candidates: usize,
    pub inserted: usize,
    pub existing: usize,
    pub skipped_reason: Option<&'static str>,
    pub last_signal_id: Option<String>,
    pub last_order_id: Option<String>,
    pub last_error: Option<String>,
    pub quote_entry_candidates: usize,
    pub quote_entry_inserted: usize,
    pub quote_entry_existing: usize,
    pub quote_entry_errors: usize,
    pub quote_close_candidates: usize,
    pub quote_close_inserted: usize,
    pub quote_close_existing: usize,
    pub quote_close_errors: usize,
    pub last_quote_event_id: Option<String>,
}

impl ExecutionCanaryTickSummary {
    pub(crate) fn has_status_change(&self) -> bool {
        self.inserted > 0
            || self.existing > 0
            || self.skipped_reason.is_some()
            || self.quote_entry_inserted > 0
            || self.quote_entry_existing > 0
            || self.quote_entry_errors > 0
            || self.quote_close_inserted > 0
            || self.quote_close_existing > 0
            || self.quote_close_errors > 0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionCanaryRunner {
    config: ExecutionConfig,
    http: reqwest::Client,
}

impl ExecutionCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.canary_enabled
    }

    pub(crate) fn interval_seconds(&self) -> u64 {
        self.config.canary_interval_seconds.max(1)
    }

    pub(crate) fn log_startup_status(&self) {
        info!(
            enabled = self.config.canary_enabled,
            dry_run = self.config.canary_dry_run,
            route = %self.config.canary_route,
            wallet_pubkey = %self.config.canary_wallet_pubkey,
            buy_size_sol = self.config.canary_buy_size_sol,
            max_open_positions = self.config.canary_max_open_positions,
            max_daily_loss_sol = self.config.canary_max_daily_loss_sol,
            max_signal_age_seconds = self.config.canary_max_signal_age_seconds,
            kill_switch_path = %self.config.canary_kill_switch_path,
            quote_canary_enabled = self.config.quote_canary_enabled,
            quote_buy_size_sol = self.config.quote_canary_buy_size_sol,
            quote_slippage_bps = self.config.quote_canary_slippage_bps,
            quote_base_url_configured = !self.config.quote_canary_base_url.trim().is_empty(),
            quote_api_key_configured = !self.config.quote_canary_api_key.trim().is_empty(),
            priority_fee_canary_enabled = self.config.priority_fee_canary_enabled,
            priority_fee_rpc_url_configured = !self.config.priority_fee_canary_rpc_url.trim().is_empty(),
            "execution canary status"
        );
    }

    pub(crate) async fn process_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryTickSummary> {
        let mut summary = ExecutionCanaryTickSummary {
            enabled: self.config.canary_enabled,
            dry_run: self.config.canary_dry_run,
            route: self.config.canary_route.clone(),
            wallet_pubkey: self.config.canary_wallet_pubkey.clone(),
            ..ExecutionCanaryTickSummary::default()
        };
        if !self.config.canary_enabled {
            summary.skipped_reason = Some("disabled");
            return Ok(summary);
        }
        if Path::new(&self.config.canary_kill_switch_path).exists() {
            summary.skipped_reason = Some("kill_switch_active");
            return Ok(summary);
        }

        let since = now - Duration::seconds(self.config.canary_max_signal_age_seconds as i64);
        let priority_fee_sample =
            if self.config.quote_canary_enabled && self.config.priority_fee_canary_enabled {
                Some(self.fetch_priority_fee_sample().await)
            } else {
                None
            };
        let signals = store
            .list_execution_canary_candidates(
                CANARY_COPY_SIGNAL_STATUS,
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading execution canary candidates")?;
        summary.candidates = signals.len();
        for signal in signals {
            let outcome = store
                .record_execution_dry_run_order(&signal.signal_id, &self.config.canary_route, now)
                .with_context(|| {
                    format!(
                        "failed recording execution dry-run order for signal {}",
                        signal.signal_id
                    )
                })?;
            summary.last_signal_id = Some(signal.signal_id);
            match outcome {
                ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
            }
        }
        if self.config.quote_canary_enabled {
            self.process_quote_entry_candidates(
                store,
                now,
                since,
                priority_fee_sample.as_ref(),
                &mut summary,
            )
            .await?;
            self.process_quote_close_candidates(
                store,
                now,
                since,
                priority_fee_sample.as_ref(),
                &mut summary,
            )
            .await?;
        }
        if let Some(order) = store
            .latest_execution_dry_run_order()
            .context("failed loading latest execution dry-run order")?
        {
            summary.last_order_id = Some(order.order_id);
        }
        Ok(summary)
    }

    async fn process_quote_entry_candidates(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<()> {
        let signals = store
            .list_execution_quote_canary_entry_candidates(
                CANARY_COPY_SIGNAL_STATUS,
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading execution quote canary entry candidates")?;
        summary.quote_entry_candidates = signals.len();
        for signal in signals {
            let event = match self
                .build_entry_quote_event(store, &signal, now, priority_fee_sample)
                .await
            {
                Ok(event) => event,
                Err(error) => entry_error_event(&signal, now, &error),
            };
            if event.quote_status == QUOTE_STATUS_ERROR {
                summary.quote_entry_errors += 1;
            }
            summary.last_quote_event_id = Some(event.event_id.clone());
            match store
                .record_execution_quote_canary_event(&event)
                .with_context(|| {
                    format!(
                        "failed recording execution entry quote canary event {}",
                        event.event_id
                    )
                })? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.quote_entry_inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.quote_entry_existing += 1,
            }
        }
        Ok(())
    }

    async fn process_quote_close_candidates(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<()> {
        let closes = store
            .list_execution_quote_canary_close_candidates(
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading execution quote canary close candidates")?;
        summary.quote_close_candidates = closes.len();
        for close in closes {
            let event = match self
                .build_close_quote_event(&close, now, priority_fee_sample)
                .await
            {
                Ok(event) => event,
                Err(error) => close_error_event(&close, now, &error),
            };
            if event.quote_status == QUOTE_STATUS_ERROR {
                summary.quote_close_errors += 1;
            }
            summary.last_quote_event_id = Some(event.event_id.clone());
            match store
                .record_execution_quote_canary_event(&event)
                .with_context(|| {
                    format!(
                        "failed recording execution close quote canary event {}",
                        event.event_id
                    )
                })? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.quote_close_inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.quote_close_existing += 1,
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
        let leader_notional_sol = observed
            .as_ref()
            .map(|value| value.sol_notional)
            .or(Some(signal.notional_sol));
        let shadow_price_sol = observed
            .as_ref()
            .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty));
        let decision_delay_ms = duration_ms_between(signal.ts, now);
        let mut event = ExecutionQuoteCanaryEventInsert {
            event_id: entry_quote_event_id(&signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: SIDE_BUY.to_string(),
            quote_status: QUOTE_STATUS_SKIPPED.to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts.clone()),
            decision_delay_ms,
            quote_latency_ms: None,
            leader_notional_sol,
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_price_sol: None,
            shadow_price_sol,
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
                let token_decimals = observed.as_ref().and_then(|value| value.token_decimals);
                if let Some(decimals) = token_decimals {
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
        let decision_delay_ms = duration_ms_between(close.closed_ts, now);
        let shadow_price_sol = price_sol_per_token(close.exit_value_sol, close.qty);
        let mut event = ExecutionQuoteCanaryEventInsert {
            event_id: close_quote_event_id(close.id),
            signal_id: Some(close.signal_id.clone()),
            shadow_closed_trade_id: Some(close.id),
            wallet_id: close.wallet_id.clone(),
            token: close.token.clone(),
            side: SIDE_SELL.to_string(),
            quote_status: QUOTE_STATUS_SKIPPED.to_string(),
            request_ts: now,
            signal_ts: Some(close.closed_ts.clone()),
            decision_delay_ms,
            quote_latency_ms: None,
            leader_notional_sol: Some(close.exit_value_sol),
            quote_in_amount_raw: close.qty_raw.clone(),
            quote_out_amount_raw: None,
            quote_price_sol: None,
            shadow_price_sol,
            slippage_bps: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            error: None,
        };
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
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("<body unavailable>"));
            return Err(anyhow!(
                "quote canary returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ));
        }
        let value: Value = response
            .json()
            .await
            .context("quote canary response JSON decode failed")?;
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
        let timeout = StdDuration::from_millis(self.config.priority_fee_canary_timeout_ms.max(1));
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
            .timeout(timeout)
            .send()
            .await
            .context("priority fee canary request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("<body unavailable>"));
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

#[derive(Debug, Clone)]
struct QuoteSample {
    in_amount: String,
    out_amount: String,
    price_impact_pct: Option<f64>,
    route_plan_json: Option<String>,
    latency_ms: u64,
}

#[derive(Debug, Clone)]
struct PriorityFeeSample {
    status: String,
    lamports: Option<u64>,
    json: Option<String>,
    error: Option<String>,
}

fn apply_quote_sample_to_event(event: &mut ExecutionQuoteCanaryEventInsert, quote: QuoteSample) {
    event.quote_status = QUOTE_STATUS_OK.to_string();
    event.quote_in_amount_raw = Some(quote.in_amount);
    event.quote_out_amount_raw = Some(quote.out_amount);
    event.price_impact_pct = quote.price_impact_pct;
    event.route_plan_json = quote.route_plan_json;
    event.quote_latency_ms = Some(quote.latency_ms);
}

fn attach_priority_fee(
    event: &mut ExecutionQuoteCanaryEventInsert,
    sample: Option<&PriorityFeeSample>,
) {
    let Some(sample) = sample else {
        return;
    };
    event.priority_fee_status = Some(sample.status.clone());
    event.priority_fee_lamports = sample.lamports;
    event.priority_fee_json = sample.json.clone();
    if let Some(error) = &sample.error {
        append_event_error(event, format!("priority_fee: {error}"));
    }
}

fn load_matching_observed_entry_leg(
    store: &SqliteStore,
    signal: &CopySignalRow,
) -> Result<Option<ExecutionCanaryObservedLeg>> {
    let Some(signature) = signal_signature(&signal.signal_id) else {
        return Ok(None);
    };
    let observed = store.load_execution_canary_observed_leg_by_signature(signature)?;
    Ok(observed.filter(|leg| leg.is_buy && leg.token_mint == signal.token))
}

fn signal_signature(signal_id: &str) -> Option<&str> {
    if signal_id.trim().is_empty() {
        return None;
    }
    let mut parts = signal_id.split(':');
    if matches!(parts.next(), Some("shadow")) {
        parts.next().filter(|value| !value.trim().is_empty())
    } else {
        Some(signal_id)
    }
}

fn entry_quote_event_id(signal_id: &str) -> String {
    format!("quote:entry:{signal_id}")
}

fn close_quote_event_id(close_id: i64) -> String {
    format!("quote:close:{close_id}")
}

fn entry_error_event(
    signal: &CopySignalRow,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: entry_quote_event_id(&signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: SIDE_BUY.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: duration_ms_between(signal.ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: None,
        quote_out_amount_raw: None,
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        error: Some(short_error(error)),
    }
}

fn close_error_event(
    close: &ExecutionCanaryCloseCandidate,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: close_quote_event_id(close.id),
        signal_id: Some(close.signal_id.clone()),
        shadow_closed_trade_id: Some(close.id),
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
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
        error: Some(short_error(error)),
    }
}

fn quote_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("quote canary base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/quote") {
        Ok(without_slash.to_string())
    } else {
        Ok(format!("{without_slash}/quote"))
    }
}

fn sol_to_lamports_raw(sol: f64) -> Result<String> {
    if !sol.is_finite() || sol <= 0.0 {
        return Err(anyhow!("invalid SOL amount for quote canary: {sol}"));
    }
    let lamports = (sol * 1_000_000_000.0).round();
    if lamports < 1.0 || lamports > u64::MAX as f64 {
        return Err(anyhow!(
            "SOL amount cannot be represented in lamports: {sol}"
        ));
    }
    Ok((lamports as u64).to_string())
}

fn price_sol_per_token(sol: f64, tokens: f64) -> Option<f64> {
    if sol.is_finite() && tokens.is_finite() && sol >= 0.0 && tokens > 0.0 {
        Some(sol / tokens)
    } else {
        None
    }
}

fn quote_slippage_bps_for_buy(quote_price: Option<f64>, shadow_price: Option<f64>) -> Option<f64> {
    let (Some(quote_price), Some(shadow_price)) = (quote_price, shadow_price) else {
        return None;
    };
    if shadow_price > 0.0 {
        Some((quote_price - shadow_price) / shadow_price * 10_000.0)
    } else {
        None
    }
}

fn quote_slippage_bps_for_sell(quote_price: Option<f64>, shadow_price: Option<f64>) -> Option<f64> {
    let (Some(quote_price), Some(shadow_price)) = (quote_price, shadow_price) else {
        return None;
    };
    if shadow_price > 0.0 {
        Some((shadow_price - quote_price) / shadow_price * 10_000.0)
    } else {
        None
    }
}

fn raw_amount_to_ui(raw: Option<&str>, decimals: u8) -> Option<f64> {
    let value = raw?.parse::<f64>().ok()?;
    let divisor = 10f64.powi(i32::from(decimals));
    if value.is_finite() && divisor.is_finite() && divisor > 0.0 {
        Some(value / divisor)
    } else {
        None
    }
}

fn duration_ms_between(from: DateTime<Utc>, to: DateTime<Utc>) -> Option<u64> {
    let millis = to.signed_duration_since(from).num_milliseconds();
    u64::try_from(millis).ok()
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn numeric_field(value: &Value, field: &str) -> Option<f64> {
    match value.get(field)? {
        Value::Number(number) => number.as_f64(),
        Value::String(raw) => raw.parse::<f64>().ok(),
        _ => None,
    }
}

fn priority_fee_lamports(result: &Value) -> Option<u64> {
    json_u64(result.get("recommended"))
        .or_else(|| {
            result
                .get("per_compute_unit")
                .and_then(|value| json_u64(value.get("high")))
        })
        .or_else(|| {
            result
                .get("per_compute_unit")
                .and_then(|value| json_u64(value.get("medium")))
        })
        .or_else(|| {
            result
                .get("per_transaction")
                .and_then(|value| json_u64(value.get("high")))
        })
        .or_else(|| {
            result
                .get("per_transaction")
                .and_then(|value| json_u64(value.get("medium")))
        })
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(raw) => raw.parse::<u64>().ok(),
        _ => None,
    }
}

fn append_event_error(event: &mut ExecutionQuoteCanaryEventInsert, message: String) {
    event.error = Some(match event.error.take() {
        Some(current) if !current.is_empty() => format!("{current}; {message}"),
        _ => message,
    });
}

fn short_error(error: &anyhow::Error) -> String {
    truncate_for_log(&format!("{error:#}"), 500)
}

fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let mut out = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}
