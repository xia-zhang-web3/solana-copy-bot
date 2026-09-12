//! Admission/completion run synchronously on the existing execution owner.
use super::{hot_observed::*, job::*, priority_retry::*, *};
use copybot_core_types::SwapEvent;

impl ExecutionQuoteCanaryRunner {
    pub(crate) fn entry_pending(&self, signal_id: &str) -> bool {
        self.entry_claims.contains_signal(signal_id)
    }
    pub(crate) fn prepare_hot_quote(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        now: DateTime<Utc>,
    ) -> Result<Option<HotQuoteAdmission>> {
        if !self.is_enabled() || !observed_swap_is_buy(swap) {
            return Ok(None);
        }
        let signal_id = observed_buy_signal_id(swap);
        let event_id = entry_quote_event_id(&signal_id);
        let Some(claim) = self.entry_claims.claim(&event_id) else {
            return Ok(None);
        };
        let observed = store
            .load_execution_canary_observed_leg_by_signature(&swap.signature)?
            .context("hot quote admission requires durable observation")?;
        anyhow::ensure!(
            observed.is_buy
                && observed.wallet_id == swap.wallet
                && observed.token_mint == swap.token_out
                && observed.slot == swap.slot
                && observed.ts_utc == swap.ts_utc
                && observed.token_qty == swap.amount_out
                && observed.sol_notional == swap.amount_in
                && observed.token_raw_amount.as_deref()
                    == swap
                        .exact_amounts
                        .as_ref()
                        .map(|e| e.amount_out_raw.as_str())
                && observed.token_decimals
                    == swap.exact_amounts.as_ref().map(|e| e.amount_out_decimals),
            "hot quote observation identity mismatch"
        );
        let mut existing = store.load_latest_execution_quote_canary_entry_event(&signal_id)?;
        if let Some(event) = &existing {
            anyhow::ensure!(
                event.event_id == event_id
                    && event.signal_id.as_deref() == Some(&signal_id)
                    && event.wallet_id == swap.wallet
                    && event.token == swap.token_out
                    && event.side == SIDE_BUY,
                "hot quote existing entry identity mismatch"
            );
            if !entry_event_needs_priority_fee_retry(event)
                || !self.config.priority_fee_canary_enabled
            {
                return Ok(None);
            }
        }
        if self.owner_decisions {
            if let Some(event) = &existing {
                existing = Some(store.pend_execution_quote_entry(event)?);
            }
        }
        let job = HotQuoteJob::new(
            self.http.clone(),
            self.config.clone(),
            self.priority_fee.clone(),
            swap.clone(),
            now,
            existing.is_some(),
        );
        Ok(Some(HotQuoteAdmission {
            job,
            origin: HotQuoteOrigin {
                swap: swap.clone(),
                observed,
                requested_at: now,
                existing,
                wait_for_shadow: false,
                saved_entry: None,
                shadow_finished: false,
                shadow_recorded: None,
                buy_receipt: None,
                _claim: claim,
            },
        }))
    }

    pub(crate) fn finish_hot_quote(
        &self,
        store: &SqliteStore,
        origin: &HotQuoteOrigin,
        output: std::result::Result<HotQuoteOutput, &'static str>,
        now: DateTime<Utc>,
        gate_refusal: Option<&'static str>,
    ) -> Result<&'static str> {
        let signal = origin.signal_id();
        let current = store.load_latest_execution_quote_canary_entry_event(&signal)?;
        // Even a valid late completion cannot supplement a different winner's samples.
        if current != origin.existing {
            return Ok("hot_quote_entry_changed");
        }
        let output = match output {
            Ok(HotQuoteOutput::Fresh(bundle)) if origin.wait_for_shadow => {
                anyhow::ensure!(
                    origin.existing.is_none()
                        && bundle.event.event_id == origin.event_id()
                        && bundle.event.signal_id.as_deref() == Some(&origin.signal_id())
                        && bundle.event.wallet_id == origin.swap.wallet
                        && bundle.event.token == origin.swap.token_out
                        && bundle.event.side == SIDE_BUY
                        && bundle.event.request_ts == origin.requested_at
                        && bundle.event.signal_ts == Some(origin.swap.ts_utc),
                    "hot quote completion identity mismatch"
                );
                let mut summary = ExecutionQuoteCanaryTickSummary::default();
                self.record_entry_event(store, bundle, &mut summary)?;
                if let Some(reason) = self.hot_origin_refusal(store, origin, now, gate_refusal)? {
                    self.refuse_hot_buy(store, origin, reason)?;
                    return Ok(reason);
                }
                return Ok(if summary.entry_errors > 0 {
                    "hot_quote_error_recorded"
                } else {
                    "hot_quote_recorded"
                });
            }
            other => other,
        };
        let refusal = self.hot_origin_refusal(store, origin, now, gate_refusal)?;
        if let Some(reason) = refusal {
            if origin.wait_for_shadow {
                self.refuse_hot_buy(store, origin, reason)?;
            } else {
                self.refuse_hot_quote(store, origin, reason)?;
            }
            return Ok(reason);
        }
        match output {
            Err(reason) => {
                self.refuse_hot_quote(store, origin, reason)?;
                Ok(reason)
            }
            Ok(HotQuoteOutput::Fresh(bundle)) => {
                anyhow::ensure!(
                    origin.existing.is_none()
                        && bundle.event.event_id == origin.event_id()
                        && bundle.event.signal_id.as_deref() == Some(&origin.signal_id())
                        && bundle.event.wallet_id == origin.swap.wallet
                        && bundle.event.token == origin.swap.token_out
                        && bundle.event.side == SIDE_BUY
                        && bundle.event.request_ts == origin.requested_at
                        && bundle.event.signal_ts == Some(origin.swap.ts_utc),
                    "hot quote completion identity mismatch"
                );
                let mut summary = ExecutionQuoteCanaryTickSummary::default();
                self.record_entry_event(store, bundle, &mut summary)?;
                Ok(if summary.entry_errors > 0 {
                    "hot_quote_error_recorded"
                } else {
                    "hot_quote_recorded"
                })
            }
            Ok(HotQuoteOutput::ExistingPriority(priority)) => {
                let Some(mut event) = current else {
                    return Ok("hot_quote_entry_changed");
                };
                if let Some(priority) = priority.filter(priority_fee_sample_is_usable) {
                    if origin.wait_for_shadow || entry_event_needs_priority_fee_retry(&event) {
                        self.mark_event_priority_fee_ok(store, &mut event, &priority)?;
                    }
                }
                Ok("hot_quote_priority_completed")
            }
        }
    }
    pub(crate) fn hot_origin_refusal(
        &self,
        store: &SqliteStore,
        origin: &HotQuoteOrigin,
        now: DateTime<Utc>,
        gate_refusal: Option<&'static str>,
    ) -> Result<Option<&'static str>> {
        let signal = origin.signal_id();
        if store.execution_quote_entry_refused(&signal)? {
            return Ok(Some("hot_buy_refused"));
        }
        let mut refusal = gate_refusal;
        if !self.is_enabled()
            || !self.config.canary_enabled
            || std::path::Path::new(&self.config.canary_kill_switch_path).exists()
        {
            refusal = Some("hot_quote_disabled");
        }
        let max_age = chrono::Duration::seconds(self.config.canary_max_signal_age_seconds as i64);
        if now < origin.swap.ts_utc
            || now - origin.swap.ts_utc > max_age
            || now < origin.requested_at
            || now - origin.requested_at > max_age
        {
            refusal = Some("hot_quote_stale");
        }
        if store
            .load_execution_canary_observed_leg_by_signature(&origin.swap.signature)?
            .as_ref()
            != Some(&origin.observed)
        {
            refusal = Some("hot_quote_observation_changed");
        }
        if !store.was_wallet_followed_at(&origin.swap.wallet, origin.swap.ts_utc)?
            || !store.was_wallet_followed_at(&origin.swap.wallet, now)?
        {
            refusal = Some("hot_quote_source_changed");
        }
        if let Some(signal) = store.load_copy_signal_by_signal_id(&signal)? {
            if signal.wallet_id != origin.swap.wallet
                || signal.token != origin.swap.token_out
                || signal.side != SIDE_BUY
                || origin.shadow_recorded.as_ref().is_some_and(|recorded| {
                    recorded.signal_id != signal.signal_id
                        || recorded.wallet_id != signal.wallet_id
                        || recorded.token != signal.token
                        || recorded.side != signal.side
                        || recorded.notional_sol.to_bits() != signal.notional_sol.to_bits()
                })
            {
                refusal = Some("hot_quote_signal_changed");
            }
        }
        Ok(refusal)
    }
    pub(crate) fn refuse_hot_buy(
        &self,
        store: &SqliteStore,
        origin: &HotQuoteOrigin,
        reason: &'static str,
    ) -> Result<()> {
        if let Some(event) = store.load_execution_quote_canary_event_by_id(&origin.event_id())? {
            anyhow::ensure!(
                event.signal_id.as_deref() == Some(&origin.signal_id())
                    && event.wallet_id == origin.swap.wallet
                    && event.token == origin.swap.token_out
                    && event.signal_ts == Some(origin.swap.ts_utc),
                "hot BUY refusal entry identity changed"
            );
            return store.mark_execution_quote_entry_refused(&event, reason);
        }
        let mut event = hot_observed_buy_error_event(
            &origin.signal_id(),
            &origin.swap,
            origin.requested_at,
            &anyhow::anyhow!(reason),
        );
        event.decision_reason = Some(format!("hot_buy_refused:{reason}"));
        // A refusal is not a provider sample and cannot overwrite an existing winner.
        store.record_execution_quote_canary_event(&event)?;
        Ok(())
    }
    pub(crate) fn refuse_hot_quote(
        &self,
        store: &SqliteStore,
        origin: &HotQuoteOrigin,
        reason: &'static str,
    ) -> Result<()> {
        if origin.existing.is_some() {
            return Ok(());
        }
        let event = hot_observed_buy_error_event(
            &origin.signal_id(),
            &origin.swap,
            origin.requested_at,
            &anyhow::anyhow!(reason),
        );
        // A refusal is not a provider sample and cannot overwrite an existing winner.
        store.record_execution_quote_canary_event(&event)?;
        Ok(())
    }
}
