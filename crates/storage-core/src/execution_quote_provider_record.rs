use super::*;

impl SqliteDiscoveryStore {
    pub fn record_execution_quote_canary_provider_sample(
        &self,
        sample: &ExecutionQuoteCanaryProviderSampleInsert,
    ) -> Result<ExecutionQuoteCanaryRecordOutcome> {
        ensure_execution_quote_canary_provider_samples_table(self)?;
        let availability = crate::quote_response_availability::Write::new(
            &self.conn,
            "execution_quote_canary_provider_samples",
            sample.quote_response_available_ts,
            sample.quote_status == "ok",
        )?;
        let available_column = availability.column;
        let available_value = availability.placeholder;
        let available_update = availability.update;
        let timing_available = crate::quote_http_timing::timing_write_available(
            &self.conn,
            "execution_quote_canary_provider_samples",
            sample.http_request_started_ts,
        )?;
        let actual_start = sample.http_request_started_ts.map(|ts| ts.to_rfc3339());
        let timing_column = if timing_available {
            ", http_request_started_ts"
        } else {
            ""
        };
        let timing_value = if timing_available { ", ?18" } else { "" };
        let timing_update = if timing_available {
            "http_request_started_ts=excluded.http_request_started_ts,"
        } else {
            ""
        };
        let quote_latency_ms = optional_u64_to_i64(
            "execution_quote_canary_provider_samples.quote_latency_ms",
            sample.quote_latency_ms,
        )?;
        let inserted = self
            .execute_with_retry(|conn| {

                        let original_values = params![
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
                    ];
                        let mut values = original_values.to_vec();
                        if timing_available { values.push(&actual_start); }
                        if availability.enabled { values.push(&availability.value); }
                        conn.execute(
                    &format!("INSERT OR IGNORE INTO execution_quote_canary_provider_samples(
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
                        error{timing_column}{available_column}
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17{timing_value}{available_value}
                    ) ON CONFLICT(event_id, provider) DO UPDATE SET
                        {timing_update}{available_update}
                        quote_status=excluded.quote_status, request_ts=excluded.request_ts,
                        quote_latency_ms=excluded.quote_latency_ms, quote_in_amount_raw=excluded.quote_in_amount_raw,
                        quote_out_amount_raw=excluded.quote_out_amount_raw, quote_response_json=excluded.quote_response_json,
                        quote_price_sol=excluded.quote_price_sol, shadow_price_sol=excluded.shadow_price_sol,
                        slippage_bps=excluded.slippage_bps, price_impact_pct=excluded.price_impact_pct,
                        route_plan_json=excluded.route_plan_json, decision_status=excluded.decision_status,
                        decision_reason=excluded.decision_reason, error=excluded.error
                    WHERE excluded.side = 'sell' AND execution_quote_canary_provider_samples.side = 'sell'
                      AND EXISTS(SELECT 1 FROM execution_quote_canary_events AS e
                          JOIN copy_signals AS s ON s.signal_id=e.signal_id
                          WHERE e.event_id=excluded.event_id AND e.request_ts=excluded.request_ts
                            AND s.status='execution_sell_intent'
                            AND NOT EXISTS(SELECT 1 FROM orders WHERE signal_id=s.signal_id))"),
                    values.as_slice(),
                )
            })
            .context("failed recording execution quote canary provider sample")?;
        if inserted > 0 {
            Ok(ExecutionQuoteCanaryRecordOutcome::Inserted)
        } else {
            Ok(ExecutionQuoteCanaryRecordOutcome::Existing)
        }
    }
}
