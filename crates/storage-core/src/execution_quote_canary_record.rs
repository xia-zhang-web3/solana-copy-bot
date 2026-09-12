use super::*;

impl SqliteDiscoveryStore {
    pub fn record_execution_quote_canary_event(
        &self,
        event: &ExecutionQuoteCanaryEventInsert,
    ) -> Result<ExecutionQuoteCanaryRecordOutcome> {
        ensure_execution_quote_canary_tables(self)?;
        let availability = crate::quote_response_availability::Write::new(
            &self.conn,
            "execution_quote_canary_events",
            event.quote_response_available_ts,
            event.quote_status == "ok",
        )?;
        let available_column = availability.column;
        let available_value = availability.placeholder;
        let available_update = availability.update;
        let timing_available = crate::quote_http_timing::timing_write_available(
            &self.conn,
            "execution_quote_canary_events",
            event.http_request_started_ts,
        )?;
        let actual_start = event.http_request_started_ts.map(|ts| ts.to_rfc3339());
        let timing_column = if timing_available {
            ", http_request_started_ts"
        } else {
            ""
        };
        let timing_value = if timing_available { ", ?27" } else { "" };
        let timing_update = if timing_available {
            "http_request_started_ts=excluded.http_request_started_ts,"
        } else {
            ""
        };
        let quote_latency_ms = optional_u64_to_i64(
            "execution_quote_canary_events.quote_latency_ms",
            event.quote_latency_ms,
        )?;
        let decision_delay_ms = optional_u64_to_i64(
            "execution_quote_canary_events.decision_delay_ms",
            event.decision_delay_ms,
        )?;
        let priority_fee_lamports = optional_u64_to_i64(
            "execution_quote_canary_events.priority_fee_lamports",
            event.priority_fee_lamports,
        )?;
        let existed: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_quote_canary_events WHERE event_id = ?1)",
            [&event.event_id],
            |row| row.get(0),
        )?;
        let inserted = self
            .execute_with_retry(|conn| {

                        let original_values = params![
                        &event.event_id,
                        event.signal_id.as_deref(),
                        event.shadow_closed_trade_id,
                        &event.wallet_id,
                        &event.token,
                        &event.side,
                        &event.quote_status,
                        event.request_ts.to_rfc3339(),
                        event.signal_ts.as_ref().map(DateTime::to_rfc3339),
                        decision_delay_ms,
                        quote_latency_ms,
                        event.leader_notional_sol,
                        event.quote_in_amount_raw.as_deref(),
                        event.quote_out_amount_raw.as_deref(),
                        event.quote_response_json.as_deref(),
                        event.quote_price_sol,
                        event.shadow_price_sol,
                        event.slippage_bps,
                        event.price_impact_pct,
                        event.route_plan_json.as_deref(),
                        event.priority_fee_status.as_deref(),
                        priority_fee_lamports,
                        event.priority_fee_json.as_deref(),
                        event.decision_status.as_deref(),
                        event.decision_reason.as_deref(),
                        event.error.as_deref(),
                    ];
                        let mut values = original_values.to_vec();
                        if timing_available { values.push(&actual_start); }
                        if availability.enabled { values.push(&availability.value); }
                        conn.execute(
                    &format!("INSERT OR IGNORE INTO execution_quote_canary_events(
                        event_id,
                        signal_id,
                        shadow_closed_trade_id,
                        wallet_id,
                        token,
                        side,
                        quote_status,
                        request_ts,
                        signal_ts,
                        decision_delay_ms,
                        quote_latency_ms,
                        leader_notional_sol,
                        quote_in_amount_raw,
                        quote_out_amount_raw,
                        quote_response_json,
                        quote_price_sol,
                        shadow_price_sol,
                        slippage_bps,
                        price_impact_pct,
                        route_plan_json,
                        priority_fee_status,
                        priority_fee_lamports,
                        priority_fee_json,
                        decision_status,
                        decision_reason,
                        error{timing_column}{available_column}
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
                        ?24, ?25, ?26{timing_value}{available_value}
                    ) ON CONFLICT(event_id) DO UPDATE SET
                        {timing_update}{available_update}
                        quote_status=excluded.quote_status, request_ts=excluded.request_ts,
                        decision_delay_ms=excluded.decision_delay_ms, quote_latency_ms=excluded.quote_latency_ms,
                        leader_notional_sol=excluded.leader_notional_sol, quote_in_amount_raw=excluded.quote_in_amount_raw,
                        quote_out_amount_raw=excluded.quote_out_amount_raw, quote_response_json=excluded.quote_response_json,
                        quote_price_sol=excluded.quote_price_sol, shadow_price_sol=excluded.shadow_price_sol,
                        slippage_bps=excluded.slippage_bps, price_impact_pct=excluded.price_impact_pct,
                        route_plan_json=excluded.route_plan_json, priority_fee_status=excluded.priority_fee_status,
                        priority_fee_lamports=excluded.priority_fee_lamports, priority_fee_json=excluded.priority_fee_json,
                        decision_status=excluded.decision_status, decision_reason=excluded.decision_reason, error=excluded.error
                    WHERE execution_quote_canary_events.signal_id IS excluded.signal_id
                      AND execution_quote_canary_events.signal_ts IS excluded.signal_ts
                      AND execution_quote_canary_events.wallet_id = excluded.wallet_id
                      AND execution_quote_canary_events.token = excluded.token
                      AND execution_quote_canary_events.side = 'sell' AND excluded.side = 'sell'
                      AND execution_quote_canary_events.shadow_closed_trade_id IS NULL
                      AND excluded.shadow_closed_trade_id IS NULL
                      AND EXISTS(SELECT 1 FROM copy_signals WHERE signal_id=excluded.signal_id AND status='execution_sell_intent')
                      AND NOT EXISTS(SELECT 1 FROM orders WHERE signal_id=excluded.signal_id)"),
                    values.as_slice(),
                )
            })
            .context("failed recording execution quote canary event")?;
        if inserted > 0 && !existed {
            Ok(ExecutionQuoteCanaryRecordOutcome::Inserted)
        } else {
            Ok(ExecutionQuoteCanaryRecordOutcome::Existing)
        }
    }
}
