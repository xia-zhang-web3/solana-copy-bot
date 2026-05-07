use super::*;

impl ShadowRiskGuard {
    pub(crate) fn compute_infra_block_signal(
        &self,
        now: DateTime<Utc>,
    ) -> Option<InfraBlockSignal> {
        if self.infra_samples.is_empty() {
            return None;
        }
        let latest = self.infra_samples.back().copied()?;

        if let Some(started_at) = self.lag_breach_since {
            if now - started_at
                >= chrono::Duration::minutes(
                    self.config.shadow_infra_lag_breach_minutes.max(1) as i64
                )
            {
                return Some(InfraBlockSignal {
                    key: InfraBlockKey::LagP95,
                    reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                        &latest,
                        format!(
                            "lag_p95_over_threshold_for={}m threshold_ms={}",
                            self.config.shadow_infra_lag_breach_minutes,
                            self.config.shadow_infra_lag_p95_threshold_ms
                        ),
                    ),
                });
            }
        }

        let window_start =
            now - chrono::Duration::minutes(self.config.shadow_infra_window_minutes.max(1) as i64);
        let has_complete_window_coverage = self
            .infra_samples
            .front()
            .map(|sample| sample.ts_utc <= window_start)
            .unwrap_or(false);
        let baseline = self
            .infra_samples
            .iter()
            .copied()
            .find(|sample| sample.ts_utc >= window_start)
            .unwrap_or_else(|| self.infra_samples.front().copied().unwrap_or(latest));

        let delta_enqueued = latest
            .ws_notifications_enqueued
            .saturating_sub(baseline.ws_notifications_enqueued);
        let delta_replaced = latest
            .ws_notifications_replaced_oldest
            .saturating_sub(baseline.ws_notifications_replaced_oldest);
        let delta_grpc_transaction_updates_total = latest
            .grpc_transaction_updates_total
            .saturating_sub(baseline.grpc_transaction_updates_total);
        let delta_parse_rejected_total = latest
            .parse_rejected_total
            .saturating_sub(baseline.parse_rejected_total);
        let delta_grpc_decode_errors = latest
            .grpc_decode_errors
            .saturating_sub(baseline.grpc_decode_errors);
        let delta_rpc_429 = latest.rpc_429.saturating_sub(baseline.rpc_429);
        let delta_rpc_5xx = latest.rpc_5xx.saturating_sub(baseline.rpc_5xx);

        const INFRA_PARSER_STALL_MIN_TX_UPDATES: u64 = 25;
        const INFRA_PARSER_STALL_ERROR_RATIO_THRESHOLD: f64 = 0.95;

        if has_complete_window_coverage
            && delta_enqueued == 0
            && delta_replaced == 0
            && delta_grpc_transaction_updates_total == 0
            && delta_rpc_429 == 0
            && delta_rpc_5xx == 0
        {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::NoIngestionProgress,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "no_ingestion_progress_for={}m",
                        self.config.shadow_infra_window_minutes.max(1)
                    ),
                ),
            });
        }

        if has_complete_window_coverage
            && delta_enqueued == 0
            && delta_grpc_transaction_updates_total >= INFRA_PARSER_STALL_MIN_TX_UPDATES
        {
            let parser_errors_delta =
                delta_parse_rejected_total.saturating_add(delta_grpc_decode_errors);
            if parser_errors_delta > 0 {
                let parser_error_ratio =
                    parser_errors_delta as f64 / delta_grpc_transaction_updates_total as f64;
                if parser_error_ratio >= INFRA_PARSER_STALL_ERROR_RATIO_THRESHOLD {
                    return Some(InfraBlockSignal {
                        key: InfraBlockKey::ParserStall,
                        reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                            &latest,
                            format!(
                                "parser_stall_for={}m tx_updates_delta={} parser_errors_delta={} error_ratio={:.4}",
                                self.config.shadow_infra_window_minutes.max(1),
                                delta_grpc_transaction_updates_total,
                                parser_errors_delta,
                                parser_error_ratio
                            ),
                        ),
                    });
                }
            }
        }

        if delta_enqueued > 0 {
            let replaced_ratio = delta_replaced as f64 / delta_enqueued as f64;
            if replaced_ratio > self.config.shadow_infra_replaced_ratio_threshold {
                if !self.uses_yellowstone_ingestion() {
                    return Some(InfraBlockSignal {
                        key: InfraBlockKey::ReplacedRatio,
                        reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                            &latest,
                            format!(
                                "replaced_ratio={:.4} delta_replaced={} delta_enqueued={}",
                                replaced_ratio, delta_replaced, delta_enqueued
                            ),
                        ),
                    });
                }
            }
        }
        if delta_rpc_429 >= self.config.shadow_infra_rpc429_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc429,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "rpc_429_delta={} threshold={}",
                        delta_rpc_429, self.config.shadow_infra_rpc429_delta_threshold
                    ),
                ),
            });
        }
        if delta_rpc_5xx >= self.config.shadow_infra_rpc5xx_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc5xx,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "rpc_5xx_delta={} threshold={}",
                        delta_rpc_5xx, self.config.shadow_infra_rpc5xx_delta_threshold
                    ),
                ),
            });
        }
        None
    }

    pub(crate) fn uses_yellowstone_ingestion(&self) -> bool {
        self.ingestion_source == "yellowstone_grpc"
    }
}
