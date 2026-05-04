fn should_request_persisted_stream_catch_up(telemetry: &PersistedStreamProgressTelemetry) -> bool {
    if telemetry.phase == DiscoveryPersistedRebuildPhase::Replay {
        return telemetry.budget_exhausted_reason.is_some();
    }
    if telemetry.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints {
        return false;
    }
    matches!(
        (
            telemetry.collect_buy_mints_mode,
            telemetry.budget_exhausted_reason,
        ),
        (_, Some(PersistedStreamBudgetExhaustedReason::PageBudget))
            | (
                CollectBuyMintsMode::FreshScan,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget)
            )
    )
}

fn should_request_persisted_stream_catch_up_pressure_override(
    telemetry: &PersistedStreamProgressTelemetry,
) -> bool {
    (telemetry.phase == DiscoveryPersistedRebuildPhase::Replay
        && telemetry.replay_subphase == Some("sol_leg")
        && telemetry.budget_exhausted_reason.is_some())
        || (telemetry.phase == DiscoveryPersistedRebuildPhase::Replay
            && telemetry.replay_subphase == Some("wallet_stats")
            && !telemetry.replay_wallet_stats_complete
            && telemetry.replay_wallet_stats_wallet_cursor.is_some()
            && matches!(
                telemetry.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget)
            )
            && telemetry.cycle_pages_processed > 0
            && telemetry.cycle_rows_processed > 0
            && telemetry.wallets_buffered > 0)
        || (telemetry.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints
            && telemetry.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
            && matches!(
                telemetry.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget)
            )
            && telemetry.collect_buy_mints_cursor_token.is_some()
            && telemetry.cycle_pages_processed > 0
            && telemetry.cycle_rows_processed > 0
            && telemetry.cycle_unique_buy_mints_discovered > 0
            && telemetry.wallets_buffered == 0)
}

#[derive(Debug)]
enum PersistedStreamRebuildAdvanceOutcome {
    Completed {
        snapshots: Vec<WalletSnapshot>,
        telemetry: PersistedStreamProgressTelemetry,
    },
    InProgress {
        telemetry: PersistedStreamProgressTelemetry,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedStreamRebuildRestoreOutcome {
    StartedFresh,
    ResumedExisting,
    CarriedForwardMetricsWindow,
    ResumedStaleMetricsWindow,
}

impl PersistedStreamRebuildRestoreOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::StartedFresh => "started_fresh",
            Self::ResumedExisting => "resumed_existing",
            Self::CarriedForwardMetricsWindow => "carried_forward_metrics_window",
            Self::ResumedStaleMetricsWindow => "resumed_stale_metrics_window",
        }
    }
}

#[derive(Debug, Clone)]
struct PersistedStreamPhaseAdvance {
    rows_processed: usize,
    pages_processed: usize,
    replay_wallet_stats_rows_processed: usize,
    replay_wallet_stats_pages_processed: usize,
    replay_sol_leg_rows_processed: usize,
    replay_sol_leg_pages_processed: usize,
    replay_sol_leg_elapsed_ms: u64,
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    source_exhausted: bool,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_cursor_token: Option<String>,
    unique_buy_mints_discovered: usize,
    budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
}
