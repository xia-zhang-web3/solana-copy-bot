use std::sync::atomic::Ordering;

use super::{
    ObservedSwapRecentRawJournalConfig, ObservedSwapWriterConfig, ObservedSwapWriterTelemetry,
    OBSERVED_SWAP_BATCH_MAX_SIZE, OBSERVED_SWAP_DISCOVERY_AGGREGATE_OVERFLOW_CAPACITY_MULTIPLIER,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_CAP,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_MULTIPLIER,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP,
    OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES,
    OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES,
};

pub(super) fn observed_swap_writer_normal_try_enqueue_soft_limit(
    config: &ObservedSwapWriterConfig,
) -> usize {
    if let Some(limit) = config.normal_try_enqueue_soft_limit_override {
        return limit.max(1);
    }

    let discovery_critical_reserve_requests =
        observed_swap_writer_discovery_critical_reserve_requests(config);
    let normal_capacity = config
        .channel_capacity
        .saturating_sub(discovery_critical_reserve_requests)
        .max(1);
    if config.aggregate_writes_enabled {
        return normal_capacity;
    }

    // `try_enqueue()` is only used by non-critical irrelevant swaps. When aggregate writes
    // are disabled, one queued raw batch is enough to preserve best-effort persistence without
    // letting this lowest-priority class consume the entire normal writer budget before
    // backpressure starts.
    let noncritical_irrelevant_budget =
        OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES
            .saturating_mul(config.batch_max_size.max(1))
            .max(1);

    noncritical_irrelevant_budget.min(normal_capacity)
}

pub(super) fn recent_raw_journal_adaptive_write_coalesce_max_batches(
    config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    config
        .write_coalesce_max_batches
        .max(1)
        .saturating_mul(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_MULTIPLIER)
        .min(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_CAP)
        .max(config.write_coalesce_max_batches.max(1))
}

pub(super) fn recent_raw_journal_adaptive_write_coalesce_max_rows(
    config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    OBSERVED_SWAP_BATCH_MAX_SIZE
        .max(1)
        .saturating_mul(recent_raw_journal_adaptive_write_coalesce_max_batches(
            config,
        ))
        .min(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP)
        .max(OBSERVED_SWAP_BATCH_MAX_SIZE.max(1))
}

pub(super) fn recent_raw_journal_adaptive_coalesce_pressure(
    telemetry: &ObservedSwapWriterTelemetry,
) -> bool {
    telemetry.journal_queue_depth_batches() > 0
        || telemetry.journal_queue_row_debt() > 0
        || telemetry
            .journal_overflow_depth_batches
            .load(Ordering::Relaxed)
            > 0
        || telemetry.journal_overflow_row_debt.load(Ordering::Relaxed) > 0
}

pub(super) fn observed_swap_writer_aggregate_queue_capacity(
    config: &ObservedSwapWriterConfig,
) -> usize {
    if !config.aggregate_writes_enabled {
        return 0;
    }
    config
        .channel_capacity
        .max(1)
        .div_ceil(config.batch_max_size.max(1))
}

pub(super) fn observed_swap_writer_default_aggregate_overflow_capacity_batches(
    channel_capacity: usize,
    batch_max_size: usize,
    aggregate_writes_enabled: bool,
) -> usize {
    if !aggregate_writes_enabled {
        return 0;
    }
    channel_capacity
        .max(1)
        .div_ceil(batch_max_size.max(1))
        .saturating_mul(OBSERVED_SWAP_DISCOVERY_AGGREGATE_OVERFLOW_CAPACITY_MULTIPLIER)
}

pub(super) fn recent_raw_journal_overflow_row_capacity(
    batch_max_size: usize,
    journal_config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    journal_config
        .overflow_capacity_batches
        .max(1)
        .saturating_mul(batch_max_size.max(1))
        .saturating_mul(journal_config.write_coalesce_max_batches.max(1))
}

pub(super) fn observed_swap_writer_discovery_critical_reserve_requests(
    config: &ObservedSwapWriterConfig,
) -> usize {
    let reserved_requests = OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES
        .max(1)
        .saturating_mul(config.batch_max_size.max(1));
    reserved_requests.min(config.channel_capacity.saturating_sub(1))
}
