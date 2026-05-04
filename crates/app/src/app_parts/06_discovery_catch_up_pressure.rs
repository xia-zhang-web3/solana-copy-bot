#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiscoveryCatchUpBlockReason {
    ShadowQueueFull,
    WriterPendingRequests,
    WriterAggregateQueueDepth,
    WriterAggregateOverflowDepth,
    WriterJournalQueueDepth,
    WriterJournalOverflowDepth,
    YellowstoneOutputQueueFill,
}

impl DiscoveryCatchUpBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ShadowQueueFull => "shadow_queue_full",
            Self::WriterPendingRequests => "writer_pending_requests",
            Self::WriterAggregateQueueDepth => "writer_aggregate_queue_depth_batches",
            Self::WriterAggregateOverflowDepth => "writer_aggregate_overflow_depth_batches",
            Self::WriterJournalQueueDepth => "writer_journal_queue_depth_batches",
            Self::WriterJournalOverflowDepth => "writer_journal_overflow_depth_batches",
            Self::YellowstoneOutputQueueFill => "yellowstone_output_queue_fill_ratio",
        }
    }
}

fn discovery_catch_up_has_writer_queue_pressure(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
) -> bool {
    observed_swap_writer_snapshot.aggregate_queue_depth_batches > 0
        || observed_swap_writer_snapshot.aggregate_overflow_depth_batches > 0
        || observed_swap_writer_snapshot.journal_queue_depth_batches > 0
        || observed_swap_writer_snapshot.journal_overflow_depth_batches > 0
}
