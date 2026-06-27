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
    pub would_execute: usize,
    pub would_force_exit: usize,
    pub would_skip: usize,
    pub decision_unknown: usize,
    pub last_event_id: Option<String>,
}
