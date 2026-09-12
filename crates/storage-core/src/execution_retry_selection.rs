/// Raw sort values from a selection query; not an execution grant or persisted proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRetryCursor {
    pub first: String,
    pub second: String,
    pub third: String,
}
pub struct ExecutionRetryOrder {
    pub order: crate::ExecutionCanaryOrder,
    pub cursor: ExecutionRetryCursor,
}
pub struct ExecutionRetryQuote {
    pub event_id: String,
    pub cursor: ExecutionRetryCursor,
}
