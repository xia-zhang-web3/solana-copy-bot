#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskEventRow {
    pub rowid: i64,
    pub event_id: String,
    pub event_type: String,
    pub severity: String,
    pub ts: String,
    pub details_json: Option<String>,
}
