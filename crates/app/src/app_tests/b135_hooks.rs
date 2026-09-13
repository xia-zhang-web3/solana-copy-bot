//! Passive boundary counters only; cannot grant authority or alter control flow.
use std::sync::{Mutex, OnceLock};
static CALLS: OnceLock<Mutex<Vec<(String, String)>>> = OnceLock::new();
pub(crate) fn mark(kind: &str, id: &str) {
    CALLS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .push((kind.into(), id.into()));
}
pub(super) fn count(id: &str) -> usize {
    CALLS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, v)| v == id)
        .count()
}
