//! Runner-local traversal after source refusals. Never caches source authority.
use copybot_storage_core::ExecutionRetryCursor;
use std::sync::{Arc, Mutex};

pub(crate) const SOURCE_REFUSAL_VISIT_BUDGET: usize = 8;
#[derive(Clone, Copy)]
pub(crate) enum Family {
    Candidate,
    Cooldown,
    UnknownSubmit,
    NotSent,
    Combined,
    FreshQuote,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct Continuation(Arc<Mutex<[Option<ExecutionRetryCursor>; 6]>>);
impl Continuation {
    pub(crate) fn after(&self, family: Family) -> Option<ExecutionRetryCursor> {
        self.0.lock().expect("source retry cursor lock")[family as usize].clone()
    }
    /// Normal successful selections keep their old priority. Only a local refusal
    /// starts continuation; a completed pass wraps and permits repaired A again.
    pub(crate) fn visited(&self, family: Family, cursor: ExecutionRetryCursor, refused: bool) {
        let mut points = self.0.lock().expect("source retry cursor lock");
        let point = &mut points[family as usize];
        if refused || point.is_some() {
            *point = Some(cursor);
        }
    }
    pub(crate) fn wrap(&self, family: Family) {
        self.0.lock().expect("source retry cursor lock")[family as usize] = None;
    }
}
