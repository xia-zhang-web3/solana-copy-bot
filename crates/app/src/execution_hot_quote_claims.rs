//! Canonical entry claims outlive shadow-worker completion and never cross restart.
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Default)]
pub(crate) struct EntryClaims(Arc<Mutex<HashSet<String>>>);

#[derive(Debug)]
pub(crate) struct EntryClaim {
    id: String,
    claims: EntryClaims,
}
impl EntryClaims {
    pub(crate) fn contains_signal(&self, signal: &str) -> bool {
        self.0
            .lock()
            .expect("entry claims lock")
            .contains(&crate::execution_quote_canary_helpers::entry_quote_event_id(signal))
    }
    pub(crate) fn claim(&self, event_id: &str) -> Option<EntryClaim> {
        self.0
            .lock()
            .expect("entry claims lock")
            .insert(event_id.to_owned())
            .then(|| EntryClaim {
                id: event_id.to_owned(),
                claims: self.clone(),
            })
    }
}
impl Drop for EntryClaim {
    fn drop(&mut self) {
        self.claims
            .0
            .lock()
            .expect("entry claims lock")
            .remove(&self.id);
    }
}
