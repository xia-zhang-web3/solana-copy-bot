//! The observer cannot replace End, clear hold, or make a failed export fatal.
use super::End;
use crate::source::durable::{
    continuity_telemetry::ReaderIdentity,
    reader_diagnostics::{event, ReaderDiagnostics},
    reader_error_witness::ErrorWitness,
};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};
pub(super) struct TerminalObserver {
    journal: Arc<ReaderDiagnostics>,
    reader: Option<ReaderIdentity>,
    secrets: Vec<String>,
    finished: bool,
}
impl TerminalObserver {
    pub(super) fn new(
        journal: Arc<ReaderDiagnostics>,
        reader: Option<ReaderIdentity>,
        secrets: Vec<String>,
    ) -> Self {
        journal.exporter();
        let out = Self {
            journal,
            reader,
            secrets,
            finished: false,
        };
        out.observe("task_observer_attached", "NOT_TERMINAL", None);
        out
    }
    fn observe(&self, kind: &'static str, outcome: &'static str, end: Option<&End>) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut witness = event(self.reader.clone(), kind, outcome);
            witness.error = match end {
                Some(End::Status(e)) => Some(ErrorWitness::status(e, &self.secrets)),
                Some(End::Ping(e)) => Some(ErrorWitness::error(e, &self.secrets)),
                _ => None,
            };
            self.journal.record(witness);
        }));
        if result.is_err() {
            self.journal.observer_failed();
        }
    }
    pub(super) fn finish(&mut self, end: &End) {
        self.finished = true;
        let outcome = match end {
            End::Status(_) => "STATUS",
            End::Ping(_) => "PING_ERROR",
            End::Eof => "EOF",
            End::Budget(reason) => reason,
        };
        self.observe("terminal_end", outcome, Some(end));
    }
}
impl Drop for TerminalObserver {
    fn drop(&mut self) {
        if !self.finished {
            self.observe(
                if std::thread::panicking() {
                    "task_panicked"
                } else {
                    "task_future_dropped"
                },
                "UNAVAILABLE_NO_END_RETURNED",
                None,
            );
        }
    }
}
