//! Each current-thread test owns a fresh root until its test thread exits.
//! Reopened DB handles and loopback callbacks use the same root; no process env writes.
use super::temporary_output_fixture::OutputRoot;
use anyhow::Result;
use std::{cell::RefCell, path::PathBuf};
thread_local! { static ROOT: RefCell<Option<OutputRoot>> = const { RefCell::new(None) }; }
pub(super) fn begin(name: &str) -> Result<PathBuf> {
    ROOT.with(|slot| {
        *slot.borrow_mut() = Some(OutputRoot::new(name)?);
        Ok(slot.borrow().as_ref().unwrap().path().to_owned())
    })
}
pub(super) fn root() -> PathBuf {
    ROOT.with(|slot| {
        if slot.borrow().is_none() {
            *slot.borrow_mut() = Some(OutputRoot::new("receipt-output").unwrap());
        }
        slot.borrow().as_ref().unwrap().path().to_owned()
    })
}
pub(super) fn inputs() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/app_tests/fixtures/b93_direct")
}
pub(super) fn results() -> PathBuf {
    // Retained ignored harnesses using receipt helpers keep their explicit exports.
    ROOT.with(|slot| {
        if slot.borrow().is_none() {
            std::env::var_os("B92_RESULTS").map(PathBuf::from)
        } else {
            None
        }
    })
    .unwrap_or_else(|| root().join("results"))
}
