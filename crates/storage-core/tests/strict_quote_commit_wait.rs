//! Actual SQLite commit-hook wait, installed only in this external test process.
#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::{ensure, Result};
use chrono::Utc;
use f::*;
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_void},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
static ARMED: AtomicBool = AtomicBool::new(false);
static HIT: AtomicBool = AtomicBool::new(false);
unsafe extern "C" fn commit(_: *mut c_void) -> c_int {
    if ARMED.swap(false, Ordering::SeqCst) {
        HIT.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(6200));
    }
    0
}
unsafe extern "C" fn install(
    db: *mut ffi::sqlite3,
    _: *mut *mut c_char,
    _: *const ffi::sqlite3_api_routines,
) -> c_int {
    unsafe {
        ffi::sqlite3_commit_hook(db, Some(commit), std::ptr::null_mut());
    }
    ffi::SQLITE_OK
}
struct Hook;
impl Drop for Hook {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_cancel_auto_extension(Some(install));
        }
    }
}
#[test]
fn r1_actual_commit_wait_downgrades_exact_saved_result_and_retries() -> Result<()> {
    ensure!(unsafe { ffi::sqlite3_auto_extension(Some(install)) } == ffi::SQLITE_OK);
    let _hook = Hook;
    let f = fixture()?;
    let now = Utc::now();
    let c = claim(&f, now)?;
    ARMED.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let out =
        f.db.store
            .complete_strict_sell_quote(&c, limits(), observation(&c, now), Utc::now)?;
    ensure!(HIT.load(Ordering::SeqCst) && start.elapsed() >= Duration::from_secs(6));
    ensure!(out.outcome != QuoteOutcome::Current);
    let wire: String =
        f.db.conn()?
            .query_row("SELECT record FROM ordered_sell_quote_results", [], |r| {
                r.get(0)
            })?;
    ensure!(serde_json::from_str::<QuoteObservation>(&wire)? == out);
    ensure!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, Utc::now)?,
        QuoteClaimStep::Claimed(_)
    ));
    Ok(())
}
