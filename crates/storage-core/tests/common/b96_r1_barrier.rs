//! Deterministic SQLite statement barrier on exactly one newly opened connection.
use anyhow::{ensure, Result};
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    sync::{Condvar, Mutex},
    time::Duration,
};
// armed, reached, released, timed out
static STATE: Mutex<(bool, bool, bool, bool)> = Mutex::new((false, false, false, false));
static CV: Condvar = Condvar::new();
unsafe extern "C" fn trace(_: c_uint, _: *mut c_void, stmt: *mut c_void, _: *mut c_void) -> c_int {
    let sql = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if !sql.is_null()
        && unsafe { CStr::from_ptr(sql) }
            .to_bytes()
            .starts_with(b"SELECT owner,intent_id FROM source_sell_signature_claims")
    {
        if let Ok(mut s) = STATE.lock() {
            if s.0 {
                s.0 = false;
                s.1 = true;
                CV.notify_all();
                while !s.2 {
                    match CV.wait_timeout(s, Duration::from_secs(10)) {
                        Ok((mut next, timeout)) => {
                            if timeout.timed_out() {
                                next.3 = true;
                                break;
                            }
                            s = next;
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    }
    0
}
unsafe extern "C" fn install(
    db: *mut ffi::sqlite3,
    _: *mut *mut c_char,
    _: *const ffi::sqlite3_api_routines,
) -> c_int {
    unsafe {
        ffi::sqlite3_trace_v2(
            db,
            ffi::SQLITE_TRACE_STMT as u32,
            Some(trace),
            std::ptr::null_mut(),
        )
    }
}
pub fn connection<T>(open: impl FnOnce() -> Result<T>) -> Result<T> {
    ensure!(unsafe { ffi::sqlite3_auto_extension(Some(install)) } == ffi::SQLITE_OK);
    let value = open();
    unsafe {
        ffi::sqlite3_cancel_auto_extension(Some(install));
    }
    value
}
pub struct Gate;
impl Gate {
    pub fn arm() -> Self {
        *STATE.lock().unwrap() = (true, false, false, false);
        Self
    }
    pub fn reached(&self) -> Result<()> {
        let (s, wait) = CV
            .wait_timeout_while(STATE.lock().unwrap(), Duration::from_secs(10), |s| !s.1)
            .unwrap();
        ensure!(
            !wait.timed_out() && s.1 && !s.3,
            "writer did not reach claim barrier"
        );
        Ok(())
    }
    pub fn release(&self) {
        STATE.lock().unwrap().2 = true;
        CV.notify_all();
    }
    pub fn completed(&self) {
        assert_eq!(*STATE.lock().unwrap(), (false, true, true, false));
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}
