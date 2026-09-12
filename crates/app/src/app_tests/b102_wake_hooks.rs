//! Hold the actual blocking consumer after its final SQLite pending read has
//! produced a value, before the old bool reaches the app. No business mutation.
use anyhow::{ensure, Result};
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    sync::{Condvar, Mutex},
    time::Duration,
};
static STATE: Mutex<(bool, bool, bool, bool)> = Mutex::new((false, false, false, false));
static CV: Condvar = Condvar::new();
unsafe extern "C" fn trace(
    _: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _: *mut c_void,
) -> c_int {
    let sql = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if sql.is_null()
        || unsafe { ffi::sqlite3_get_autocommit(ctx.cast()) } == 0
        || unsafe { CStr::from_ptr(sql) }.to_bytes()
            != b"SELECT EXISTS(SELECT 1 FROM association_shadow_sell_work)"
    {
        return 0;
    }
    let mut s = STATE.lock().unwrap();
    if s.0 && !s.1 {
        s.1 = true;
        CV.notify_all();
        while !s.2 {
            let (next, t) = CV.wait_timeout(s, Duration::from_secs(8)).unwrap();
            s = next;
            if t.timed_out() {
                s.3 = true;
                break;
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
    unsafe { ffi::sqlite3_trace_v2(db, ffi::SQLITE_TRACE_PROFILE as u32, Some(trace), db.cast()) }
}
pub struct Installed;
impl Installed {
    pub fn new() -> Result<Self> {
        ensure!(unsafe { ffi::sqlite3_auto_extension(Some(install)) } == ffi::SQLITE_OK);
        Ok(Self)
    }
}
impl Drop for Installed {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_cancel_auto_extension(Some(install));
        }
    }
}
pub struct Hold;
impl Hold {
    pub fn arm() -> Self {
        *STATE.lock().unwrap() = (true, false, false, false);
        Self
    }
    pub fn hit(&self) -> bool {
        STATE.lock().unwrap().1
    }
    pub fn release(&self) {
        STATE.lock().unwrap().2 = true;
        CV.notify_all();
    }
    pub fn verify(&self) {
        let s = STATE.lock().unwrap();
        assert!(s.1 && !s.3);
    }
}
impl Drop for Hold {
    fn drop(&mut self) {
        self.release();
        STATE.lock().unwrap().0 = false;
    }
}
