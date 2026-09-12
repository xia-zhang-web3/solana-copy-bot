//! Test-only real SQLite faults/barrier attached to the consumer connection.
use anyhow::{ensure, Result};
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    sync::{Condvar, Mutex},
    time::Duration,
};
#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    Off,
    PreRead,
    PostRead,
    Commit,
    HoldPost,
}
// mode, positive proof reads, hit, released, timeout
static STATE: Mutex<(Mode, usize, bool, bool, bool)> =
    Mutex::new((Mode::Off, 0, false, false, false));
static CV: Condvar = Condvar::new();
unsafe extern "C" fn trace(
    _: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _: *mut c_void,
) -> c_int {
    let sql = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if sql.is_null()
        || !unsafe { CStr::from_ptr(sql) }
            .to_bytes()
            .starts_with(b"SELECT json_array(intent_id,signature,version,policy,record)")
    {
        return 0;
    }
    if let Ok(mut s) = STATE.lock() {
        s.1 += 1;
        let hit = match s.0 {
            Mode::PreRead => s.1 == 1,
            Mode::PostRead | Mode::HoldPost => s.1 == 3,
            _ => false,
        };
        if hit {
            s.2 = true;
            CV.notify_all();
            if s.0 == Mode::HoldPost {
                while !s.3 {
                    match CV.wait_timeout(s, Duration::from_secs(10)) {
                        Ok((mut next, t)) => {
                            if t.timed_out() {
                                next.4 = true;
                                break;
                            }
                            s = next;
                        }
                        Err(_) => break,
                    }
                }
            } else {
                unsafe { ffi::sqlite3_interrupt(ctx.cast()) };
            }
        }
    }
    0
}
unsafe extern "C" fn commit(_: *mut c_void) -> c_int {
    if let Ok(mut s) = STATE.lock() {
        if s.0 == Mode::Commit && s.1 > 0 && !s.2 {
            s.2 = true;
            return 1;
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
        ffi::sqlite3_commit_hook(db, Some(commit), std::ptr::null_mut());
        ffi::sqlite3_trace_v2(db, ffi::SQLITE_TRACE_STMT as u32, Some(trace), db.cast())
    }
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
pub struct Fault;
impl Fault {
    pub fn arm(mode: Mode) -> Self {
        *STATE.lock().unwrap() = (mode, 0, false, false, false);
        Self
    }
    pub fn hit(&self) -> bool {
        STATE.lock().unwrap().2
    }
    pub fn release(&self) {
        STATE.lock().unwrap().3 = true;
        CV.notify_all();
    }
    pub fn verify(&self) {
        let s = STATE.lock().unwrap();
        assert!(s.2 && !s.4);
    }
}
impl Drop for Fault {
    fn drop(&mut self) {
        self.release();
        STATE.lock().unwrap().0 = Mode::Off;
    }
}
