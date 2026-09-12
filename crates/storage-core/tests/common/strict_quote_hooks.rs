//! Real SQLite connection hooks, confined to the external fault test binary.
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    sync::Mutex,
};
static STATE: Mutex<(u8, usize, bool)> = Mutex::new((0, 0, false));
unsafe extern "C" fn trace(
    _: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _: *mut c_void,
) -> c_int {
    let sql = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if sql.is_null()
        || !unsafe { CStr::from_ptr(sql) }.to_bytes().starts_with(
            b"SELECT attempt,owner,lease_until,binding,record,binding_attempt FROM ordered_sell_quote_results",
        )
    {
        return 0;
    }
    let mut s = STATE.lock().unwrap();
    s.1 += 1;
    if (s.0 == 1 && s.1 == 2) || (s.0 == 2 && s.1 == 3) {
        s.2 = true;
        unsafe { ffi::sqlite3_interrupt(ctx.cast()) };
    }
    0
}
unsafe extern "C" fn commit(_: *mut c_void) -> c_int {
    let mut s = STATE.lock().unwrap();
    if s.0 == 3 && s.1 >= 2 {
        s.2 = true;
        1
    } else {
        0
    }
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
pub struct Hooks;
impl Hooks {
    pub fn new() -> Self {
        assert_eq!(
            unsafe { ffi::sqlite3_auto_extension(Some(install)) },
            ffi::SQLITE_OK
        );
        Self
    }
}
impl Drop for Hooks {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_cancel_auto_extension(Some(install));
        }
    }
}
pub fn arm(mode: u8) {
    *STATE.lock().unwrap() = (mode, 0, false);
}
pub fn disarm() {
    let s = *STATE.lock().unwrap();
    *STATE.lock().unwrap() = (0, 0, false);
    assert!(s.2, "fault not exercised: {s:?}");
}
