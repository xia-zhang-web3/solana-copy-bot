#[path = "common/b95_shadow.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{
    association_inbox::AssociationInbox, association_sell_preparation::*,
    association_sell_shadow_types::*,
};
use f::*;
use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Condvar, Mutex,
    },
    time::Duration,
};
static ARMED: AtomicBool = AtomicBool::new(false);
// reached, released, timed_out; local to this integration-test process.
static STATE: Mutex<(bool, bool, bool)> = Mutex::new((false, false, false));
static CV: Condvar = Condvar::new();
unsafe extern "C" fn trace(_: c_uint, _: *mut c_void, stmt: *mut c_void, _: *mut c_void) -> c_int {
    // SQLite owns stmt for the duration of this callback. No SQL/reentrant use.
    let sql = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if !sql.is_null()
        && unsafe { CStr::from_ptr(sql) }
            .to_bytes()
            .windows(b"FROM shadow_lots l".len())
            .any(|w| w == b"FROM shadow_lots l")
        && ARMED.swap(false, Ordering::SeqCst)
    {
        if let Ok(mut state) = STATE.lock() {
            state.0 = true;
            CV.notify_all();
            while !state.1 {
                match CV.wait_timeout(state, Duration::from_secs(10)) {
                    Ok((mut s, timeout)) => {
                        if timeout.timed_out() {
                            s.2 = true;
                            break;
                        }
                        state = s;
                    }
                    Err(_) => break,
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
#[test]
fn b95_actual_preparation_uses_one_snapshot_during_concurrent_atomic_writer() -> Result<()> {
    let mut f = within()?;
    let id = lot(&mut f, "origin", 42, "block", 4)?;
    f.db.conn()?.pragma_update(None, "journal_mode", "WAL")?;
    // Attach a test-only SQLite trace to the actual private inbox connection.
    // No production hook, new Cargo feature or alternate preparation API.
    assert_eq!(
        unsafe { ffi::sqlite3_auto_extension(Some(install)) },
        ffi::SQLITE_OK
    );
    let reader = AssociationInbox::open(&f.db.path, limits());
    unsafe {
        ffi::sqlite3_cancel_auto_extension(Some(install));
    }
    let reader = reader?;
    let path = f.db.path.clone();
    let order = f.order.clone();
    let writer = std::thread::spawn(move || -> Result<()> {
        let c = rusqlite::Connection::open(path)?;
        c.busy_timeout(Duration::from_secs(5))?;
        let s = STATE.lock().unwrap();
        let (s, wait) = CV
            .wait_timeout_while(s, Duration::from_secs(10), |s| !s.0)
            .unwrap();
        anyhow::ensure!(!wait.timed_out() && s.0, "reader barrier not reached");
        drop(s);
        let tx = c.unchecked_transaction()?;
        tx.execute(
            "UPDATE shadow_lots SET qty=1.25,risk_context='quarantined_legacy' WHERE id=?1",
            [id],
        )?;
        tx.execute(
            "UPDATE orders SET status='execution_canary_submitted' WHERE order_id=?1",
            [order],
        )?;
        tx.commit()?;
        let mut s = STATE.lock().unwrap();
        s.1 = true;
        CV.notify_all();
        Ok(())
    });
    ARMED.store(true, Ordering::SeqCst);
    let during = reader.sell_preparation("sell");
    let written = writer.join().expect("writer thread panicked");
    written?;
    let during = during?.unwrap();
    let state = *STATE.lock().unwrap();
    assert_eq!(state, (true, true, false));
    assert!(during.current.pending_buys.is_empty());
    let old = &during.current.shadow.as_ref().unwrap().lots[0];
    assert_eq!(old.qty_bits, 3.5f64.to_bits());
    assert_eq!(old.risk_context, "market");
    assert_eq!(old.relation, ShadowRelation::AfterSell);
    let after = reader.sell_preparation("sell")?.unwrap();
    assert_eq!(after.current.pending_buys.len(), 1);
    let new = &after.current.shadow.unwrap().lots[0];
    assert_eq!(new.qty_bits, 1.25f64.to_bits());
    assert_eq!(new.risk_context, "quarantined_legacy");
    assert_eq!(
        after.current.selected_chain,
        Check::Blocked(Reason::FinancialSetChanged)
    );
    assert_eq!(after.first, during.first);
    assert_eq!(after.current.trade_authority, "trade_authority_none");
    Ok(())
}
