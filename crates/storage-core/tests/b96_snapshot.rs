#[path = "common/b96_ordered.rs"]
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
fn b96_stage_immediate_blocks_writer_and_never_mixes_financial_shadow_snapshot() -> Result<()> {
    let mut f = within()?;
    let id = lot(&mut f, "origin", 42, "block", 4)?;
    f.db.conn()?.pragma_update(None, "journal_mode", "WAL")?;
    assert_eq!(
        unsafe { ffi::sqlite3_auto_extension(Some(install)) },
        ffi::SQLITE_OK
    );
    let reader = AssociationInbox::open(&f.db.path, limits());
    unsafe {
        ffi::sqlite3_cancel_auto_extension(Some(install));
    }
    let mut reader = reader?;
    let path = f.db.path.clone();
    let order = f.order.clone();
    let (committed, after_commit) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || -> Result<bool> {
        let mut c = rusqlite::Connection::open(path)?;
        c.busy_timeout(Duration::ZERO)?;
        let s = STATE.lock().unwrap();
        let (s, wait) = CV
            .wait_timeout_while(s, Duration::from_secs(10), |s| !s.0)
            .unwrap();
        anyhow::ensure!(!wait.timed_out() && s.0, "stage barrier not reached");
        drop(s);
        let attempt = c.execute_batch("BEGIN IMMEDIATE");
        let blocked = matches!(&attempt, Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ErrorCode::DatabaseBusy);
        if attempt.is_ok() {
            // Negative-control arm: lack of the writer reservation is observed
            // before releasing the stage. No timing oracle or sleep.
            c.execute(
                "UPDATE shadow_lots SET qty=1.25,risk_context='quarantined_legacy' WHERE id=?1",
                [id],
            )?;
            c.execute(
                "UPDATE orders SET status='execution_canary_submitted' WHERE order_id=?1",
                [&order],
            )?;
            c.execute_batch("COMMIT")?;
        }
        {
            let mut s = STATE.lock().unwrap();
            s.1 = true;
            CV.notify_all();
        }
        // Wait for stage return only AFTER releasing its callback barrier.
        after_commit.recv_timeout(Duration::from_secs(10))?;
        if blocked {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute(
                "UPDATE shadow_lots SET qty=1.25,risk_context='quarantined_legacy' WHERE id=?1",
                [id],
            )?;
            tx.execute(
                "UPDATE orders SET status='execution_canary_submitted' WHERE order_id=?1",
                [&order],
            )?;
            tx.commit()?;
        }
        Ok(blocked)
    });
    ARMED.store(true, Ordering::SeqCst);
    let during = reader.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1);
    committed.send(())?;
    let blocked = writer.join().expect("writer thread panicked")?;
    assert!(
        blocked,
        "competing writer must be blocked until stage commit"
    );
    let OrderedSellStage::Inserted(i) = during? else {
        panic!("must insert old complete snapshot")
    };
    assert_eq!(*STATE.lock().unwrap(), (true, true, false));
    assert!(i.staged_evaluation.pending_buys.is_empty());
    let old = &i.staged_evaluation.shadow.as_ref().unwrap().lots[0];
    assert_eq!(old.qty_bits, 3.5f64.to_bits());
    assert_eq!(old.relation, ShadowRelation::AfterSell);
    let after = reader.sell_preparation("sell")?.unwrap();
    assert_eq!(after.current.pending_buys.len(), 1);
    let new = &after.current.shadow.as_ref().unwrap().lots[0];
    assert_eq!(new.qty_bits, 1.25f64.to_bits());
    assert_eq!(new.risk_context, "quarantined_legacy");
    assert_eq!(
        reader.revalidate_ordered_source_sell_intent(&i.intent_id)?,
        OrderedSellDecision::Blocked(OrderedSellReason::SelectedChain(Check::Blocked(
            Reason::FinancialSetChanged
        )))
    );
    assert_eq!(
        reader.load_ordered_source_sell_intent_history(&i.intent_id)?,
        Some(*i)
    );
    Ok(())
}
