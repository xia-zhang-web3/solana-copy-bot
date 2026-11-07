use super::*;
use copybot_ingestion::transaction_membership::{selected, Capture};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        let folder = std::env::temp_dir().join(format!(
            "copybot-membership-{}-{}-{}",
            std::process::id(),
            FIXTURE_ID.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&folder).unwrap();
        Self(folder)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("journal.db")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn chosen(n: u8) -> SwapEvent {
    let mut swap = recent_raw_journal_backpressure_swap(n as usize, Utc::now());
    swap.signature = bs58::encode(vec![n; 64]).into_string();
    swap.slot = (1..100_000)
        .find(|s| selected(&swap.signature, *s))
        .unwrap();
    swap
}

fn write(store: &SqliteStore, capture: &Capture, swaps: &[SwapEvent]) -> Result<()> {
    super::super::write_recent_raw_journal_batch_with_membership(
        &ObservedSwapWriterTelemetry::default(),
        swaps,
        || Instant::now() + StdDuration::from_secs(2),
        |rows, end| store.insert_recent_raw_journal_batch_bulk_with_deadline(rows, Utc::now(), end),
        |rows, receipt| capture.observe_journal(rows, receipt),
    )
}

#[test]
fn transaction_membership_real_commit_and_conflicting_slot_are_honest() -> Result<()> {
    let fixture = Fixture::new();
    let store = prepare_recent_raw_journal_store_for_test(&fixture.path())?;
    let row = chosen(9);
    let capture = Capture::new(true, 8);
    write(&store, &capture, std::slice::from_ref(&row))?;
    let mut conflict = row.clone();
    conflict.slot = (row.slot + 1..100_000)
        .find(|s| selected(&row.signature, *s))
        .unwrap();
    write(&store, &capture, &[row.clone(), conflict.clone()])?;
    capture.close();
    let v = capture.snapshot();
    let records = v["records"].as_array().unwrap();
    let first = records.iter().find(|x| x["slot"] == row.slot).unwrap();
    let other = records.iter().find(|x| x["slot"] == conflict.slot).unwrap();
    assert_eq!(first["journal_inserted"]["count"], 1);
    assert_eq!(first["journal_batch_unattributed"]["count"], 1);
    assert_eq!(other["journal_inserted"]["count"], 0);
    assert_eq!(other["journal_batch_unattributed"]["count"], 1);
    let db = Connection::open(fixture.path())?;
    assert_eq!(
        db.query_row(
            "SELECT slot FROM observed_swaps WHERE signature=?",
            [&row.signature],
            |r| r.get::<_, u64>(0)
        )?,
        row.slot
    );
    Ok(())
}

#[test]
fn transaction_membership_actual_rollback_does_not_publish_commit() -> Result<()> {
    let fixture = Fixture::new();
    let store = prepare_recent_raw_journal_store_for_test(&fixture.path())?;
    let db = Connection::open(fixture.path())?;
    db.execute_batch("CREATE TRIGGER membership_abort BEFORE INSERT ON observed_swaps BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;")?;
    let capture = Capture::new(true, 8);
    assert!(write(&store, &capture, &[chosen(2)]).is_err());
    capture.close();
    assert_eq!(capture.snapshot()["records"].as_array().unwrap().len(), 0);
    assert_eq!(capture.snapshot()["complete"], false);
    assert_eq!(
        db.query_row("SELECT count(*) FROM observed_swaps", [], |r| r
            .get::<_, u64>(0))?,
        0
    );
    Ok(())
}

#[test]
fn transaction_membership_partial_prefix_then_failure_has_no_suffix_commit() {
    let rows = [chosen(3), chosen(4)];
    let capture = Capture::new(true, 8);
    let mut calls = 0;
    let result = super::super::write_recent_raw_journal_batch_with_membership(
        &ObservedSwapWriterTelemetry::default(),
        &rows,
        || Instant::now() + StdDuration::from_secs(2),
        |suffix, _| {
            calls += 1;
            if calls == 1 {
                assert_eq!(suffix.len(), 2);
                Ok((recent_raw_journal_write_summary_for_test(1, 1), true))
            } else {
                assert_eq!(suffix[0].signature, rows[1].signature);
                Err(anyhow!("fixture refusal"))
            }
        },
        |rows, receipt| capture.observe_journal(rows, receipt),
    );
    assert!(result.is_err());
    capture.close();
    let v = capture.snapshot();
    assert_eq!(v["complete"], false);
    assert_eq!(v["records"].as_array().unwrap().len(), 1);
    assert_eq!(v["records"][0]["signature"], rows[0].signature);
    assert_eq!(v["records"][0]["journal_inserted"]["count"], 1);
}

#[test]
fn transaction_membership_opt_out_preserves_actual_write() -> Result<()> {
    let fixture = Fixture::new();
    let store = prepare_recent_raw_journal_store_for_test(&fixture.path())?;
    let capture = Capture::new(false, 8);
    let rows = [chosen(5)];
    write(&store, &capture, &rows)?;
    assert!(capture.snapshot()["records"].as_array().unwrap().is_empty());
    assert_eq!(
        Connection::open(fixture.path())?.query_row(
            "SELECT count(*) FROM observed_swaps",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    Ok(())
}
