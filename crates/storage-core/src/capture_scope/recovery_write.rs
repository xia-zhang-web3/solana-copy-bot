use super::CaptureStore;
use anyhow::{ensure, Result};
use rusqlite::Connection;

pub(super) fn active(db: &Connection, epoch: i64) -> Result<()> {
    let valid: bool = db.query_row(
        "SELECT epoch=? AND status='running' FROM capture_meta WHERE id=1",
        [epoch],
        |r| r.get(0),
    )?;
    ensure!(valid, "protected capture consumer fenced or failed");
    Ok(())
}
pub(super) fn full(db: &Connection, bytes: usize, rows: usize) -> Result<bool> {
    let full:bool=db.query_row("SELECT
        (SELECT count(*) FROM capture_events)+(SELECT count(*) FROM capture_requests)+
        (SELECT count(*) FROM capture_scope_revisions)+(SELECT count(*) FROM capture_recovery_evidence)+
        (SELECT count(*) FROM capture_recovery)+(SELECT count(*) FROM capture_recovery_outbox)+
        (SELECT count(*) FROM capture_recovery_links)+(SELECT count(*) FROM capture_recovery_anchor_links)+?>max_rows
        OR used_bytes+?>max_bytes FROM capture_meta WHERE id=1",[rows as i64,bytes as i64],|r|r.get(0))?;
    Ok(full)
}
pub(super) fn capacity(db: &Connection, bytes: usize, rows: usize) -> Result<()> {
    ensure!(
        !full(db, bytes, rows)?,
        "protected recovery capacity reached; incomplete coverage"
    );
    Ok(())
}
pub(super) fn enabled(db: &Connection) -> Result<bool> {
    let exists:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_scope_revisions')",[],|r|r.get(0))?;
    if !exists {
        return Ok(false);
    }
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM capture_scope_revisions)",
        [],
        |r| r.get(0),
    )?)
}
pub(super) fn fully_bound(db: &Connection) -> Result<()> {
    super::recovery_anchor::complete(db)?;
    super::recovery_links::complete(db)?;
    let unbound:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM capture_events e LEFT JOIN capture_recovery r ON r.live_seq=e.seq WHERE r.id IS NULL)",[],|r|r.get(0))?;
    ensure!(
        !unbound,
        "protected stream receive/identity handoff unfinished before checkpoint ACK"
    );
    Ok(())
}
pub(super) fn charge(db: &Connection, bytes: usize) -> Result<()> {
    db.execute(
        "UPDATE capture_meta SET used_bytes=used_bytes+? WHERE id=1",
        [bytes as i64],
    )?;
    Ok(())
}
impl CaptureStore {
    /// A known write error closes current authority; unknown outcomes never issue another write.
    pub fn fail_closed(&mut self, reason: &str) -> Result<()> {
        self.known()?;
        ensure!(
            !reason.is_empty() && reason.len() <= 1024,
            "protected failure reason invalid"
        );
        let result = self.gap(reason, true).and_then(|_| {
            let (epoch, status): (i64, String) = self.db.query_row(
                "SELECT epoch,status FROM capture_meta WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                epoch == self.epoch && status == "failed",
                "protected fail-closed readback mismatch"
            );
            Ok(())
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
    pub(super) fn known(&self) -> Result<()> {
        ensure!(!self.uncertain,"protected capture prior write outcome unknown; reopen and reconcile durable rows before ACK");
        Ok(())
    }
}
