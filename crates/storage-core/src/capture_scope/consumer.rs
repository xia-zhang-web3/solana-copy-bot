use super::CaptureStore;
use anyhow::{ensure, Result};
use rusqlite::{params, TransactionBehavior};

impl CaptureStore {
    /// Fences any previous consumer. Persisted members/pins remain protection rules;
    /// old admissions cannot authorize new BUYs in this epoch.
    pub fn start(&mut self) -> Result<()> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE capture_meta SET epoch=epoch+1,gap=gap+1,status='running',
            reason='restart_or_initial_provider_coverage_unknown' WHERE id=1",
            [],
        )?;
        self.epoch = tx.query_row("SELECT epoch FROM capture_meta WHERE id=1", [], |r| {
            r.get(0)
        })?;
        tx.commit()?;
        Ok(())
    }
    pub fn accept_pending(&mut self, now: f64) -> Result<()> {
        ensure!(
            self.pending()?.is_empty(),
            "restore pending capture before admission"
        );
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: bool = tx.query_row(
            "SELECT epoch=? AND status='running' FROM capture_meta WHERE id=1",
            [self.epoch],
            |r| r.get(0),
        )?;
        ensure!(active, "capture consumer fenced or failed");
        let mut q = tx.prepare(
            "SELECT id FROM capture_requests WHERE state='PENDING' ORDER BY id LIMIT 129",
        )?;
        let ids = q
            .query_map([], |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(q);
        ensure!(ids.len() <= 128, "capture request bound exceeded");
        for id in ids {
            let count: i64 = tx.query_row(
                "SELECT count(*) FROM capture_members WHERE request_id=?",
                [id],
                |r| r.get(0),
            )?;
            ensure!(count <= 128, "capture member bound exceeded");
            // Pins are in this same DB and precede virtual lot creation. No gap at demotion.
            tx.execute(
                "UPDATE capture_requests SET state='DEMOTED' WHERE state='ACKED'",
                [],
            )?;
            tx.execute("UPDATE capture_requests SET state='ACKED',epoch=?,gap=(SELECT gap FROM capture_meta),
                ack_seq=(SELECT coalesce(max(seq),0) FROM capture_events),ack_at=? WHERE id=?",
                params![self.epoch, now, id])?;
        }
        tx.commit()?;
        // ACK is readable only after the protection transaction has committed.
        let _: i64 = self
            .db
            .query_row("SELECT epoch FROM capture_meta WHERE id=1", [], |r| {
                r.get(0)
            })?;
        Ok(())
    }
    pub fn gap(&mut self, reason: &str, fatal: bool) -> Result<()> {
        self.db.execute("UPDATE capture_meta SET gap=gap+1,reason=CASE WHEN status='failed' THEN reason ELSE ? END,
            status=CASE WHEN ? THEN 'failed' ELSE status END WHERE id=1 AND epoch=?",
            params![reason, fatal, self.epoch])?;
        Ok(())
    }
}
