//! Restore the raw-receive to checked-identity handoff before provider intake.
use super::CaptureStore;
use anyhow::{ensure, Result};

#[derive(Debug, Clone)]
pub struct UnboundStreamReceipt {
    pub seq: i64,
    pub raw: Vec<u8>,
    pub received_at: f64,
    pub source_at: Option<f64>,
    pub stage: String,
    pub signature: Option<String>,
    pub wallet: String,
    pub slot: u64,
    pub epoch: i64,
    pub fingerprint: String,
}
impl CaptureStore {
    pub fn unbound_stream_receipts(&self, limit: usize) -> Result<Vec<UnboundStreamReceipt>> {
        self.known()?;
        ensure!(
            (1..=128).contains(&limit),
            "protected restore batch bound invalid"
        );
        let mut q=self.db.prepare("SELECT e.seq,e.raw,e.received_at,e.source_at,e.stage,e.signature,e.wallet,e.slot,e.epoch,e.fingerprint FROM capture_events e LEFT JOIN capture_recovery r ON r.live_seq=e.seq WHERE r.id IS NULL ORDER BY e.seq LIMIT ?")?;
        let values = q
            .query_map([limit as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, f64>(2)?,
                    r.get::<_, Option<f64>>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, String>(9)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        values
            .into_iter()
            .map(
                |(
                    seq,
                    raw,
                    received_at,
                    source_at,
                    stage,
                    signature,
                    wallet,
                    slot,
                    epoch,
                    fingerprint,
                )| {
                    Ok(UnboundStreamReceipt {
                        seq,
                        raw,
                        received_at,
                        source_at,
                        stage,
                        signature,
                        wallet,
                        slot: slot.parse()?,
                        epoch,
                        fingerprint,
                    })
                },
            )
            .collect()
    }
    pub fn stream_receipt_scope(&self, seq: i64) -> Result<i64> {
        self.known()?;
        let revision=self.db.query_row("SELECT s.revision FROM capture_scope_revisions s JOIN capture_events e ON e.epoch=s.epoch WHERE e.seq=? AND (s.request_id=e.request_id OR e.request_id IS NULL) AND EXISTS(SELECT 1 FROM json_each(s.wallets_json) w WHERE w.value=e.wallet) ORDER BY s.revision DESC LIMIT 1",[seq],|r|r.get(0))?;
        Ok(revision)
    }
}
