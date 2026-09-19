use super::{CaptureReceipt, CaptureStore};
use anyhow::{bail, ensure, Result};
use copybot_core_types::SwapEvent;
use rusqlite::{params, OptionalExtension, TransactionBehavior};

impl CaptureStore {
    /// Raw bytes commit before decoding. No queue/retention eviction or receipt upgrade.
    pub fn receive(
        &mut self,
        signature: Option<&str>,
        wallet: &str,
        slot: u64,
        raw: &[u8],
        fingerprint: &str,
        received_at: f64,
        source_at: Option<f64>,
    ) -> Result<Option<CaptureReceipt>> {
        ensure!(
            raw.len() <= 8_388_608,
            "capture envelope size bound exceeded"
        );
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (epoch, status): (i64, String) = tx.query_row(
            "SELECT epoch,status FROM capture_meta WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            epoch == self.epoch && status == "running",
            "capture consumer fenced or failed"
        );
        let request: Option<i64> = tx
            .query_row(
                "SELECT r.id FROM capture_requests r JOIN capture_members m ON m.request_id=r.id
            WHERE r.state='ACKED' AND m.wallet=? LIMIT 1",
                [wallet],
                |r| r.get(0),
            )
            .optional()?;
        let risk: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM capture_obligations WHERE wallet=? AND state!='SETTLED')",
            [wallet],
            |r| r.get(0),
        )?;
        if request.is_none() && !risk {
            return Ok(None);
        }
        if let Some(signature) = signature {
            let prior: Option<(i64, String, String)> = tx
                .query_row(
                    "SELECT seq,stage,fingerprint FROM capture_events WHERE signature=?",
                    [signature],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((seq, stage, prior)) = prior {
                ensure!(prior == fingerprint, "capture signature conflict");
                return Ok(Some(CaptureReceipt { seq, stage }));
            }
        }
        let full: bool = tx.query_row(
            "SELECT (SELECT count(*) FROM capture_events)>=max_rows
            OR used_bytes+?>max_bytes FROM capture_meta WHERE id=1",
            [(raw.len() + 16_384) as i64],
            |r| r.get(0),
        )?;
        if full {
            tx.execute("UPDATE capture_meta SET gap=gap+1,status='failed',reason='capture_capacity' WHERE id=1", [])?;
            tx.commit()?;
            bail!("capture capacity reached; incomplete coverage");
        }
        tx.execute("INSERT INTO capture_events(signature,wallet,slot,epoch,request_id,received_at,source_at,raw,fingerprint)
            VALUES(?,?,?,?,?,?,?,?,?)", params![signature,wallet,slot.to_string(),self.epoch,request,received_at,source_at,raw,fingerprint])?;
        let seq = tx.last_insert_rowid();
        tx.execute(
            "UPDATE capture_meta SET used_bytes=used_bytes+? WHERE id=1",
            [(raw.len() + 16_384) as i64],
        )?;
        tx.commit()?;
        Ok(Some(CaptureReceipt {
            seq,
            stage: "RECEIVED".into(),
        }))
    }
    pub fn finish(&mut self, seq: i64, event: Option<&SwapEvent>, reason: &str) -> Result<()> {
        let payload = event.map(serde_json::to_string).transpose()?;
        ensure!(
            payload.as_ref().map_or(0, |p| p.len()) <= 16_384,
            "capture decoded size bound exceeded"
        );
        let stage = if event.is_some() {
            "DURABLE"
        } else {
            "REJECTED"
        };
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: bool = tx.query_row(
            "SELECT epoch=? AND status='running' FROM capture_meta WHERE id=1",
            [self.epoch],
            |r| r.get(0),
        )?;
        ensure!(active, "capture consumer fenced or failed");
        if let Some(event) = event {
            let (signature, wallet, slot): (Option<String>, String, String) = tx.query_row(
                "SELECT signature,wallet,slot FROM capture_events WHERE seq=?",
                [seq],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            ensure!(
                signature.as_deref() == Some(event.signature.as_str())
                    && wallet == event.wallet
                    && slot == event.slot.to_string(),
                "capture decoded identity mismatch"
            );
        }
        tx.execute("UPDATE capture_events SET stage=?,event_json=?,reason=? WHERE seq=? AND stage='RECEIVED'",
            params![stage,payload,reason,seq])?;
        tx.commit()?;
        let actual: (String, Option<String>) = self.db.query_row(
            "SELECT stage,event_json FROM capture_events WHERE seq=?",
            [seq],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            actual == (stage.into(), payload),
            "capture durable readback mismatch"
        );
        Ok(())
    }
}
