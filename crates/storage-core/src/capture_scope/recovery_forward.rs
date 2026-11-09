//! Incremental exact forward receipts. A terminal label alone never permits ACK.
use super::CaptureStore;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{ProtectedObservation, ProtectedSourceKind};
use rusqlite::{params, Connection, TransactionBehavior};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const BATCH: i64 = 128;
const WSOL: &str = "So11111111111111111111111111111111111111112";
#[derive(Clone)]
struct Receipt {
    id: i64,
    state: String,
    reason: Option<String>,
    disposition: Option<String>,
    hash: Option<String>,
    identity: String,
}
fn tuple_hash(state: &str, reason: &str, disposition: &str) -> String {
    let mut digest = Sha256::new();
    for part in [state, reason, disposition] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    format!("{:x}", digest.finalize())
}
pub(super) fn unfinished(db: &Connection) -> Result<bool> {
    // Both predicates match partial indexes. Do not scan all retained receipts per block.
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM capture_recovery_outbox INDEXED BY capture_recovery_pending WHERE state='PENDING') OR EXISTS(SELECT 1 FROM capture_recovery_outbox INDEXED BY capture_recovery_forward_unverified WHERE state!='PENDING' AND (acknowledged_sha256 IS NULL OR acknowledged_sha256 IS NOT disposition_sha256))",[],|r|r.get(0))?)
}
fn quantity(o: &ProtectedObservation) -> Result<(&str, &str, &str)> {
    let facts = o
        .facts
        .as_ref()
        .context("protected forward swap facts missing")?;
    let (side, mint) = if facts.token_in == WSOL && facts.token_out != WSOL {
        ("BUY", facts.token_out.as_str())
    } else if facts.token_out == WSOL && facts.token_in != WSOL {
        ("SELL", facts.token_in.as_str())
    } else {
        anyhow::bail!("protected forward pair is not SOL");
    };
    let joint = facts.exact_amounts.as_ref().map(|a| {
        if side == "BUY" {
            (a.amount_out_raw.as_str(), a.amount_out_decimals)
        } else {
            (a.amount_in_raw.as_str(), a.amount_in_decimals)
        }
    });
    let raw = if let Some(q) = &o.token_quantity {
        ensure!(
            q.side == side && q.mint == mint,
            "protected forward token quantity identity mismatch"
        );
        ensure!(
            joint.is_none_or(|(raw, decimals)| raw == q.raw && decimals == q.decimals),
            "protected forward token quantity conflicts with checked exact leg"
        );
        q.raw.as_str()
    } else {
        joint
            .context("protected forward exact token quantity missing")?
            .0
    };
    ensure!(
        !raw.is_empty()
            && raw.bytes().all(|b| b.is_ascii_digit())
            && raw.parse::<u64>().is_ok_and(|n| n > 0),
        "protected forward token quantity invalid"
    );
    Ok((side, mint, raw))
}
fn validate(db: &Connection, row: &Receipt) -> Result<String> {
    let reason = row
        .reason
        .as_deref()
        .context("protected forward terminal reason missing")?;
    let disposition = row
        .disposition
        .as_deref()
        .context("protected forward terminal disposition missing or oversized")?;
    let hash = row
        .hash
        .as_deref()
        .context("protected forward terminal hash missing")?;
    ensure!(
        hash == tuple_hash(&row.state, reason, disposition),
        "protected forward terminal tuple hash mismatch"
    );
    let saved =
        super::recovery::read(db, &row.identity)?.context("protected forward identity missing")?;
    ensure!(
        saved.receipt.id == row.id && saved.receipt.recovery_first,
        "protected forward outbox identity mismatch"
    );
    let o = &saved.observation;
    let (provenance, raw_hash): (String, String) = db.query_row(
        "SELECT provenance_json,raw_hash FROM capture_recovery WHERE id=?",
        [row.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let provenance: Value = serde_json::from_str(&provenance)?;
    let mut expected = json!({"version":1,"recovery_id":row.id,"signature":o.signature,
        "wallet":o.wallet,"slot":o.slot,"source_kind":o.source_kind,"provenance":provenance,
        "block":o.block,"raw_sha256":raw_hash,"timeliness":"not_established",
        "trading_admission":false,"follower_action":"none","status":"EVIDENCE_ONLY",
        "reason":"recovered_non_swap_or_undecodable"});
    let expected_state = if o.facts.is_some() {
        let (side, mint, raw) = quantity(o)?;
        expected["side"] = json!(side);
        expected["mint"] = json!(mint);
        expected["token_qty_raw"] = json!(raw);
        expected["status"] = json!("MISSED_UNEXECUTED");
        expected["reason"] = json!(if o.source_kind == ProtectedSourceKind::Stream {
            "protected_stream_no_admission"
        } else if side == "SELL" {
            "recovered_historical_sell_unexecuted"
        } else {
            "recovered_historical_buy_no_admission"
        });
        "MISSED"
    } else {
        "REJECTED"
    };
    ensure!(
        row.state == expected_state && expected["reason"] == reason,
        "protected forward terminal disposition class mismatch"
    );
    ensure!(
        serde_json::from_str::<Value>(disposition)? == expected,
        "protected forward terminal canonical/nontrading disposition mismatch"
    );
    Ok(hash.to_owned())
}

impl CaptureStore {
    /// Validate at most128 previously unverified terminal receipts. False means
    /// pending consumer work or another bounded validation batch remains.
    /// Any corrupt or unknown receipt returns an error and cannot authorize ACK.
    pub fn verify_forward_dispositions(&mut self) -> Result<bool> {
        self.known()?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::recovery_write::active(&tx, self.epoch)?;
        let mut query=tx.prepare("SELECT o.recovery_id,o.state,CASE WHEN length(CAST(o.reason AS BLOB))<=1024 THEN o.reason END,CASE WHEN length(CAST(o.disposition_json AS BLOB))<=262144 THEN o.disposition_json END,CASE WHEN length(o.disposition_sha256)=64 THEN o.disposition_sha256 END,r.identity_key FROM capture_recovery_outbox o INDEXED BY capture_recovery_forward_unverified JOIN capture_recovery r ON r.id=o.recovery_id WHERE o.state!='PENDING' AND (o.acknowledged_sha256 IS NULL OR o.acknowledged_sha256 IS NOT o.disposition_sha256) ORDER BY o.recovery_id LIMIT ?")?;
        let rows = query
            .query_map([BATCH], |r| {
                Ok(Receipt {
                    id: r.get(0)?,
                    state: r.get(1)?,
                    reason: r.get(2)?,
                    disposition: r.get(3)?,
                    hash: r.get(4)?,
                    identity: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(query);
        for row in &rows {
            let hash = validate(&tx, row)?;
            let changed=tx.execute("UPDATE capture_recovery_outbox SET acknowledged_sha256=? WHERE recovery_id=? AND state=? AND disposition_sha256=?",params![hash,row.id,row.state,row.hash])?;
            ensure!(
                changed == 1,
                "protected forward acknowledgement row missing"
            );
        }
        let result=tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            for row in &rows {
                let actual:(String,Option<String>,Option<String>,Option<String>,Option<String>)=self.db.query_row("SELECT state,reason,disposition_json,disposition_sha256,acknowledged_sha256 FROM capture_recovery_outbox WHERE recovery_id=?",[row.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
                ensure!(actual==(row.state.clone(),row.reason.clone(),row.disposition.clone(),row.hash.clone(),row.hash.clone()),"protected forward acknowledgement commit readback mismatch");
            }
            Ok(!unfinished(&self.db)?)
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
}
