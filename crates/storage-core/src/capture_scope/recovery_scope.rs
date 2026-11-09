//! Immutable scope revisions. Request publication remains downstream of a new baseline.
use super::CaptureStore;
use anyhow::{ensure, Result};
use copybot_core_types::association_recovery::DurableCheckpoint;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedScope {
    pub revision: i64,
    pub epoch: i64,
    pub request_id: Option<i64>,
    pub wallets: Vec<String>,
    pub baseline: Option<DurableCheckpoint>,
    #[serde(default)]
    pub ack_gap: Option<i64>,
}

fn read_scope(db: &Connection, state: &str) -> Result<Option<ProtectedScope>> {
    let row: Option<(i64, i64, Option<i64>, String, Option<String>, Option<i64>)> = db.query_row(
        "SELECT revision,epoch,request_id,wallets_json,baseline_json,ack_gap FROM capture_scope_revisions WHERE state=?",
        [state], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    row.map(
        |(revision, epoch, request_id, wallets, baseline, ack_gap)| {
            Ok(ProtectedScope {
                revision,
                epoch,
                request_id,
                wallets: serde_json::from_str(&wallets)?,
                baseline: baseline.map(|s| serde_json::from_str(&s)).transpose()?,
                ack_gap,
            })
        },
    )
    .transpose()
}
pub(super) fn desired(db: &Connection) -> Result<(Option<i64>, Vec<String>)> {
    let request_id = db.query_row("SELECT id FROM capture_requests WHERE state IN ('PENDING','ACKED') ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).optional()?;
    let mut q = db.prepare("SELECT wallet FROM capture_members WHERE request_id=?
        UNION SELECT wallet FROM capture_obligations WHERE state!='SETTLED' ORDER BY wallet LIMIT 129")?;
    let wallets = q
        .query_map([request_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        wallets.len() <= 128,
        "protected scope union exceeds 128 wallets"
    );
    Ok((request_id, wallets))
}
fn unfinished(db: &Connection) -> Result<bool> {
    super::recovery_forward::unfinished(db)
}

impl CaptureStore {
    pub fn active_scope(&self) -> Result<Option<ProtectedScope>> {
        self.known()?;
        read_scope(&self.db, "ACTIVE")
    }
    pub fn needs_transition(&self) -> Result<bool> {
        self.known()?;
        let (request_id, wallets) = desired(&self.db)?;
        let gap: i64 = self
            .db
            .query_row("SELECT gap FROM capture_meta WHERE id=1", [], |r| r.get(0))?;
        Ok(self.active_scope()?.is_none_or(|s| {
            s.epoch != self.epoch
                || s.request_id != request_id
                || s.wallets != wallets
                || s.ack_gap != Some(gap)
        }))
    }
    /// Stable across restarts: an unfinished planned revision is completed first.
    /// No request becomes ACKED here, and an old cursor is never copied as baseline.
    pub fn prepare_scope(&mut self) -> Result<ProtectedScope> {
        self.known()?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure!(
            !unfinished(&tx)?,
            "protected recovery outbox must drain before scope transition"
        );
        super::recovery_write::active(&tx, self.epoch)?;
        if let Some(scope) = read_scope(&tx, "PLANNED")? {
            return Ok(scope);
        }
        // Same IMMEDIATE lock as Python pin-before-lot: a prior BUY either pins
        // before this snapshot or observes its revoked ACK after this commit.
        tx.execute("UPDATE capture_meta SET gap=gap+1,reason='protected_scope_transition' WHERE id=1 AND epoch=?",[self.epoch])?;
        let (request_id, wallets) = desired(&tx)?;
        let wire = serde_json::to_string(&wallets)?;
        super::recovery_write::capacity(&tx, wire.len() + 2048, 1)?;
        tx.execute("INSERT INTO capture_scope_revisions(epoch,request_id,wallets_json,state) VALUES(?,?,?,'PLANNED')", params![self.epoch,request_id,wire])?;
        let revision = tx.last_insert_rowid();
        super::recovery_write::charge(&tx, wire.len() + 2048)?;
        let expected = ProtectedScope {
            revision,
            epoch: self.epoch,
            request_id,
            wallets,
            baseline: None,
            ack_gap: None,
        };
        let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            ensure!(
                read_scope(&self.db, "PLANNED")? == Some(expected.clone()),
                "protected scope prepare readback mismatch"
            );
            Ok(expected)
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
    /// Called with the actual committed/read-back inbox checkpoint for this revision.
    /// Capture receipt/outbox commits precede inbox checkpoint commit; any error forbids ACK.
    pub fn commit_scope(
        &mut self,
        scope: &ProtectedScope,
        baseline: &DurableCheckpoint,
        now: f64,
    ) -> Result<()> {
        self.known()?;
        ensure!(
            self.verify_forward_dispositions()?,
            "protected forward dispositions not verified before scope ACK"
        );
        ensure!(
            now.is_finite() && now >= 0.,
            "protected scope invalid acknowledgement time"
        );
        ensure!(
            baseline.block.scope.valid() && baseline.block.scope.wallets == scope.wallets,
            "protected scope baseline wallets mismatch"
        );
        ensure!(
            scope.epoch == self.epoch && scope.baseline.is_none(),
            "protected scope epoch mismatch"
        );
        let wire = serde_json::to_string(baseline)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::recovery_write::active(&tx, self.epoch)?;
        ensure!(
            read_scope(&tx, "PLANNED")? == Some(scope.clone()),
            "protected scope planned identity changed"
        );
        ensure!(
            !unfinished(&tx)?,
            "protected recovery outbox must drain before scope ACK"
        );
        super::recovery_write::fully_bound(&tx)?;
        let receiving: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM capture_events WHERE stage='RECEIVED')",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            !receiving,
            "protected pending stream receipt must finish before scope ACK"
        );
        let mut q =
            tx.prepare("SELECT DISTINCT wallet FROM capture_obligations WHERE state!='SETTLED'")?;
        let pins = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(q);
        ensure!(
            pins.iter().all(|w| scope.wallets.binary_search(w).is_ok()),
            "protected scope omitted unsettled pin"
        );
        super::recovery_write::capacity(&tx, wire.len(), 0)?;
        tx.execute(
            "UPDATE capture_scope_revisions SET state='RETIRED' WHERE state='ACTIVE'",
            [],
        )?;
        let gap: i64 = tx.query_row("SELECT gap FROM capture_meta WHERE id=1", [], |r| r.get(0))?;
        tx.execute("UPDATE capture_scope_revisions SET state='ACTIVE',baseline_json=?,ack_gap=? WHERE revision=? AND state='PLANNED'",params![wire,gap,scope.revision])?;
        if let Some(id) = scope.request_id {
            let state: String =
                tx.query_row("SELECT state FROM capture_requests WHERE id=?", [id], |r| {
                    r.get(0)
                })?;
            ensure!(
                state == "PENDING" || state == "ACKED",
                "protected scope request no longer admissible"
            );
            tx.execute(
                "UPDATE capture_requests SET state='DEMOTED' WHERE state='ACKED' AND id!=?",
                [id],
            )?;
            tx.execute(
                "UPDATE capture_requests SET state='SUPERSEDED' WHERE state='PENDING' AND id<?",
                [id],
            )?;
            tx.execute("UPDATE capture_requests SET state='ACKED',epoch=?,gap=(SELECT gap FROM capture_meta WHERE id=1),ack_seq=(SELECT coalesce(max(seq),0) FROM capture_events),ack_at=?,available_seq=NULL,available_at=NULL WHERE id=?",params![self.epoch,now,id])?;
        }
        super::recovery_write::charge(&tx, wire.len())?;
        let mut expected = scope.clone();
        expected.baseline = Some(baseline.clone());
        expected.ack_gap = Some(gap);
        let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            ensure!(
                read_scope(&self.db, "ACTIVE")? == Some(expected),
                "protected scope commit readback mismatch"
            );
            if let Some(id) = scope.request_id {
                let ack: (String, i64, i64) = self.db.query_row(
                    "SELECT state,epoch,gap FROM capture_requests WHERE id=?",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                let gap: i64 =
                    self.db
                        .query_row("SELECT gap FROM capture_meta WHERE id=1", [], |r| r.get(0))?;
                ensure!(
                    ack == ("ACKED".into(), self.epoch, gap),
                    "protected scope request ACK readback mismatch"
                );
            }
            Ok(())
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
    pub fn planned_scope(&self) -> Result<Option<ProtectedScope>> {
        self.known()?;
        read_scope(&self.db, "PLANNED")
    }
    pub fn recovery_outbox_pending(&self) -> Result<bool> {
        self.known()?;
        unfinished(&self.db)
    }
}
