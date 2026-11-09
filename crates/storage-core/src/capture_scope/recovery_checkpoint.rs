use super::{CaptureStore, ProtectedScope};
use anyhow::{ensure, Result};
use copybot_core_types::association_recovery::DurableCheckpoint;
use rusqlite::{params, OptionalExtension, TransactionBehavior};

impl CaptureStore {
    pub fn durable_capture_identity(&self) -> Result<String> {
        self.known()?;
        let id: String = self.db.query_row(
            "SELECT generation FROM capture_recovery_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "protected capture identity invalid"
        );
        Ok(id)
    }
    pub fn consumer_epoch(&self) -> i64 {
        self.epoch
    }
    pub fn desired_wallets(&self) -> Result<Vec<String>> {
        self.known()?;
        super::recovery_scope::desired(&self.db).map(|(_, wallets)| wallets)
    }
    /// Latest actual committed head, including a just-retired stale planned revision.
    /// It is an anchor only: a new wallet revision must establish its own checkpoint.
    pub fn latest_checkpoint(&self) -> Result<Option<DurableCheckpoint>> {
        self.known()?;
        let wire:Option<String>=self.db.query_row("SELECT baseline_json FROM capture_scope_revisions WHERE baseline_json IS NOT NULL ORDER BY revision DESC LIMIT 1",[],|r|r.get(0)).optional()?;
        wire.map(|w| serde_json::from_str(&w).map_err(Into::into))
            .transpose()
    }
    pub fn record_checkpoint(&mut self, revision: i64, head: &DurableCheckpoint) -> Result<()> {
        self.write_checkpoint(revision, head, false)
    }
    /// Restart completes the old exact inbox/scope first. Retiring this revision
    /// never ACKs its pending member request or re-labels its immutable epoch.
    pub fn recover_planned(
        &mut self,
        scope: &ProtectedScope,
        head: &DurableCheckpoint,
    ) -> Result<()> {
        self.known()?;
        ensure!(
            self.planned_scope()?
                .as_ref()
                .is_some_and(|s| s.revision == scope.revision
                    && s.epoch == scope.epoch
                    && s.wallets == scope.wallets),
            "protected stale plan identity mismatch"
        );
        ensure!(
            scope.epoch != self.epoch,
            "current planned scope must use normal scope ACK"
        );
        self.write_checkpoint(scope.revision, head, true)
    }
    fn write_checkpoint(
        &mut self,
        revision: i64,
        head: &DurableCheckpoint,
        retire: bool,
    ) -> Result<()> {
        self.known()?;
        ensure!(
            self.verify_forward_dispositions()?,
            "protected forward dispositions not verified before checkpoint ACK"
        );
        let wire = serde_json::to_string(head)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::recovery_write::active(&tx, self.epoch)?;
        super::recovery_write::fully_bound(&tx)?;
        let (state, wallets, prior): (String, String, Option<String>) = tx.query_row(
            "SELECT state,wallets_json,baseline_json FROM capture_scope_revisions WHERE revision=?",
            [revision],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        ensure!(
            if retire {
                state == "PLANNED"
            } else {
                state == "ACTIVE"
            },
            "protected checkpoint revision state invalid"
        );
        ensure!(
            head.block.scope.valid()
                && serde_json::from_str::<Vec<String>>(&wallets)? == head.block.scope.wallets,
            "protected checkpoint scope mismatch"
        );
        let prior_bytes = prior.as_ref().map_or(0, |s| s.len());
        if let Some(old) = prior {
            let old: DurableCheckpoint = serde_json::from_str(&old)?;
            ensure!(
                head.block.observation.child.slot >= old.block.observation.child.slot,
                "protected checkpoint regressed"
            );
            if head.block.observation.child.slot == old.block.observation.child.slot {
                ensure!(
                    head.block == old.block,
                    "protected checkpoint identity conflict"
                );
            }
        }
        if retire {
            let pending = super::recovery_forward::unfinished(&tx)?;
            ensure!(
                !pending,
                "protected old outbox must drain before retirement"
            );
        }
        let delta = wire.len() as i64 - prior_bytes as i64;
        super::recovery_write::capacity(&tx, delta.max(0) as usize, 0)?;
        tx.execute("UPDATE capture_scope_revisions SET baseline_json=?,state=CASE WHEN ? THEN 'RETIRED' ELSE state END WHERE revision=?",params![wire,retire,revision])?;
        tx.execute(
            "UPDATE capture_meta SET used_bytes=used_bytes+? WHERE id=1",
            [delta],
        )?;
        let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            let actual: (String, String) = self.db.query_row(
                "SELECT state,baseline_json FROM capture_scope_revisions WHERE revision=?",
                [revision],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                actual == (if retire { "RETIRED".into() } else { state }, wire),
                "protected checkpoint readback mismatch"
            );
            Ok(())
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
}
