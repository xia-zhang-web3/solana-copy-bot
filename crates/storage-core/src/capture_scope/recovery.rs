//! Atomic protected evidence, checked identities and recovery forward outbox.
use super::CaptureStore;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{
    InfoIdentity, ProtectedEvidence, ProtectedObservation, ProtectedSourceKind,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct ProtectedReceipt {
    pub id: i64,
    /// A recovery-first identity must never be upgraded to a fresh stream signal.
    pub recovery_first: bool,
    pub live_seq: Option<i64>,
}
#[derive(Debug, Clone)]
pub struct StoredProtectedObservation {
    pub receipt: ProtectedReceipt,
    pub observation: ProtectedObservation,
}
fn sha(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}
#[derive(serde::Deserialize)]
struct EvidenceMetadata {
    source_kind: ProtectedSourceKind,
    slot: u64,
    sha256: String,
    available_at_micros: i64,
    block_time: Option<i64>,
    #[serde(default)]
    anchor: Option<copybot_core_types::association_delivery::ProtectedAnchorLink>,
}
fn metadata(e: &ProtectedEvidence) -> serde_json::Value {
    json!({"source_kind":e.source_kind,"slot":e.slot,"sha256":e.sha256,
        "available_at_micros":e.available_at_micros,"block_time":e.block_time,"anchor":e.anchor})
}
fn revision(db: &rusqlite::Connection, id: i64, restore: bool) -> Result<Vec<String>> {
    let (state, wire): (String, String) = db.query_row(
        "SELECT state,wallets_json FROM capture_scope_revisions WHERE revision=?",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        state == "ACTIVE" || state == "PLANNED" || (restore && state == "RETIRED"),
        "protected observation revision retired"
    );
    Ok(serde_json::from_str(&wire)?)
}
pub(super) fn read(
    db: &rusqlite::Connection,
    identity_key: &str,
) -> Result<Option<StoredProtectedObservation>> {
    let row:Option<(i64,Option<i64>,String,bool)>=db.query_row("SELECT id,live_seq,observation_json,EXISTS(SELECT 1 FROM capture_recovery_outbox o WHERE o.recovery_id=r.id) FROM capture_recovery r WHERE identity_key=?",[identity_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    row.map(|(id, live_seq, wire, recovery_first)| {
        let observation:ProtectedObservation=serde_json::from_str(&wire)?;
        let (signature,wallet,slot,facts,info,raw,raw_hash,evidence):(Option<String>,String,String,String,String,Vec<u8>,String,String)=db.query_row("SELECT signature,wallet,slot,event_json,info_json,raw,raw_hash,evidence_id FROM capture_recovery WHERE id=?",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
        ensure!(signature==observation.signature && wallet==observation.wallet && slot==observation.slot.to_string() && key(&observation)==identity_key,"protected stored identity mismatch");
        ensure!(serde_json::from_str::<Option<copybot_core_types::association_delivery::CheckedFacts>>(&facts)?==observation.facts && serde_json::from_str::<InfoIdentity>(&info)?==observation.info,"protected stored decoded identity mismatch");
        ensure!(raw==observation.info.encoded && raw_hash==sha(&raw) && evidence==observation.evidence_sha256,"protected stored raw evidence binding mismatch");
        let (provenance,evidence_meta):(String,String)=db.query_row("SELECT r.provenance_json,e.provenance_json FROM capture_recovery r JOIN capture_recovery_evidence e ON e.evidence_id=r.evidence_id WHERE r.id=?",[id],|r|Ok((r.get(0)?,r.get(1)?)))?;
        let provenance:serde_json::Value=serde_json::from_str(&provenance)?;
        ensure!(provenance["source_kind"]==serde_json::to_value(observation.source_kind)? && provenance["block"]==serde_json::to_value(&observation.block)? && provenance["message_time"]==serde_json::to_value(&observation.message_time)? && provenance["token_quantity"]==serde_json::to_value(&observation.token_quantity)? && provenance["info_hash"]==observation.info_hash && provenance["transaction_fingerprint"]==observation.transaction_fingerprint && provenance["evidence"]==serde_json::from_str::<serde_json::Value>(&evidence_meta)?,"protected stored provenance binding mismatch");
        let anchors=super::recovery_anchor::resolved(db,&observation.evidence_sha256)?;
        ensure!(provenance["anchor_links"].as_array().is_some_and(|saved|saved.iter().all(|link|anchors.contains(link))),"protected anchor link readback mismatch");
        ensure!(recovery_first==(observation.source_kind!=ProtectedSourceKind::Stream || live_seq.is_none()),"protected stored disposition binding mismatch");
        Ok(StoredProtectedObservation {receipt: ProtectedReceipt {id,recovery_first,live_seq},observation})
    }).transpose()
}
fn key(o: &ProtectedObservation) -> String {
    o.signature
        .clone()
        .unwrap_or_else(|| format!("unsigned:{}:{}:{}", o.slot, o.wallet, o.evidence_sha256))
}

impl CaptureStore {
    pub fn protected_prior(&self, signature: &str) -> Result<Option<StoredProtectedObservation>> {
        self.known()?;
        read(&self.db, signature)
    }
    pub fn evidence(&self, hash: &str) -> Result<ProtectedEvidence> {
        self.known()?;
        let (raw, wire): (Vec<u8>, String) = self.db.query_row(
            "SELECT raw,provenance_json FROM capture_recovery_evidence WHERE evidence_id=?",
            [hash],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let meta: EvidenceMetadata = serde_json::from_str(&wire)?;
        let evidence = ProtectedEvidence {
            raw,
            source_kind: meta.source_kind,
            slot: meta.slot,
            sha256: meta.sha256,
            available_at_micros: meta.available_at_micros,
            block_time: meta.block_time,
            anchor: meta.anchor,
        };
        ensure!(
            sha(&evidence.raw) == hash && evidence.sha256 == hash,
            "protected stored evidence hash mismatch"
        );
        Ok(evidence)
    }
    /// The exact bounded source envelope commits before its observations/checkpoint.
    pub fn commit_evidence(&mut self, scope_revision: i64, e: &ProtectedEvidence) -> Result<()> {
        self.known()?;
        ensure!(
            e.available_at_micros >= 0 && !e.raw.is_empty() && sha(&e.raw) == e.sha256,
            "protected evidence hash/time invalid"
        );
        let kind = serde_json::to_value(e.source_kind)?
            .as_str()
            .context("source kind")?
            .to_owned();
        let provenance = serde_json::to_string(&metadata(e))?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::recovery_write::active(&tx, self.epoch)?;
        let prior: Option<(String, String, Vec<u8>)> = tx
            .query_row(
                "SELECT source_kind,slot,raw FROM capture_recovery_evidence WHERE evidence_id=?",
                [&e.sha256],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((source, slot, raw)) = prior {
            ensure!(
                source == kind && slot == e.slot.to_string() && raw == e.raw,
                "protected evidence identity conflict"
            );
            super::recovery_anchor::record(&tx, e)?;
            let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
                super::recovery_anchor::verify(&self.db, e)?;
                Ok(())
            });
            if result.is_err() {
                self.uncertain = true;
            }
            return result;
        }
        revision(&tx, scope_revision, false)?;
        let charge = e.raw.len() + provenance.len() + 1024;
        super::recovery_write::capacity(&tx, charge, 1)?;
        tx.execute("INSERT INTO capture_recovery_evidence(evidence_id,scope_revision,source_kind,slot,raw,raw_hash,provenance_json) VALUES(?,?,?,?,?,?,?)",params![e.sha256,scope_revision,kind,e.slot.to_string(),e.raw,e.sha256,provenance])?;
        super::recovery_write::charge(&tx, charge)?;
        super::recovery_anchor::record(&tx, e)?;
        let result=tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            let actual:(String,String,Vec<u8>,String)=self.db.query_row("SELECT source_kind,slot,raw,provenance_json FROM capture_recovery_evidence WHERE evidence_id=?",[&e.sha256],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            ensure!(actual==(kind,e.slot.to_string(),e.raw.clone(),provenance),"protected evidence commit readback mismatch"); super::recovery_anchor::verify(&self.db,e)?; Ok(())
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
    /// `equivalent` is the existing checked RPC/Geyser info-equivalence predicate.
    /// It cannot be replaced with equality of raw RPC and protobuf hashes.
    pub fn commit_observation<F>(
        &mut self,
        scope_revision: i64,
        o: &ProtectedObservation,
        live_seq: Option<i64>,
        equivalent: F,
    ) -> Result<ProtectedReceipt>
    where
        F: Fn(&InfoIdentity, &InfoIdentity) -> bool,
    {
        self.known()?;
        ensure!(
            !o.wallet.is_empty() && o.wallet.len() <= 128 && o.info.encoded.len() <= 8_388_608,
            "protected observation identity/size invalid"
        );
        if let Some(f) = &o.facts {
            ensure!(
                Some(f.signature.as_str()) == o.signature.as_deref()
                    && f.wallet == o.wallet
                    && f.slot == o.slot,
                "protected observation decoded identity mismatch"
            );
        }
        ensure!(
            o.source_kind == ProtectedSourceKind::Stream
                || (o.block.is_some()
                    && matches!(
                        o.message_time,
                        copybot_core_types::association_delivery::MessageTime::RecoveredBlock { .. }
                    )),
            "protected recovered provenance invalid"
        );
        let identity = key(o);
        let observation = serde_json::to_string(o)?;
        let facts = serde_json::to_string(&o.facts)?;
        ensure!(
            facts.len() <= 16_384,
            "protected decoded size bound exceeded"
        );
        let info = serde_json::to_string(&o.info)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::recovery_write::active(&tx, self.epoch)?;
        let wallets = revision(
            &tx,
            scope_revision,
            live_seq.is_some() && o.source_kind == ProtectedSourceKind::Stream,
        )?;
        ensure!(
            wallets.binary_search(&o.wallet).is_ok(),
            "protected observation outside immutable scope"
        );
        let (evidence_kind,evidence_slot,evidence):(String,String,String)=tx.query_row("SELECT source_kind,slot,provenance_json FROM capture_recovery_evidence WHERE evidence_id=?",[&o.evidence_sha256],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        let kind = serde_json::to_value(o.source_kind)?
            .as_str()
            .context("source kind")?
            .to_owned();
        ensure!(
            evidence_kind == kind && evidence_slot == o.slot.to_string(),
            "protected observation evidence binding mismatch"
        );
        let provenance = serde_json::to_string(
            &json!({"source_kind":o.source_kind,"evidence":serde_json::from_str::<serde_json::Value>(&evidence)?,"block":o.block,"message_time":o.message_time,"info_hash":o.info_hash,"transaction_fingerprint":o.transaction_fingerprint,"token_quantity":o.token_quantity,"anchor_links":super::recovery_anchor::resolved(&tx,&o.evidence_sha256)?}),
        )?;
        if let Some(prior) = read(&tx, &identity)? {
            let p = &prior.observation;
            ensure!(
                p.signature == o.signature
                    && p.wallet == o.wallet
                    && p.slot == o.slot
                    && p.facts == o.facts
                    && p.token_quantity == o.token_quantity
                    && equivalent(&p.info, &o.info),
                "protected checked identity conflict"
            );
            let prior_link =
                super::recovery_links::read(&tx, prior.receipt.id, &o.evidence_sha256)?;
            let expected_link = prior_link
                .clone()
                .unwrap_or_else(|| (kind.clone(), provenance.clone()));
            ensure!(
                expected_link.0 == kind,
                "protected duplicate evidence kind conflict"
            );
            if prior_link.is_none() {
                super::recovery_write::capacity(&tx, provenance.len() + 512, 1)?;
                tx.execute(
                    "INSERT INTO capture_recovery_links VALUES(?,?,?,?)",
                    params![prior.receipt.id, o.evidence_sha256, kind, provenance],
                )?;
                super::recovery_write::charge(&tx, provenance.len() + 512)?;
            }
            let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
                let actual =
                    read(&self.db, &identity)?.context("protected duplicate readback missing")?;
                ensure!(
                    actual.observation == prior.observation
                        && actual.receipt.recovery_first == prior.receipt.recovery_first,
                    "protected duplicate readback changed"
                );
                super::recovery_links::verify(
                    &self.db,
                    actual.receipt.id,
                    &o.evidence_sha256,
                    &expected_link,
                )?;
                Ok(actual.receipt)
            });
            if result.is_err() {
                self.uncertain = true;
            }
            return result;
        }
        if o.source_kind == ProtectedSourceKind::Stream {
            if let Some(seq) = live_seq {
                let bound: (Option<String>, String, String, i64, String) = tx.query_row(
                    "SELECT signature,wallet,slot,epoch,fingerprint FROM capture_events WHERE seq=?",
                    [seq],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?,r.get(3)?,r.get(4)?)),
                )?;
                let epoch: i64 = tx.query_row(
                    "SELECT epoch FROM capture_scope_revisions WHERE revision=?",
                    [scope_revision],
                    |r| r.get(0),
                )?;
                ensure!(
                    bound
                        == (
                            o.signature.clone(),
                            o.wallet.clone(),
                            o.slot.to_string(),
                            epoch,
                            o.transaction_fingerprint.clone()
                        ),
                    "protected stream receipt identity mismatch"
                );
            }
        } else {
            ensure!(
                live_seq.is_none(),
                "recovered observation cannot claim original stream receipt"
            );
        }
        let raw_hash = sha(&o.info.encoded);
        let charge = observation.len()
            + facts.len()
            + info.len()
            + provenance.len()
            + o.info.encoded.len()
            + 4096;
        let recovered = o.source_kind != ProtectedSourceKind::Stream || live_seq.is_none();
        super::recovery_write::capacity(&tx, charge, if recovered { 3 } else { 2 })?;
        tx.execute("INSERT INTO capture_recovery(identity_key,signature,wallet,slot,scope_revision,event_json,info_json,provenance_json,observation_json,raw,raw_hash,evidence_id,live_seq) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",params![identity,o.signature,o.wallet,o.slot.to_string(),scope_revision,facts,info,provenance,observation,o.info.encoded,raw_hash,o.evidence_sha256,live_seq])?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO capture_recovery_links VALUES(?,?,?,?)",
            params![id, o.evidence_sha256, kind, provenance],
        )?;
        if recovered {
            tx.execute(
                "INSERT INTO capture_recovery_outbox(recovery_id) VALUES(?)",
                [id],
            )?;
        }
        super::recovery_write::charge(&tx, charge)?;
        let result = tx.commit().map_err(anyhow::Error::from).and_then(|_| {
            let actual =
                read(&self.db, &identity)?.context("protected observation readback missing")?;
            ensure!(
                actual.observation == *o
                    && actual.receipt.recovery_first == recovered
                    && actual.receipt.live_seq == live_seq,
                "protected observation commit readback mismatch"
            );
            super::recovery_links::verify(
                &self.db,
                actual.receipt.id,
                &o.evidence_sha256,
                &(kind, provenance),
            )?;
            Ok(actual.receipt)
        });
        if result.is_err() {
            self.uncertain = true;
        }
        result
    }
}
