//! Storage boundaries on task-owned temporary databases, without provider/network input.
use copybot_core_types::association_delivery::{
    InfoIdentity, MessageTime, ProtectedBlockIdentity, ProtectedEvidence, ProtectedObservation,
    ProtectedSourceKind,
};
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use copybot_core_types::association_recovery::{BlockCheckpoint, DurableCheckpoint, ReplayScope};
use copybot_storage_core::capture_scope::{CaptureStore, SCHEMA};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

struct Fixture {
    _dir: TempDir,
    path: std::path::PathBuf,
    db: Connection,
    store: CaptureStore,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db.execute(
            "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000)",
            [],
        )
        .unwrap();
        let mut store = CaptureStore::open(&path).unwrap();
        store.start().unwrap();
        Self {
            _dir: dir,
            path,
            db,
            store,
        }
    }
    fn request(&self, wallet: &str) -> i64 {
        self.db
            .execute(
                "INSERT INTO capture_requests(request_key,payload,expires) VALUES(?,'{}',99999)",
                [wallet],
            )
            .unwrap();
        let id = self.db.last_insert_rowid();
        self.db
            .execute(
                "INSERT INTO capture_members VALUES(?,?)",
                params![id, wallet],
            )
            .unwrap();
        id
    }
    fn bind_stream(&mut self, revision: i64, seq: i64, signature: &str, raw: &[u8]) {
        let mut e = self.evidence();
        e.source_kind = ProtectedSourceKind::Stream;
        e.raw = raw.to_vec();
        e.sha256 = format!("{:x}", Sha256::digest(&e.raw));
        self.store.commit_evidence(revision, &e).unwrap();
        let mut o = self.observation(&e);
        o.signature = Some(signature.into());
        o.message_time = MessageTime::CreatedAt {
            seconds: 2,
            nanos: 0,
        };
        o.block = None;
        o.transaction_fingerprint = "fingerprint".into();
        self.store
            .commit_observation(revision, &o, Some(seq), |a, b| a == b)
            .unwrap();
    }
    fn finish_no_swap_outbox(&self) {
        let mut q=self.db.prepare("SELECT r.id,r.signature,r.wallet,r.slot,r.observation_json,r.provenance_json,r.raw_hash FROM capture_recovery r JOIN capture_recovery_outbox o ON o.recovery_id=r.id WHERE o.state='PENDING'").unwrap();
        let rows = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        drop(q);
        for (id, signature, wallet, slot, observation, provenance, raw_hash) in rows {
            let observation: ProtectedObservation = serde_json::from_str(&observation).unwrap();
            assert!(observation.facts.is_none());
            let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
            let event = serde_json::json!({"version":1,"recovery_id":id,"signature":signature,"wallet":wallet,"slot":slot.parse::<u64>().unwrap(),"source_kind":observation.source_kind,"provenance":provenance,"block":observation.block,"raw_sha256":raw_hash,"timeliness":"not_established","trading_admission":false,"follower_action":"none","status":"EVIDENCE_ONLY","reason":"recovered_non_swap_or_undecodable"});
            let disposition = serde_json::to_string(&event).unwrap();
            let reason = "recovered_non_swap_or_undecodable";
            let hash = forward_hash("REJECTED", reason, &disposition);
            self.db.execute("UPDATE capture_recovery_outbox SET state='REJECTED',reason=?,disposition_json=?,disposition_sha256=? WHERE recovery_id=?",params![reason,disposition,hash,id]).unwrap();
        }
    }
    fn head(wallets: Vec<String>, slot: u64) -> DurableCheckpoint {
        DurableCheckpoint {
            session: "test-scope-session".into(),
            sequence: 1,
            from_slot: slot,
            overlap: vec![],
            block: BlockCheckpoint {
                scope: ReplayScope {
                    policy: "durable_checkpoint_replay_v1".into(),
                    wallets,
                    programs: vec!["program".into()],
                    raydium_programs: vec![],
                    pumpswap_programs: vec![],
                },
                observation: ParentObservation {
                    child: BlockKey {
                        slot,
                        hash: "11111111111111111111111111111111".into(),
                    },
                    parent: BlockKey {
                        slot: slot - 1,
                        hash: "11111111111111111111111111111111".into(),
                    },
                    issue: None,
                },
                executed_transaction_count: 1,
                supplied_transaction_count: 1,
                claims: vec![],
            },
        }
    }
    fn evidence(&self) -> ProtectedEvidence {
        let raw = b"controlled complete block fixture".to_vec();
        ProtectedEvidence {
            source_kind: ProtectedSourceKind::CompleteLiveBlock,
            slot: 7,
            sha256: format!("{:x}", Sha256::digest(&raw)),
            raw,
            available_at_micros: 1000000,
            block_time: Some(10),
            anchor: None,
        }
    }
    fn observation(&self, e: &ProtectedEvidence) -> ProtectedObservation {
        ProtectedObservation {
            signature: Some("signature".into()),
            wallet: "alice".into(),
            slot: 7,
            facts: None,
            token_quantity: None,
            info: InfoIdentity {
                encoded: vec![1, 2, 3],
                float_bits: vec![],
            },
            info_hash: "normalized-hash".into(),
            message_time: MessageTime::RecoveredBlock {
                block_time: Some(10),
            },
            source_kind: e.source_kind,
            evidence_sha256: e.sha256.clone(),
            block: Some(ProtectedBlockIdentity {
                blockhash: "hash".into(),
                parent_slot: 6,
                parent_blockhash: "parent".into(),
                transaction_index: 0,
                executed_transaction_count: 1,
                block_time: Some(10),
            }),
            transaction_fingerprint: "not-stream".into(),
        }
    }
}

fn forward_hash(state: &str, reason: &str, disposition: &str) -> String {
    let mut hash = Sha256::new();
    for part in [state, reason, disposition] {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

#[path = "protected_recovery/lifecycle.rs"]
mod lifecycle;

#[path = "protected_recovery/provenance.rs"]
mod provenance;

#[path = "protected_recovery/unknown.rs"]
mod unknown;

#[path = "protected_recovery/forward.rs"]
mod forward;
