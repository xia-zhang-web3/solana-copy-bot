//! Fourth capture is a real transport recovery; three earlier full pairs are setup.
use super::corpus::{self, Corpus};
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{AdmissionFacts, MessageTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path};

const ANCHOR: u64 = corpus::FIRST + 16;

pub(super) struct Proof {
    pairs: BTreeMap<String, String>,
    anchor_ack: Option<Value>,
    recovered_seen: bool,
    fresh_seen_after_ack: bool,
}

impl Proof {
    pub(super) fn preload(out: &Path, corpus: &Corpus) -> Result<Self> {
        let root = out.join("anchor-pairs");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        ensure!(fs::read_dir(&root)?.next().is_none(), "fresh diagnostic directory");
        // Setup uses background blocks; OLD's synthetic transaction is for the
        // transport decoder case, not an independently captured full anchor.
        for slot in [corpus::FIRST, corpus::FIRST + 2, corpus::FIRST + 3] {
            let grpc = corpus.block(slot);
            let raw = corpus.raw(slot);
            let http = crate::source::http_recovery::normalize_confirmed_http_block(slot, &raw)?;
            let wire = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":slot,"result":raw}))?;
            ensure!(
                crate::source::http_recovery::anchor_evidence_io_tests::compare(
                    root.to_str(), &grpc, &http, &wire
                )?,
                "preloaded full pair must MATCH"
            );
        }
        let proof = Self {
            pairs: inventory(&root)?, anchor_ack: None,
            recovered_seen: false, fresh_seen_after_ack: false,
        };
        save(&out.join("SATURATION_PRELOAD.json"), &json!({
            "purpose":"three full diagnostic pairs only; not three transport reconnects",
            "preloaded_pairs":3,"files":proof.pairs,
            "next_actual_transport_anchor":ANCHOR,"ack_proven":false
        }))?;
        Ok(proof)
    }

    pub(super) fn committed_ack(&mut self, slot: u64, hash: &str, observed: chrono::DateTime<chrono::Utc>) {
        if slot == ANCHOR {
            self.anchor_ack = Some(json!({"slot":slot,"hash":hash,
                "committed_readback_and_receiver_ack_unix_ns":observed.timestamp_nanos_opt().unwrap()}));
        }
    }

    pub(super) fn admission(&mut self, a: &AdmissionFacts) -> Result<()> {
        if a.facts.slot == corpus::RECOVERED {
            ensure!(matches!(a.message_time, MessageTime::RecoveredBlock { .. }),
                "HTTP origin must remain RecoveredBlock");
            self.recovered_seen = true;
        }
        if a.facts.slot == corpus::FRESH {
            ensure!(self.anchor_ack.is_some(), "fresh BUY before committed fourth anchor ACK");
            ensure!(matches!(a.message_time, MessageTime::CreatedAt { .. }),
                "post-recovery BUY must have real producer CreatedAt");
            self.fresh_seen_after_ack = true;
        }
        Ok(())
    }

    pub(super) fn state(&self) -> Value {
        json!({"preloaded_pair_count":3,"fourth_anchor":ANCHOR,
            "committed_anchor_ack":self.anchor_ack,"recovered_admission_seen":self.recovered_seen,
            "fresh_created_at_admission_after_ack":self.fresh_seen_after_ack})
    }

    pub(super) fn unchanged(&self, out: &Path) -> Result<()> {
        let root = out.join("anchor-pairs");
        ensure!(inventory(&root)? == self.pairs, "first three complete pair files changed");
        let pair_dirs = fs::read_dir(&root)?.filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("pair-")).count();
        ensure!(pair_dirs == 3, "full capture grew beyond three pairs");
        save(&out.join("SATURATION_FILES_UNCHANGED.json"),
            &json!({"full_pair_count":pair_dirs,"hashes_unchanged":true,"files":self.pairs}))
    }

    pub(super) fn finish(&self, out: &Path, database: &Path, corpus: &Corpus, quotes: &[Value]) -> Result<()> {
        ensure!(self.anchor_ack.is_some() && self.recovered_seen && self.fresh_seen_after_ack,
            "fourth ACK/recovered/fresh causal chain incomplete");
        let sql = rusqlite::Connection::open(database)?;
        for (slot, expected) in [(corpus::RECOVERED, 0), (corpus::FRESH, 1)] {
            let n: u64 = sql.query_row(
                "SELECT count(*) FROM native_buy_decisions WHERE signature=?1",
                [corpus.signature(slot)], |r| r.get(0)
            )?;
            ensure!(n == expected, "recovered refusal or fresh native decision mismatch");
        }
        ensure!(quotes.len() == 1 && quotes[0]["signature"] == corpus.signature(corpus::FRESH),
            "single mock quote must belong to fresh BUY");
        save(&out.join("SATURATION_CONTINUATION.json"), &json!({
            "verdict":"PASS_FOURTH_CAPTURE_ACK_THEN_FRESH_NATIVE_MOCK_QUOTE",
            "chain":self.state(),"recovered_native_decisions":0,"fresh_native_decisions":1,
            "mock_quote":quotes[0],"first_three_full_pairs_unchanged":true,
            "real_provider_calls":0,"real_financial_calls":0,"live_copy_proven":false
        }))
    }
}

fn inventory(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for number in 1..=3 {
        let name = format!("pair-{number:02}");
        let directory = root.join(&name);
        let manifest: Value = serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
        ensure!(manifest["complete"] == true && manifest["comparison"] == "MATCH",
            "preloaded full-pair manifest incomplete");
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            ensure!(entry.file_type()?.is_file(), "unexpected diagnostic member");
            let file = entry.file_name().into_string().ok().context("UTF8 diagnostic path")?;
            result.insert(format!("{name}/{file}"), format!("{:x}", Sha256::digest(fs::read(entry.path())?)));
        }
    }
    Ok(result)
}

fn save(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}
