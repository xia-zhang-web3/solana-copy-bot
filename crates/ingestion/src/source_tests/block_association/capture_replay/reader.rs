use super::*;
use anyhow::{bail, ensure, Context};
use manifest::{bounded_file, number, MAX_RESERVE};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Capture {
    pub manifest: Value,
    pub messages: Vec<(u64, SubscribeUpdate)>,
    pub policy: policy::DecoderPolicy,
    pub ignored_kinds: BTreeMap<String, u64>,
}

// Capture-only integrity boundary. No fixture directory, expected verdicts or
// caller-supplied decoder config. Every refusal precedes association/analysis.
pub(super) fn read(dir: &Path) -> Result<Capture> {
    let raw_manifest = bounded_file(&dir.join("manifest.json"), MAX_RESERVE)?;
    let m: Value = serde_json::from_slice(&raw_manifest)?;
    let limits = manifest::validate(&m)?;
    ensure!(
        raw_manifest.len() as u64 <= limits.metadata_reserve,
        "profile manifest size bound"
    );
    let policy = request::validate(&m["request"])?;
    let rows = m["messages"].as_array().context("missing message rows")?;
    ensure!(
        rows.len() as u64 == number(&m, "messages_received")?
            && rows.len() as u64 <= limits.messages,
        "message row count mismatch"
    );
    let mut messages = vec![];
    let mut bytes = 0u64;
    let mut previous = 0;
    let mut kinds = BTreeMap::<String, u64>::new();
    let mut ignored_kinds = BTreeMap::<String, u64>::new();
    let mut files = BTreeSet::from(["manifest.json".to_string()]);
    for (i, row) in rows.iter().enumerate() {
        ensure!(
            number(row, "sequence")? == (i + 1) as u64 && row["session_id"] == m["session_id"],
            "message order/session mismatch"
        );
        let ns = number(row, "arrival_offset_ns")?;
        ensure!(
            ns >= previous && ns <= number(&m, "elapsed_ns")?,
            "arrival order/bound mismatch"
        );
        previous = ns;
        let size = number(row, "encoded_bytes")?;
        ensure!(
            size <= limits.message_bytes && row.get("refused").is_none(),
            "refused/oversized row in retained prefix"
        );
        let kind = row["kind"].as_str().context("missing message kind")?;
        *kinds.entry(kind.into()).or_default() += 1;
        match kind {
            "ping" | "pong" | "slot" | "block_meta" | "transaction_status" => {
                ensure!(
                    row["saved"] == false
                        && row.get("file").is_none()
                        && row.get("sha256").is_none(),
                    "invalid ignored metadata row"
                );
                *ignored_kinds.entry(kind.into()).or_default() += 1;
                continue;
            }
            "transaction" | "block" => ensure!(
                row["saved"] == true,
                "unsaved transaction/block in retained prefix"
            ),
            _ => bail!("unexpected heavy or unknown message kind"),
        }
        let name = format!("{:06}.pb", i + 1);
        ensure!(row["file"] == name, "unexpected envelope filename");
        files.insert(name.clone());
        ensure!(
            bytes + size + limits.metadata_reserve <= limits.total_bytes,
            "capture total output bound"
        );
        let raw = bounded_file(&dir.join(&name), limits.message_bytes)?;
        ensure!(
            raw.len() as u64 == size && row["sha256"] == format!("{:x}", Sha256::digest(&raw)),
            "envelope size/hash mismatch"
        );
        bytes += size;
        let decoded =
            SubscribeUpdate::decode(raw.as_slice()).context("invalid envelope protobuf")?;
        ensure!(
            decoded.encode_to_vec() == raw,
            "unsupported envelope wire representation"
        );
        match decoded.update_oneof.as_ref() {
            Some(subscribe_update::UpdateOneof::Transaction(_)) => {
                ensure!(kind == "transaction", "message kind mismatch")
            }
            Some(subscribe_update::UpdateOneof::Block(b)) => ensure!(
                kind == "block" && b.accounts.is_empty() && b.entries.is_empty(),
                "block kind/heavy input mismatch"
            ),
            _ => bail!("saved envelope is not transaction/block"),
        }
        messages.push((ns, decoded));
    }
    ensure!(
        m["message_kinds"] == json!(kinds)
            && number(&m, "payload_bytes")? == bytes
            && number(&m, "envelopes_written")? == messages.len() as u64,
        "manifest counters mismatch"
    );
    let mut actual = BTreeSet::new();
    for entry in std::fs::read_dir(dir)?.take(limits.messages as usize + 2) {
        let e = entry?;
        let name = e
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid filename"))?;
        ensure!(files.contains(&name), "undeclared capture file");
        actual.insert(name);
    }
    ensure!(actual == files, "missing capture file");
    Ok(Capture {
        manifest: m,
        messages,
        policy,
        ignored_kinds,
    })
}
