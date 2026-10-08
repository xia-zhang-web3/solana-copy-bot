//! Sealed original result bytes; only the numeric RPC id is adapted to this client.
use anyhow::{ensure, Context, Result};
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

pub(super) struct Corpus {
    pub records: BTreeMap<u64, Value>,
    pub anchor: SubscribeUpdateBlock,
    pub count: usize,
    root: PathBuf,
}
fn checked(bytes: &[u8], record: &Value, byte_key: &str, hash_key: &str) -> Result<()> {
    ensure!(
        bytes.len() as u64 == record[byte_key].as_u64().context("fixture bytes")?,
        "fixture byte integrity"
    );
    ensure!(
        format!("{:x}", Sha256::digest(bytes))
            == record[hash_key].as_str().context("fixture sha")?,
        "fixture sha integrity"
    );
    Ok(())
}
impl Corpus {
    pub fn load(number: usize) -> Result<Self> {
        let text = if number == 0 {
            include_str!("../../../tests/fixtures/recovery_06_profile_465.json")
        } else {
            include_str!("../../../tests/fixtures/recovery_06_profile_659.json")
        };
        let profile: Value = serde_json::from_str(text)?;
        let records: BTreeMap<_, _> = profile["records"]
            .as_array()
            .context("fixture records")?
            .iter()
            .map(|r| (r["slot"].as_u64().unwrap(), r.clone()))
            .collect();
        let pair = PathBuf::from(std::env::var("COPYBOT_RECOVERY_06_ANCHORS_DIR")?)
            .join(format!("pair-{:02}", number + 1));
        let manifest: Value = serde_json::from_slice(&std::fs::read(pair.join("manifest.json"))?)?;
        ensure!(
            manifest["complete"] == true && manifest["comparison"] == "MATCH",
            "original anchor not complete MATCH"
        );
        let pb = std::fs::read(pair.join("grpc_typed.pb"))?;
        checked(&pb, &manifest["files"]["grpc_typed.pb"], "bytes", "sha256")?;
        let floats = std::fs::read(pair.join("grpc_ui_amount_bits.bin"))?;
        checked(
            &floats,
            &manifest["files"]["grpc_ui_amount_bits.bin"],
            "bytes",
            "sha256",
        )?;
        ensure!(floats.len() % 17 == 0, "anchor float sidecar size");
        let mut anchor = SubscribeUpdateBlock::decode(pb.as_slice())?;
        for row in floats.chunks_exact(17) {
            let position = u32::from_le_bytes(row[..4].try_into()?) as usize;
            let index = u32::from_le_bytes(row[5..9].try_into()?) as usize;
            let bits = u64::from_le_bytes(row[9..17].try_into()?);
            let meta = anchor
                .transactions
                .get_mut(position)
                .context("float transaction")?
                .meta
                .as_mut()
                .context("float meta")?;
            let balances = match row[4] {
                0 => &mut meta.pre_token_balances,
                1 => &mut meta.post_token_balances,
                _ => anyhow::bail!("float side"),
            };
            balances
                .get_mut(index)
                .context("float row")?
                .ui_token_amount
                .as_mut()
                .context("float amount")?
                .ui_amount = f64::from_bits(bits);
        }
        ensure!(
            anchor.slot == profile["live_anchor_slot"].as_u64().unwrap(),
            "independent anchor slot"
        );
        Ok(Self {
            count: records.len(),
            records,
            anchor,
            root: PathBuf::from(std::env::var("COPYBOT_RECOVERY_06_HTTP_EVIDENCE_DIR")?),
        })
    }
    pub fn bytes(&self, slot: u64) -> Result<Vec<u8>> {
        let r = self.records.get(&slot).context("fixture slot")?;
        let name = r["body_file"].as_str().context("body filename")?;
        ensure!(
            PathBuf::from(name).file_name().and_then(|s| s.to_str()) == Some(name),
            "fixture basename"
        );
        let bytes = std::fs::read(self.root.join(name))?;
        checked(&bytes, r, "body_bytes", "body_sha256")?;
        ensure!(bytes.len() <= 16 << 20, "accepted raw body limit");
        Ok(bytes)
    }
    pub fn delay_ms(&self, slot: u64) -> u64 {
        let r = &self.records[&slot];
        r["observed_attempt_service_lower_bound_ms"]
            .as_u64()
            .unwrap()
            + r["modeled_minimum_retry_backoff_ms"].as_u64().unwrap()
    }
    pub fn initial(&self) -> Result<SubscribeUpdateBlock> {
        let slot = *self.records.keys().nth(1).context("initial child")?;
        let mut value: Value = serde_json::from_slice(&self.bytes(slot)?)?;
        crate::source::http_recovery::normalize_confirmed_http_block(slot, &value["result"].take())
    }
    pub fn adapted(&self, slot: u64, id: u64) -> Result<Vec<u8>> {
        adapt_rpc_id(
            self.bytes(slot)?,
            self.records[&slot]["rpc_id"].as_u64().unwrap(),
            id,
        )
    }
}

fn adapt_rpc_id(mut bytes: Vec<u8>, expected: u64, id: u64) -> Result<Vec<u8>> {
    // Inspect only the envelope token, irrespective of top-level property order.
    // Nested result objects and escaped strings must never be mistaken for its id.
    let (mut depth, mut quoted, mut escaped, mut string_start) = (0usize, false, false, 0usize);
    let mut span = None;
    for (i, &byte) in bytes.iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
                continue;
            }
            if byte == b'\\' {
                escaped = true;
                continue;
            }
            if byte != b'"' {
                continue;
            }
            quoted = false;
            if depth != 1 || &bytes[string_start..i] != b"id" {
                continue;
            }
            let mut at = i + 1;
            while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
                at += 1;
            }
            if bytes.get(at) != Some(&b':') {
                continue;
            }
            at += 1;
            while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
                at += 1;
            }
            let mut end = at;
            while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                end += 1;
            }
            ensure!(
                span.is_none() && end > at,
                "unambiguous numeric envelope id"
            );
            ensure!(
                std::str::from_utf8(&bytes[at..end])?.parse::<u64>()? == expected,
                "original RPC id"
            );
            span = Some((at, end));
            continue;
        }
        match byte {
            b'"' => {
                quoted = true;
                string_start = i + 1;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth = depth.checked_sub(1).context("envelope nesting")?;
            }
            _ => {}
        }
    }
    ensure!(!quoted && depth == 0, "envelope complete");
    let (start, end) = span.context("top-level id token")?;
    bytes.splice(start..end, id.to_string().bytes());
    ensure!(bytes.len() <= 16 << 20, "adapted raw body limit");
    Ok(bytes)
}

#[test]
fn rpc_id_splice_handles_saved_property_order_and_preserves_nested_result_bytes() {
    for body in [
        br#"{"jsonrpc":"2.0","id":1234,"result":{"id":9,"amount":2.153092023604439,"s":"escaped\\\"id\\\""}}"#.as_slice(),
        br#"{"jsonrpc":"2.0","result":{"id":9,"amount":2.153092023604439,"s":"escaped\\\"id\\\""},"id":1234}"#.as_slice(),
    ] {
        let adapted = adapt_rpc_id(body.to_vec(), 1234, 1).unwrap();
        let before: Value = serde_json::from_slice(body).unwrap();
        let after: Value = serde_json::from_slice(&adapted).unwrap();
        assert_eq!(before["result"], after["result"]);
        assert_eq!(after["id"], 1);
        let expected = std::str::from_utf8(body).unwrap().replace("\"id\":1234", "\"id\":1");
        assert_eq!(adapted, expected.as_bytes());
    }
}
