//! Locked plain Base58 decode is bijective; compare the former predicate too.
use super::value;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

fn before(v: &Value, length: Option<usize>) -> Result<Vec<u8>> {
    let text = v.as_str().context("http_recovery_expected_base58")?;
    let decoded = bs58::decode(text).into_vec()?;
    ensure!(
        length.is_none_or(|n| decoded.len() == n) && bs58::encode(&decoded).into_string() == text,
        "http_recovery_base58_identity"
    );
    Ok(decoded)
}
fn equivalent(v: &Value, length: Option<usize>) {
    let old = before(v, length);
    let new = value::base58(v, length);
    match (old, new) {
        (Ok(old), Ok(new)) => assert_eq!(old, new),
        (Err(old), Err(new)) => assert_eq!(old.to_string(), new.to_string()),
        _ => panic!("Base58 accepted/rejected set changed for length={length:?}"),
    }
}

#[test]
fn base58_bijection_preserves_empty_leading_zero_digits_and_fixed_lengths() {
    for s in [
        "",
        "1",
        "11",
        "111111",
        "123456789",
        "11123456789",
        "zzzz",
        "2",
    ] {
        for length in [None, Some(32), Some(64)] {
            equivalent(&json!(s), length);
        }
    }
    for length in [32, 64] {
        for leading in 0..=length {
            let mut bytes = vec![251; length];
            bytes[..leading].fill(0);
            let text = bs58::encode(&bytes).into_string();
            equivalent(&json!(text), Some(length));
            assert_eq!(value::base58(&json!(text), Some(length)).unwrap(), bytes);
        }
        for wrong in [length - 1, length + 1] {
            let v = json!("1".repeat(wrong));
            equivalent(&v, Some(length));
            assert!(value::base58(&v, Some(length)).is_err());
            equivalent(&v, None);
            assert_eq!(value::base58(&v, None).unwrap(), vec![0; wrong]);
        }
    }
    let alphabet = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    for &a in alphabet {
        equivalent(&json!((a as char).to_string()), None);
        for &b in alphabet {
            equivalent(&json!(String::from_utf8(vec![a, b]).unwrap()), None);
        }
    }
}

#[test]
fn base58_rejects_identical_invalid_ascii_unicode_whitespace_and_non_strings() {
    for s in [
        "0", "O", "I", "l", "1 2", " 123", "123 ", "123\n", "\t123", "12+3", "12/3", "12=3", "\0",
        "é", "1é", "１２", "💰",
    ] {
        for length in [None, Some(32), Some(64)] {
            let v = json!(s);
            equivalent(&v, length);
            assert!(value::base58(&v, length).is_err());
        }
    }
    for v in [
        Value::Null,
        json!(1),
        json!(false),
        json!([]),
        json!({"key":"1"}),
    ] {
        equivalent(&v, None);
        assert!(value::base58(&v, None).is_err());
    }
}

#[derive(Debug, Default)]
struct Timing {
    count: u64,
    total_us: u64,
    max_us: u64,
}
impl Timing {
    fn note(&mut self, elapsed: Duration) {
        let us: u64 = elapsed.as_micros().try_into().unwrap();
        self.count += 1;
        self.total_us = self.total_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
    }
}
#[derive(Debug, Default)]
pub(super) struct DifferentialTimings {
    fields: u64,
    fixed32: u64,
    fixed64: u64,
    variable: u64,
    before: Timing,
    after: Timing,
    normalizer: Timing,
}
impl DifferentialTimings {
    pub(super) fn normalization(&mut self, elapsed: Duration) {
        self.normalizer.note(elapsed);
    }
    fn field(&mut self, v: &Value, length: Option<usize>, actual: Option<&[u8]>) {
        let start = Instant::now();
        let old = before(v, length).unwrap();
        self.before.note(start.elapsed());
        let start = Instant::now();
        let new = value::base58(v, length).unwrap();
        self.after.note(start.elapsed());
        assert_eq!(old, new, "saved Base58 output bytes changed");
        if let Some(actual) = actual {
            assert_eq!(old, actual, "actual normalized proto bytes");
        }
        self.fields += 1;
        match length {
            Some(32) => self.fixed32 += 1,
            Some(64) => self.fixed64 += 1,
            None => self.variable += 1,
            _ => unreachable!(),
        }
    }
    fn list(&mut self, raw: &Value, actual: &[Vec<u8>], length: Option<usize>) {
        let rows = rows(raw);
        assert_eq!(rows.len(), actual.len());
        for (v, bytes) in rows.iter().zip(actual) {
            self.field(v, length, Some(bytes));
        }
    }
}
fn rows(v: &Value) -> &[Value] {
    if v.is_null() {
        &[]
    } else {
        v.as_array().unwrap()
    }
}

fn saved_identity_mutations(raw: &Value) {
    let Some(tx) = rows(&raw["transactions"]).first() else {
        return;
    };
    for (v, length) in [
        (&tx["transaction"]["signatures"][0], 64),
        (&tx["transaction"]["message"]["accountKeys"][0], 32),
    ] {
        let text = v.as_str().unwrap();
        for changed in [
            format!("0{text}"),
            format!(" {text}"),
            format!("é{text}"),
            format!("{text}\n"),
            format!("1{text}"),
        ] {
            let mutated = json!(changed);
            equivalent(&mutated, Some(length));
            assert!(value::base58(&mutated, Some(length)).is_err());
        }
        // The extra leading zero is meaningful bytes, valid for unbounded data.
        equivalent(&json!(format!("1{text}")), None);
    }
}

/// Walk RPC field paths independently and compare both predicates with actual
/// normalized proto bytes, without generating or adjusting fixture expectations.
pub(super) fn walk_saved_fields(
    raw: &Value,
    block: &SubscribeUpdateBlock,
    timings: &mut DifferentialTimings,
) {
    saved_identity_mutations(raw);
    assert_eq!(raw["blockhash"].as_str().unwrap(), block.blockhash);
    assert_eq!(
        raw["previousBlockhash"].as_str().unwrap(),
        block.parent_blockhash
    );
    timings.field(&raw["blockhash"], Some(32), None);
    timings.field(&raw["previousBlockhash"], Some(32), None);
    let transactions = rows(&raw["transactions"]);
    assert_eq!(transactions.len(), block.transactions.len());
    for (row, info) in transactions.iter().zip(&block.transactions) {
        let tx = info.transaction.as_ref().unwrap();
        let message = tx.message.as_ref().unwrap();
        let source = &row["transaction"];
        let raw_message = &source["message"];
        timings.list(&source["signatures"], &tx.signatures, Some(64));
        assert_eq!(&info.signature, &tx.signatures[0]);
        timings.list(&raw_message["accountKeys"], &message.account_keys, Some(32));
        timings.field(
            &raw_message["recentBlockhash"],
            Some(32),
            Some(&message.recent_blockhash),
        );
        let instructions = rows(&raw_message["instructions"]);
        assert_eq!(instructions.len(), message.instructions.len());
        for (ix, typed) in instructions.iter().zip(&message.instructions) {
            timings.field(&ix["data"], None, Some(&typed.data));
        }
        let lookups = rows(&raw_message["addressTableLookups"]);
        assert_eq!(lookups.len(), message.address_table_lookups.len());
        for (lookup, typed) in lookups.iter().zip(&message.address_table_lookups) {
            timings.field(&lookup["accountKey"], Some(32), Some(&typed.account_key));
        }
        let raw_meta = &row["meta"];
        let meta = info.meta.as_ref().unwrap();
        timings.list(
            &raw_meta["loadedAddresses"]["writable"],
            &meta.loaded_writable_addresses,
            Some(32),
        );
        timings.list(
            &raw_meta["loadedAddresses"]["readonly"],
            &meta.loaded_readonly_addresses,
            Some(32),
        );
        let groups = rows(&raw_meta["innerInstructions"]);
        assert_eq!(groups.len(), meta.inner_instructions.len());
        for (group, typed) in groups.iter().zip(&meta.inner_instructions) {
            let instructions = rows(&group["instructions"]);
            assert_eq!(instructions.len(), typed.instructions.len());
            for (ix, typed) in instructions.iter().zip(&typed.instructions) {
                timings.field(&ix["data"], None, Some(&typed.data));
            }
        }
        match (&raw_meta["returnData"], &meta.return_data) {
            (Value::Null, None) => (),
            (raw, Some(typed)) => {
                timings.field(&raw["programId"], Some(32), Some(&typed.program_id))
            }
            _ => panic!("saved return data presence changed"),
        }
    }
}
