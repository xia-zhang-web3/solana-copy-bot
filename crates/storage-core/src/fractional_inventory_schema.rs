//! Raw transaction shape required by the frozen inventory producer, v0/legacy subset.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::BTreeSet;
const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
fn signature(v: &Value) -> Result<()> {
    let s = v.as_str().context("fraction_signature_encoding")?;
    ensure!(s.len() <= 88, "fraction_signature_encoding");
    let mut out = [0u8; 64];
    for c in s.bytes() {
        let mut carry = BASE58
            .iter()
            .position(|b| *b == c)
            .context("fraction_signature_encoding")? as u32;
        for b in out.iter_mut().rev() {
            carry += u32::from(*b) * 58;
            *b = carry as u8;
            carry >>= 8;
        }
        ensure!(carry == 0, "fraction_signature_encoding");
    }
    let nonzero = out.iter().position(|b| *b != 0).unwrap_or(64);
    ensure!(
        s.bytes().take_while(|b| *b == b'1').count() + 64 - nonzero == 64,
        "fraction_signature_encoding"
    );
    Ok(())
}
fn instruction(ix: &Value, keys: usize, inner: bool) -> Result<()> {
    let object = ix.as_object().context("fraction_instruction_shape")?;
    ensure!(
        object.keys().all(|k| matches!(
            k.as_str(),
            "accounts" | "data" | "programIdIndex" | "stackHeight"
        )),
        "fraction_instruction_shape"
    );
    let indices = ix["accounts"]
        .as_array()
        .context("fraction_instruction_indices")?;
    for v in indices.iter().chain(std::iter::once(&ix["programIdIndex"])) {
        ensure!(
            v.as_u64().is_some_and(|n| n < keys as u64),
            "fraction_instruction_indices"
        );
    }
    let data = ix["data"].as_str().context("fraction_instruction_data")?;
    ensure!(
        data.bytes().all(|b| BASE58.contains(&b)),
        "fraction_instruction_encoding"
    );
    ensure!(
        !inner || ix["stackHeight"].as_u64().is_some_and(|n| n >= 2),
        "fraction_cpi_depth"
    );
    Ok(())
}
pub(super) fn check(t: &Value, key_count: usize) -> Result<()> {
    let tx = &t["transaction"];
    let msg = &tx["message"];
    let meta = &t["meta"];
    ensure!(
        msg.as_object()
            .context("fraction_message_shape")?
            .keys()
            .all(|k| matches!(
                k.as_str(),
                "accountKeys"
                    | "addressTableLookups"
                    | "header"
                    | "instructions"
                    | "recentBlockhash"
            )),
        "fraction_message_shape"
    );
    let static_count = msg["accountKeys"]
        .as_array()
        .context("fraction_keys")?
        .len() as u64;
    let h = &msg["header"];
    let required = h["numRequiredSignatures"]
        .as_u64()
        .context("fraction_signer_header")?;
    ensure!(
        required > 0
            && required <= static_count
            && h["numReadonlySignedAccounts"]
                .as_u64()
                .is_some_and(|n| n <= required)
            && h["numReadonlyUnsignedAccounts"]
                .as_u64()
                .is_some_and(|n| n <= static_count - required),
        "fraction_signer_header"
    );
    for s in tx["signatures"].as_array().context("fraction_signatures")? {
        signature(s)?;
    }
    if let Some(loaded) = meta.get("loadedAddresses") {
        ensure!(
            loaded
                .as_object()
                .context("fraction_loaded_keys")?
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from(["writable", "readonly"]),
            "fraction_loaded_keys"
        );
    } else {
        ensure!(
            msg.get("addressTableLookups")
                .is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty)),
            "fraction_loaded_keys_missing"
        );
    }
    for ix in msg["instructions"]
        .as_array()
        .context("fraction_instructions_missing")?
    {
        instruction(ix, key_count, false)?;
    }
    for group in meta["innerInstructions"]
        .as_array()
        .context("fraction_cpi_missing")?
    {
        for ix in group["instructions"]
            .as_array()
            .context("fraction_cpi_missing")?
        {
            instruction(ix, key_count, true)?;
        }
    }
    Ok(())
}
