use anyhow::{ensure, Context, Result};
use serde_json::Value;

pub(super) fn uint(v: &Value, name: &str) -> Result<u64> {
    v[name]
        .as_u64()
        .with_context(|| format!("http_recovery_missing_integer:{name}"))
}
pub(super) fn u32_field(v: &Value, name: &str) -> Result<u32> {
    Ok(u32::try_from(uint(v, name)?)?)
}
pub(super) fn optional_uint(v: &Value, name: &str) -> Result<Option<u64>> {
    match &v[name] {
        Value::Null => Ok(None),
        n => Ok(Some(n.as_u64().with_context(|| {
            format!("http_recovery_invalid_optional_integer:{name}")
        })?)),
    }
}
pub(super) fn text<'a>(v: &'a Value, name: &str) -> Result<&'a str> {
    v[name]
        .as_str()
        .with_context(|| format!("http_recovery_missing_string:{name}"))
}
pub(super) fn array(v: &Value) -> Result<&Vec<Value>> {
    v.as_array().context("http_recovery_expected_array")
}
pub(super) fn optional_text<'a>(v: &'a Value, name: &str) -> Result<&'a str> {
    match &v[name] {
        Value::Null => Ok(""),
        Value::String(text) => Ok(text),
        _ => anyhow::bail!("http_recovery_invalid_optional_string:{name}"),
    }
}
pub(super) fn list<T>(v: &Value, f: impl Fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    if v.is_null() {
        return Ok(vec![]);
    }
    array(v)?.iter().map(f).collect()
}
pub(super) fn base58(v: &Value, length: Option<usize>) -> Result<Vec<u8>> {
    let text = v.as_str().context("http_recovery_expected_base58")?;
    let decoded = bs58::decode(text).into_vec()?;
    ensure!(
        length.is_none_or(|n| decoded.len() == n) && bs58::encode(&decoded).into_string() == text,
        "http_recovery_base58_identity"
    );
    Ok(decoded)
}
pub(super) fn indexes(v: &Value) -> Result<Vec<u8>> {
    array(v)?
        .iter()
        .map(|n| {
            Ok(u8::try_from(
                n.as_u64().context("http_recovery_invalid_index")?,
            )?)
        })
        .collect()
}
pub(super) fn base64(text: &str) -> Result<Vec<u8>> {
    ensure!(text.len() % 4 == 0, "http_recovery_base64_padding");
    let mut bytes = Vec::with_capacity(text.len() / 4 * 3);
    let groups = text.as_bytes().chunks_exact(4);
    let count = groups.len();
    for (index, group) in groups.enumerate() {
        let digit = |c| -> Result<u32> {
            Ok(match c {
                b'A'..=b'Z' => (c - b'A') as u32,
                b'a'..=b'z' => (c - b'a' + 26) as u32,
                b'0'..=b'9' => (c - b'0' + 52) as u32,
                b'+' => 62,
                b'/' => 63,
                _ => anyhow::bail!("http_recovery_base64_character"),
            })
        };
        let a = digit(group[0])?;
        let b = digit(group[1])?;
        let padding = if group[2] == b'=' {
            2
        } else if group[3] == b'=' {
            1
        } else {
            0
        };
        ensure!(
            padding == 0 || index + 1 == count,
            "http_recovery_base64_padding"
        );
        let c = if padding == 2 {
            ensure!(group[3] == b'=', "http_recovery_base64_padding");
            0
        } else {
            digit(group[2])?
        };
        let d = if padding > 0 { 0 } else { digit(group[3])? };
        ensure!(
            (padding != 2 || b & 15 == 0) && (padding != 1 || c & 3 == 0),
            "http_recovery_base64_unused_bits"
        );
        let n = (a << 18) | (b << 12) | (c << 6) | d;
        bytes.push((n >> 16) as u8);
        if padding < 2 {
            bytes.push((n >> 8) as u8);
        }
        if padding == 0 {
            bytes.push(n as u8);
        }
    }
    Ok(bytes)
}
