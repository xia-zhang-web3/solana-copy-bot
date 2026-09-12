use super::types::{AccountObservation, KeyedAccountObservation};
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;

pub(super) fn result<'a>(body: &'a Value, expected_id: &str) -> Result<&'a Value> {
    let root = body
        .as_object()
        .ok_or_else(|| anyhow!("native_rpc_envelope"))?;
    ensure!(!root.contains_key("error"), "native_rpc_error_key");
    ensure!(
        root.get("jsonrpc").and_then(Value::as_str) == Some("2.0"),
        "native_rpc_jsonrpc"
    );
    ensure!(
        root.get("id").and_then(Value::as_str) == Some(expected_id),
        "native_rpc_id"
    );
    root.get("result")
        .ok_or_else(|| anyhow!("native_rpc_result"))
}

pub(super) fn envelope<'a>(
    body: &'a Value,
    expected_id: &str,
    floor: Option<u64>,
) -> Result<(u64, &'a Value)> {
    let result = result(body, expected_id)?
        .as_object()
        .ok_or_else(|| anyhow!("native_rpc_result"))?;
    let context = result
        .get("context")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native_rpc_context"))?;
    let slot = context
        .get("slot")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native_rpc_slot"))?;
    ensure!(
        floor.is_none_or(|floor| slot >= floor),
        "native_rpc_slot_below_floor"
    );
    let value = result
        .get("value")
        .ok_or_else(|| anyhow!("native_rpc_missing_value"))?;
    Ok((slot, value))
}

pub(super) fn fee(value: &Value) -> Result<Option<u64>> {
    if value.is_null() {
        Ok(None)
    } else {
        value
            .as_u64()
            .map(Some)
            .ok_or_else(|| anyhow!("native_rpc_fee_value"))
    }
}

pub(super) fn accounts(
    value: &Value,
    keys: &[PubkeyBytes],
) -> Result<Vec<KeyedAccountObservation>> {
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("native_rpc_accounts_value"))?;
    ensure!(rows.len() == keys.len(), "native_rpc_accounts_length");
    rows.iter()
        .zip(keys)
        .map(|(row, pubkey)| {
            Ok(KeyedAccountObservation {
                pubkey: *pubkey,
                account: account(row)?,
            })
        })
        .collect()
}

fn account(value: &Value) -> Result<AccountObservation> {
    if value.is_null() {
        return Ok(AccountObservation::Absent);
    }
    let row = value
        .as_object()
        .ok_or_else(|| anyhow!("native_rpc_account_object"))?;
    let lamports = row
        .get("lamports")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native_rpc_account_lamports"))?;
    let owner = row
        .get("owner")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native_rpc_account_owner"))?;
    ensure!(owner.len() <= 44, "native_rpc_account_owner");
    let owner_program: PubkeyBytes = bs58::decode(owner)
        .into_vec()
        .map_err(|_| anyhow!("native_rpc_account_owner"))?
        .try_into()
        .map_err(|_| anyhow!("native_rpc_account_owner"))?;
    let executable = row
        .get("executable")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("native_rpc_account_executable"))?;
    let encoded = row
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native_rpc_account_encoding"))?;
    ensure!(
        encoded.len() == 2 && encoded[1].as_str() == Some("base64"),
        "native_rpc_account_encoding"
    );
    let encoded_data = encoded[0]
        .as_str()
        .ok_or_else(|| anyhow!("native_rpc_account_encoding"))?;
    let data = STANDARD
        .decode(encoded_data)
        .map_err(|_| anyhow!("native_rpc_account_base64"))?;
    if let Some(space) = row.get("space") {
        ensure!(
            space.as_u64() == Some(data.len() as u64),
            "native_rpc_account_space"
        );
    }
    Ok(AccountObservation::Present {
        lamports,
        owner_program,
        executable,
        data,
    })
}
