//! Compiled JSON message normalization. Preserve V1/config markers for the
//! existing financial decoder refusal; normalization never authorizes V1 swaps.
use super::{meta, value::*};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

pub(super) fn instruction(v: &Value, key_count: usize) -> Result<CompiledInstruction> {
    let program_id_index = u32_field(v, "programIdIndex")?;
    let accounts = indexes(&v["accounts"])?;
    ensure!(
        (program_id_index as usize) < key_count
            && accounts.iter().all(|i| (*i as usize) < key_count),
        "http_recovery_instruction_index"
    );
    Ok(CompiledInstruction {
        program_id_index,
        accounts,
        data: base58(&v["data"], None)?,
    })
}
fn config(v: &Value) -> Result<TransactionConfig> {
    ensure!(
        v.is_object() || v.is_null(),
        "http_recovery_invalid_message_config"
    );
    if let Some(fields) = v.as_object() {
        ensure!(
            fields.keys().all(|key| matches!(
                key.as_str(),
                "priorityFee" | "computeUnitLimit" | "loadedAccountsDataSizeLimit" | "heapSize"
            )),
            "http_recovery_unknown_message_config"
        );
    }
    let small = |name| -> Result<Option<u32>> {
        optional_uint(v, name)?
            .map(u32::try_from)
            .transpose()
            .map_err(Into::into)
    };
    Ok(TransactionConfig {
        priority_fee: optional_uint(v, "priorityFee")?,
        compute_unit_limit: small("computeUnitLimit")?,
        loaded_accounts_data_size_limit: small("loadedAccountsDataSizeLimit")?,
        heap_size: small("heapSize")?,
    })
}
pub(super) fn parse(v: &Value, index: u64) -> Result<SubscribeUpdateTransactionInfo> {
    let tx = &v["transaction"];
    let raw = &tx["message"];
    ensure!(
        tx.is_object() && raw.is_object(),
        "http_recovery_compiled_transaction_required"
    );
    let signatures = array(&tx["signatures"])?
        .iter()
        .map(|s| base58(s, Some(64)))
        .collect::<Result<Vec<_>>>()?;
    let signature = signatures
        .first()
        .context("http_recovery_missing_signature")?
        .clone();
    let account_keys = array(&raw["accountKeys"])?
        .iter()
        .map(|key| base58(key, Some(32)))
        .collect::<Result<Vec<_>>>()?;
    let versioned = match &v["version"] {
        Value::String(s) if s == "legacy" => false,
        Value::Number(n) if matches!(n.as_u64(), Some(0 | 1)) => true,
        _ => anyhow::bail!("http_recovery_unknown_transaction_version"),
    };
    let config = raw.get("transactionConfig").map(config).transpose()?;
    ensure!(
        v["version"].as_u64() != Some(1) || config.is_some(),
        "http_recovery_v1_config_missing"
    );
    let loaded_writable_addresses = list(&v["meta"]["loadedAddresses"]["writable"], |key| {
        base58(key, Some(32))
    })?;
    let loaded_readonly_addresses = list(&v["meta"]["loadedAddresses"]["readonly"], |key| {
        base58(key, Some(32))
    })?;
    let key_count =
        account_keys.len() + loaded_writable_addresses.len() + loaded_readonly_addresses.len();
    ensure!(key_count <= 256, "http_recovery_account_key_bound");
    let header = MessageHeader {
        num_required_signatures: u32_field(&raw["header"], "numRequiredSignatures")?,
        num_readonly_signed_accounts: u32_field(&raw["header"], "numReadonlySignedAccounts")?,
        num_readonly_unsigned_accounts: u32_field(&raw["header"], "numReadonlyUnsignedAccounts")?,
    };
    let required = header.num_required_signatures as usize;
    ensure!(
        required > 0
            && required == signatures.len()
            && required <= account_keys.len()
            && header.num_readonly_signed_accounts as usize <= required
            && header.num_readonly_unsigned_accounts as usize <= account_keys.len() - required,
        "http_recovery_invalid_signer_header"
    );
    let instructions = array(&raw["instructions"])?
        .iter()
        .map(|ix| instruction(ix, key_count))
        .collect::<Result<Vec<_>>>()?;
    let is_vote = !versioned
        && signatures.len() < 3
        && instructions.len() == 1
        && account_keys
            .get(instructions[0].program_id_index as usize)
            .is_some_and(|key| {
                bs58::encode(key).into_string() == "Vote111111111111111111111111111111111111111"
            });
    let lookups = list(&raw["addressTableLookups"], |lookup| {
        Ok(MessageAddressTableLookup {
            account_key: base58(&lookup["accountKey"], Some(32))?,
            writable_indexes: indexes(&lookup["writableIndexes"])?,
            readonly_indexes: indexes(&lookup["readonlyIndexes"])?,
        })
    })?;
    ensure!(
        versioned
            || (lookups.is_empty()
                && loaded_writable_addresses.is_empty()
                && loaded_readonly_addresses.is_empty()),
        "http_recovery_legacy_loaded_addresses"
    );
    let message = Message {
        header: Some(header),
        account_keys,
        recent_blockhash: base58(&raw["recentBlockhash"], Some(32))?,
        instructions,
        versioned,
        address_table_lookups: lookups,
        config,
    };
    let meta = meta::parse(
        &v["meta"],
        key_count,
        loaded_writable_addresses,
        loaded_readonly_addresses,
    )?;
    Ok(SubscribeUpdateTransactionInfo {
        signature,
        is_vote,
        index,
        transaction: Some(Transaction {
            signatures,
            message: Some(message),
        }),
        meta: Some(meta),
    })
}
