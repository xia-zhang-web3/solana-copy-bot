//! Test-only RPC-to-protobuf adapter for decoder replay, not a transport or clock.
//! RPC errors retain their JSON bytes (not Solana bincode); the decoder only tests
//! presence. Unused rewards/return-data fields are outside this adapter's scope.
use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

fn number(v: &Value, field: &str) -> Result<u64> {
    v[field]
        .as_u64()
        .with_context(|| format!("missing integer {field}"))
}
fn array(v: &Value) -> Result<&Vec<Value>> {
    v.as_array().context("expected array")
}
fn optional_list<T>(v: &Value, f: impl Fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    if v.is_null() {
        return Ok(vec![]);
    }
    array(v)?.iter().map(f).collect()
}
fn bytes(v: &Value, len: Option<usize>) -> Result<Vec<u8>> {
    let decoded = bs58::decode(v.as_str().context("expected base58 string")?).into_vec()?;
    ensure!(
        len.is_none_or(|n| decoded.len() == n),
        "base58 length mismatch"
    );
    Ok(decoded)
}
fn u32_number(v: &Value, field: &str) -> Result<u32> {
    Ok(u32::try_from(number(v, field)?)?)
}
fn indexes(v: &Value) -> Result<Vec<u8>> {
    array(v)?
        .iter()
        .map(|n| Ok(u8::try_from(n.as_u64().context("invalid account index")?)?))
        .collect()
}
fn ix(v: &Value, key_count: usize) -> Result<CompiledInstruction> {
    let program_id_index = u32_number(v, "programIdIndex")?;
    let accounts = indexes(&v["accounts"])?;
    ensure!(
        (program_id_index as usize) < key_count,
        "program index outside keys"
    );
    ensure!(
        accounts.iter().all(|i| (*i as usize) < key_count),
        "account index outside keys"
    );
    Ok(CompiledInstruction {
        program_id_index,
        accounts,
        data: bytes(&v["data"], None)?,
    })
}
fn tokens(v: &Value, key_count: usize) -> Result<Vec<TokenBalance>> {
    optional_list(v, |t| {
        let account_index = u32_number(t, "accountIndex")?;
        ensure!(
            (account_index as usize) < key_count,
            "token index outside keys"
        );
        let amount = &t["uiTokenAmount"];
        let raw = amount["amount"]
            .as_str()
            .context("missing raw token amount")?;
        raw.parse::<u64>()?;
        let decimals = u32_number(amount, "decimals")?;
        ensure!(decimals <= 255, "token decimals outside byte range");
        Ok(TokenBalance {
            account_index,
            mint: t["mint"].as_str().context("missing token mint")?.into(),
            owner: t["owner"].as_str().unwrap_or("").into(),
            program_id: t["programId"].as_str().unwrap_or("").into(),
            ui_token_amount: Some(UiTokenAmount {
                amount: raw.into(),
                decimals,
                ui_amount: amount["uiAmount"].as_f64().unwrap_or_default(),
                ui_amount_string: amount["uiAmountString"].as_str().unwrap_or("").into(),
            }),
        })
    })
}

pub(super) fn update(r: &Value) -> Result<SubscribeUpdateTransaction> {
    ensure!(!r.is_null(), "RPC transaction unavailable");
    let tx = &r["transaction"];
    let m = &tx["message"];
    ensure!(
        m.get("transactionConfig").is_none(),
        "message.transactionConfig unsupported for current decoder"
    );
    let meta = &r["meta"];
    ensure!(
        meta.is_object() && meta.get("err").is_some(),
        "missing meta/error evidence"
    );
    let signatures: Vec<_> = array(&tx["signatures"])?
        .iter()
        .map(|s| bytes(s, Some(64)))
        .collect::<Result<_>>()?;
    let signature = signatures
        .first()
        .context("missing first signature")?
        .clone();
    let account_keys: Vec<_> = array(&m["accountKeys"])?
        .iter()
        .map(|k| bytes(k, Some(32)))
        .collect::<Result<_>>()?;
    let loaded_writable_addresses =
        optional_list(&meta["loadedAddresses"]["writable"], |v| bytes(v, Some(32)))?;
    let loaded_readonly_addresses =
        optional_list(&meta["loadedAddresses"]["readonly"], |v| bytes(v, Some(32)))?;
    let key_count =
        account_keys.len() + loaded_writable_addresses.len() + loaded_readonly_addresses.len();
    ensure!(key_count <= 256, "too many resolved account keys");
    let header = MessageHeader {
        num_required_signatures: u32_number(&m["header"], "numRequiredSignatures")?,
        num_readonly_signed_accounts: u32_number(&m["header"], "numReadonlySignedAccounts")?,
        num_readonly_unsigned_accounts: u32_number(&m["header"], "numReadonlyUnsignedAccounts")?,
    };
    ensure!(
        header.num_required_signatures > 0
            && header.num_required_signatures as usize == signatures.len()
            && signatures.len() <= account_keys.len(),
        "invalid signer header"
    );
    let versioned = match &r["version"] {
        Value::String(s) if s == "legacy" => false,
        Value::Number(n) if n.as_u64() == Some(0) => true,
        _ => bail!("missing/unsupported message version"),
    };
    let pre_balances: Vec<_> = array(&meta["preBalances"])?
        .iter()
        .map(|v| v.as_u64().context("invalid pre balance"))
        .collect::<Result<_>>()?;
    let post_balances: Vec<_> = array(&meta["postBalances"])?
        .iter()
        .map(|v| v.as_u64().context("invalid post balance"))
        .collect::<Result<_>>()?;
    ensure!(
        pre_balances.len() == key_count && post_balances.len() == key_count,
        "resolved balance length mismatch"
    );
    let message = Message {
        header: Some(header),
        account_keys,
        recent_blockhash: bytes(&m["recentBlockhash"], Some(32))?,
        instructions: array(&m["instructions"])?
            .iter()
            .map(|v| ix(v, key_count))
            .collect::<Result<_>>()?,
        versioned,
        config: None,
        address_table_lookups: optional_list(&m["addressTableLookups"], |a| {
            Ok(MessageAddressTableLookup {
                account_key: bytes(&a["accountKey"], Some(32))?,
                writable_indexes: indexes(&a["writableIndexes"])?,
                readonly_indexes: indexes(&a["readonlyIndexes"])?,
            })
        })?,
    };
    let is_vote = message.instructions.iter().any(|i| {
        let mut keys = message
            .account_keys
            .iter()
            .chain(loaded_writable_addresses.iter())
            .chain(loaded_readonly_addresses.iter());
        keys.nth(i.program_id_index as usize).is_some_and(|k| {
            bs58::encode(k).into_string() == "Vote111111111111111111111111111111111111111"
        })
    });
    let status = TransactionStatusMeta {
        err: if meta["err"].is_null() {
            None
        } else {
            Some(TransactionError {
                err: serde_json::to_vec(&meta["err"])?,
            })
        },
        fee: number(meta, "fee")?,
        pre_balances,
        post_balances,
        pre_token_balances: tokens(&meta["preTokenBalances"], key_count)?,
        post_token_balances: tokens(&meta["postTokenBalances"], key_count)?,
        log_messages: optional_list(&meta["logMessages"], |v| {
            Ok(v.as_str().context("invalid log message")?.into())
        })?,
        log_messages_none: meta["logMessages"].is_null(),
        inner_instructions: optional_list(&meta["innerInstructions"], |g| {
            Ok(InnerInstructions {
                index: u32_number(g, "index")?,
                instructions: array(&g["instructions"])?
                    .iter()
                    .map(|v| {
                        let i = ix(v, key_count)?;
                        let stack_height = match &v["stackHeight"] {
                            Value::Null => None,
                            n => Some(u32::try_from(n.as_u64().context("invalid stackHeight")?)?),
                        };
                        Ok(InnerInstruction {
                            program_id_index: i.program_id_index,
                            accounts: i.accounts,
                            data: i.data,
                            stack_height,
                        })
                    })
                    .collect::<Result<_>>()?,
            })
        })?,
        inner_instructions_none: meta["innerInstructions"].is_null(),
        loaded_writable_addresses,
        loaded_readonly_addresses,
        compute_units_consumed: meta["computeUnitsConsumed"].as_u64(),
        cost_units: meta["costUnits"].as_u64(),
        ..Default::default()
    };
    Ok(SubscribeUpdateTransaction {
        slot: number(r, "slot")?,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature,
            is_vote,
            transaction: Some(Transaction {
                signatures,
                message: Some(message),
            }),
            meta: Some(status),
            ..Default::default()
        }),
    })
}
