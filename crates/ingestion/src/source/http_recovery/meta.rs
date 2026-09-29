use super::{transaction::instruction, value::*};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

fn tokens(v: &Value, key_count: usize) -> Result<Vec<TokenBalance>> {
    list(v, |row| {
        let account_index = u32_field(row, "accountIndex")?;
        ensure!(
            (account_index as usize) < key_count,
            "http_recovery_token_index"
        );
        let raw = text(&row["uiTokenAmount"], "amount")?;
        let _: u64 = raw.parse().context("http_recovery_invalid_raw_amount")?;
        let decimals = u32_field(&row["uiTokenAmount"], "decimals")?;
        ensure!(decimals <= 255, "http_recovery_token_decimals");
        let ui = &row["uiTokenAmount"]["uiAmount"];
        // Proto uses a scalar default for RPC null. Exact amount/string and the
        // original JSON null remain retained; no financial amount uses this default.
        let ui_amount = if ui.is_null() {
            0.0
        } else {
            ui.as_f64().context("http_recovery_token_ui_amount")?
        };
        Ok(TokenBalance {
            account_index,
            mint: text(row, "mint")?.into(),
            owner: optional_text(row, "owner")?.into(),
            program_id: optional_text(row, "programId")?.into(),
            ui_token_amount: Some(UiTokenAmount {
                ui_amount,
                decimals,
                amount: raw.into(),
                ui_amount_string: text(&row["uiTokenAmount"], "uiAmountString")?.into(),
            }),
        })
    })
}
pub(super) fn rewards(v: &Value) -> Result<Vec<Reward>> {
    list(v, |row| {
        let reward_type = match &row["rewardType"] {
            Value::Null => RewardType::Unspecified,
            Value::String(kind) => match kind.as_str() {
                "fee" | "Fee" => RewardType::Fee,
                "rent" => RewardType::Rent,
                "staking" => RewardType::Staking,
                "voting" => RewardType::Voting,
                "deactivatedStake" => RewardType::DeactivatedStake,
                _ => anyhow::bail!("http_recovery_unknown_reward_type"),
            },
            _ => anyhow::bail!("http_recovery_invalid_reward_type"),
        };
        let commission = |name| -> Result<String> {
            match &row[name] {
                Value::Null => Ok(String::new()),
                n => Ok(n
                    .as_u64()
                    .context("http_recovery_reward_commission")?
                    .to_string()),
            }
        };
        Ok(Reward {
            pubkey: text(row, "pubkey")?.into(),
            lamports: row["lamports"]
                .as_i64()
                .context("http_recovery_reward_lamports")?,
            post_balance: uint(row, "postBalance")?,
            reward_type: reward_type as i32,
            commission: commission("commission")?,
            commission_bps: commission("commissionBps")?,
        })
    })
}
pub(super) fn parse(
    v: &Value,
    key_count: usize,
    loaded_writable_addresses: Vec<Vec<u8>>,
    loaded_readonly_addresses: Vec<Vec<u8>>,
) -> Result<TransactionStatusMeta> {
    ensure!(
        v.is_object() && v.get("err").is_some(),
        "http_recovery_missing_meta_error"
    );
    let balances = |name| -> Result<Vec<u64>> {
        let balances = array(&v[name])?
            .iter()
            .map(|n| n.as_u64().context("http_recovery_invalid_native_balance"))
            .collect::<Result<Vec<_>>>()?;
        ensure!(balances.len() == key_count, "http_recovery_balance_count");
        Ok(balances)
    };
    if let Some(status) = v.get("status") {
        ensure!(
            (v["err"].is_null() && status.get("Ok").is_some())
                || (!v["err"].is_null() && status.get("Err") == Some(&v["err"])),
            "http_recovery_status_error_conflict"
        );
    }
    let err = if v["err"].is_null() {
        None
    } else {
        ensure!(
            v["err"].is_string() || v["err"].is_object(),
            "http_recovery_invalid_transaction_error"
        );
        Some(TransactionError {
            err: super::error::encode(&v["err"])?,
        })
    };
    let inner_instructions = list(&v["innerInstructions"], |group| {
        Ok(InnerInstructions {
            index: u32_field(group, "index")?,
            instructions: array(&group["instructions"])?
                .iter()
                .map(|raw| {
                    let ix = instruction(raw, key_count)?;
                    Ok(InnerInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts,
                        data: ix.data,
                        stack_height: optional_uint(raw, "stackHeight")?
                            .map(u32::try_from)
                            .transpose()?,
                    })
                })
                .collect::<Result<_>>()?,
        })
    })?;
    let return_data = match &v["returnData"] {
        Value::Null => None,
        raw => {
            let data = array(&raw["data"])?;
            ensure!(
                data.len() == 2 && data[1] == "base64",
                "http_recovery_return_data_encoding"
            );
            Some(ReturnData {
                program_id: base58(&raw["programId"], Some(32))?,
                data: base64(
                    data[0]
                        .as_str()
                        .context("http_recovery_return_data_string")?,
                )?,
            })
        }
    };
    Ok(TransactionStatusMeta {
        err,
        fee: uint(v, "fee")?,
        pre_balances: balances("preBalances")?,
        post_balances: balances("postBalances")?,
        inner_instructions,
        inner_instructions_none: v["innerInstructions"].is_null(),
        log_messages: list(&v["logMessages"], |line| {
            Ok(line.as_str().context("http_recovery_log_string")?.into())
        })?,
        log_messages_none: v["logMessages"].is_null(),
        pre_token_balances: tokens(&v["preTokenBalances"], key_count)?,
        post_token_balances: tokens(&v["postTokenBalances"], key_count)?,
        rewards: rewards(&v["rewards"])?,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        return_data_none: v["returnData"].is_null(),
        compute_units_consumed: optional_uint(v, "computeUnitsConsumed")?,
        cost_units: optional_uint(v, "costUnits")?,
    })
}
