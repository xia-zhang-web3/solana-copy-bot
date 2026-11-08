//! Bounded extra raw mint/account reads; missing evidence never becomes a profile.
use crate::live_inventory::{
    LiveTokenPosition, Token2022ProfileCollectionFailure as Failure, TokenProgram,
};
use crate::live_portfolio_rpc::LivePortfolioRpcClient;
use crate::token2022_profile::{
    Token2022BoundAccount, Token2022PositionProfile, Token2022RawAccount, PROFILE_VERSION,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const MAX_KEYS: usize = 256;
const BATCH_KEYS: usize = 32;
const MAX_RAW_BYTES: usize = 4096;

pub(crate) fn collect(
    client: &LivePortfolioRpcClient,
    wallet: &str,
    slot: u64,
    positions: &mut [LiveTokenPosition],
) {
    let keys: BTreeSet<String> = positions
        .iter()
        .filter(|p| p.program == TokenProgram::Token2022)
        .flat_map(|p| {
            std::iter::once(p.mint.clone())
                .chain(p.token_2022_accounts.iter().map(|a| a.pubkey.clone()))
        })
        .collect();
    if keys.len() > MAX_KEYS {
        for p in positions
            .iter_mut()
            .filter(|p| p.program == TokenProgram::Token2022)
        {
            p.token_2022_collection_failure = Some(Failure::Budget);
        }
        return;
    }
    let keys: Vec<_> = keys.into_iter().collect();
    let mut accounts = BTreeMap::new();
    for chunk in keys.chunks(BATCH_KEYS) {
        let response = client.post_profile_rpc(
            json!([chunk, {"encoding":"base64", "commitment":"confirmed", "minContextSlot":slot}]),
        );
        let parsed = response
            .map_err(|reason| match reason {
                crate::live_inventory::InventoryFailure::Budget => Failure::Budget,
                crate::live_inventory::InventoryFailure::Transport => Failure::Unavailable,
                _ => Failure::Malformed,
            })
            .and_then(|r| parse_batch(&r, chunk, slot));
        match parsed {
            Ok(rows) => accounts.extend(rows),
            Err(reason) => accounts.extend(chunk.iter().cloned().map(|k| (k, Err(reason)))),
        }
    }
    for p in positions
        .iter_mut()
        .filter(|p| p.program == TokenProgram::Token2022)
    {
        match assemble(wallet, p, &accounts) {
            Ok(profile) => {
                p.token_2022_profile = Some(profile);
                p.token_2022_collection_failure = None;
            }
            Err(reason) => {
                p.token_2022_profile = None;
                p.token_2022_collection_failure = Some(reason);
            }
        }
    }
}

fn parse_batch(
    response: &Value,
    keys: &[String],
    minimum_slot: u64,
) -> Result<BTreeMap<String, Result<Token2022RawAccount, Failure>>, Failure> {
    let slot = response
        .pointer("/result/context/slot")
        .and_then(Value::as_u64)
        .ok_or(Failure::Malformed)?;
    if slot < minimum_slot {
        return Err(Failure::SlotBehind);
    }
    let rows = response
        .pointer("/result/value")
        .and_then(Value::as_array)
        .ok_or(Failure::Malformed)?;
    if rows.len() != keys.len() {
        return Err(Failure::Malformed);
    }
    Ok(keys
        .iter()
        .zip(rows)
        .map(|(key, row)| (key.clone(), raw_account(row, slot)))
        .collect())
}

fn raw_account(row: &Value, slot: u64) -> Result<Token2022RawAccount, Failure> {
    if row.is_null() {
        return Err(Failure::Missing);
    }
    let program_owner = row
        .get("owner")
        .and_then(Value::as_str)
        .ok_or(Failure::Malformed)?;
    if program_owner.len() > 44 {
        return Err(Failure::Malformed);
    }
    let executable = row
        .get("executable")
        .and_then(Value::as_bool)
        .ok_or(Failure::Malformed)?;
    let data = row
        .get("data")
        .and_then(Value::as_array)
        .ok_or(Failure::Malformed)?;
    if data.len() != 2 || data[1].as_str() != Some("base64") {
        return Err(Failure::Malformed);
    }
    let encoded = data[0].as_str().ok_or(Failure::Malformed)?;
    if encoded.len() > MAX_RAW_BYTES.div_ceil(3) * 4 {
        return Err(Failure::Budget);
    }
    let data = STANDARD.decode(encoded).map_err(|_| Failure::Malformed)?;
    if data.len() > MAX_RAW_BYTES {
        return Err(Failure::Budget);
    }
    Ok(Token2022RawAccount {
        program_owner: program_owner.to_string(),
        context_slot: slot,
        executable,
        data,
    })
}

fn assemble(
    wallet: &str,
    position: &LiveTokenPosition,
    accounts: &BTreeMap<String, Result<Token2022RawAccount, Failure>>,
) -> Result<Token2022PositionProfile, Failure> {
    let get = |key: &String| accounts.get(key).cloned().unwrap_or(Err(Failure::Missing));
    let mint_account = get(&position.mint)?;
    let token_accounts = position
        .token_2022_accounts
        .iter()
        .map(|a| {
            Ok(Token2022BoundAccount {
                pubkey: a.pubkey.clone(),
                raw: get(&a.pubkey)?,
                expected_raw_amount: a.raw_amount,
                expected_decimals: a.decimals,
                expected_state: a.state.clone(),
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(Token2022PositionProfile {
        contract_version: PROFILE_VERSION,
        wallet: wallet.to_string(),
        mint: position.mint.clone(),
        mint_account,
        accounts: token_accounts,
    })
}
