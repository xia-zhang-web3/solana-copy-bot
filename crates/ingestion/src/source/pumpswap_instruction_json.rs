use super::{supported_data, valid_pubkey};
use serde_json::Value;
use std::collections::HashSet;

pub(crate) fn json_has_supported_swap(
    result: &Value,
    meta: &Value,
    pumpswap_program_ids: &HashSet<String>,
) -> bool {
    // jsonParsed includes loaded addresses in accountKeys. Only actual, partially
    // decoded instructions with pubkey references are supported; compiled numeric
    // indexes and parsed instruction names cannot supply positive evidence here.
    let Some(keys) = result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
    else {
        return false;
    };
    let keys: HashSet<&str> = keys
        .iter()
        .filter_map(|key| key.as_str().or_else(|| key.get("pubkey")?.as_str()))
        .filter(|key| valid_pubkey(key))
        .collect();
    let Some(top) = result
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
    else {
        return false;
    };
    let supported =
        |instruction: &Value| supported_instruction(instruction, &keys, pumpswap_program_ids);
    top.iter().any(supported)
        || meta
            .get("innerInstructions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|group| {
                group
                    .get("index")
                    .and_then(Value::as_u64)
                    .is_some_and(|index| index < top.len() as u64)
            })
            .filter_map(|group| group.get("instructions")?.as_array())
            .flatten()
            .any(supported)
}

fn supported_instruction(
    instruction: &Value,
    keys: &HashSet<&str>,
    pumpswap_program_ids: &HashSet<String>,
) -> bool {
    // Ambiguous hybrid/compiled encodings are unsupported, even with a programId.
    if instruction.get("programIdIndex").is_some() || instruction.get("parsed").is_some() {
        return false;
    }
    let Some(program) = instruction.get("programId").and_then(Value::as_str) else {
        return false;
    };
    if !pumpswap_program_ids.contains(program) || !keys.contains(program) {
        return false;
    }
    let Some(accounts) = instruction.get("accounts").and_then(Value::as_array) else {
        return false;
    };
    if accounts.is_empty()
        || !accounts.iter().all(|account| {
            account
                .as_str()
                .is_some_and(|account| keys.contains(account))
        })
    {
        return false;
    }
    let Some(data) = instruction.get("data").and_then(Value::as_str) else {
        return false;
    };
    let mut bytes = [0_u8; 25];
    match bs58::decode(data).onto(&mut bytes[..]) {
        Ok(len) => supported_data(&bytes[..len]),
        Err(_) => false,
    }
}
