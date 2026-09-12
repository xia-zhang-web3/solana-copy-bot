//! Necessary PumpSwap instruction presence, not trader/amount or CPI-success proof.
use std::collections::HashSet;

#[path = "pumpswap_instruction_json.rs"]
mod json;
#[path = "pumpswap_instruction_proto.rs"]
mod proto;

pub(super) use json::json_has_supported_swap;
pub(super) use proto::proto_has_supported_swap;

pub(super) fn requires_pumpswap_instruction(
    program_ids: &HashSet<String>,
    dex_hint: &str,
    pumpswap_program_ids: &HashSet<String>,
) -> bool {
    program_ids
        .iter()
        .any(|program| pumpswap_program_ids.contains(program))
        || dex_hint.to_ascii_lowercase().contains("pump")
}

// Encodings used by the local PumpSwap direct builder. Other versions are unsupported.
fn supported_data(data: &[u8]) -> bool {
    (data.len() == 24 && data.starts_with(&[51, 230, 133, 164, 1, 127, 131, 173]))
        || (data.len() == 25
            && data.starts_with(&[198, 46, 21, 82, 180, 217, 232, 112])
            && data[24] <= 1)
}

fn valid_pubkey(key: &str) -> bool {
    let mut bytes = [0_u8; 32];
    bs58::decode(key).onto(&mut bytes[..]) == Ok(32)
}
