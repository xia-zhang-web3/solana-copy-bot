use copybot_storage_core::{
    NativeAccountObservations, NativeInstructionObservation as Ix, NativeObservation as Obs,
    ObservationCoverage as Cov, ObservationSource as Src, MAX_NATIVE_INSTRUCTIONS,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
pub(super) const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SYSTEM: &str = "11111111111111111111111111111111";
const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub(super) fn text(v: Option<&Value>, source: Src) -> Obs {
    match v {
        None | Some(Value::Null) => Obs::unknown(Cov::Missing),
        Some(v) => match v.as_str().filter(|s| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }) {
            Some(s) => Obs::known(s, source),
            None => Obs::unknown(Cov::Invalid),
        },
    }
}
pub(super) fn number(v: Option<&Value>, source: Src) -> Obs {
    match v {
        None | Some(Value::Null) => Obs::unknown(Cov::Missing),
        Some(v) => {
            let n = v.as_u64().or_else(|| {
                v.as_str()
                    .and_then(|s| s.parse::<u64>().ok().filter(|n| n.to_string() == s))
            });
            n.map(|n| Obs::known(n, source))
                .unwrap_or_else(|| Obs::unknown(Cov::Invalid))
        }
    }
}
fn instruction(v: &Value, outer: u32, inner: Option<u32>) -> Ix {
    let mut ix = Ix {
        outer_index: outer,
        inner_index: inner,
        stack_height: number(v.get("stackHeight"), Src::ParsedInstruction),
        program_id: text(v.get("programId"), Src::ParsedInstruction),
        instruction_type: Obs::unknown(Cov::Unsupported),
        fields: BTreeMap::new(),
        coverage: Cov::Known,
    };
    let program = ix.program_id.value.as_deref();
    let kind = v.pointer("/parsed/type").and_then(Value::as_str);
    let fields: &[&str] = match (program, kind) {
        (Some(SYSTEM), Some("createAccount")) => {
            &["source", "newAccount", "lamports", "space", "owner"]
        }
        (Some(SYSTEM), Some("createAccountWithSeed")) => {
            &["source", "newAccount", "base", "lamports", "space", "owner"]
        }
        (Some(SYSTEM), Some("transfer")) => &["source", "destination", "lamports"],
        (Some(SYSTEM), Some("transferWithSeed")) => &[
            "source",
            "destination",
            "lamports",
            "sourceBase",
            "sourceOwner",
        ],
        (
            Some(TOKEN | TOKEN_2022),
            Some("initializeAccount" | "initializeAccount2" | "initializeAccount3"),
        ) => &["account", "mint", "owner"],
        (Some(TOKEN | TOKEN_2022), Some("closeAccount")) => &["account", "destination", "owner"],
        (Some(TOKEN | TOKEN_2022), Some("syncNative")) => &["account"],
        (Some(TOKEN | TOKEN_2022), Some("transfer")) => {
            &["source", "destination", "authority", "amount"]
        }
        (Some(TOKEN | TOKEN_2022), Some("transferChecked")) => &[
            "source",
            "destination",
            "authority",
            "mint",
            "amount",
            "decimals",
        ],
        (Some(TOKEN | TOKEN_2022), Some("setAuthority")) => {
            &["account", "authority", "authorityType", "newAuthority"]
        }
        (Some(ATA), Some("create" | "createIdempotent")) => {
            &["source", "account", "wallet", "mint", "tokenProgram"]
        }
        _ => {
            ix.coverage = Cov::Unsupported;
            return ix;
        }
    };
    ix.instruction_type = text(v.pointer("/parsed/type"), Src::ParsedInstruction);
    let info = v.pointer("/parsed/info");
    for &key in fields {
        let field = info.and_then(|i| i.get(key)).or_else(|| {
            if matches!(key, "amount" | "decimals") {
                info.and_then(|i| i.get("tokenAmount"))
                    .and_then(|i| i.get(key))
            } else {
                None
            }
        });
        let observed = if key == "newAuthority" && field == Some(&Value::Null) {
            Obs::known("null", Src::ParsedInstruction)
        } else if matches!(key, "amount" | "lamports" | "space" | "decimals") {
            number(field, Src::ParsedInstruction)
        } else {
            text(field, Src::ParsedInstruction)
        };
        if observed.coverage != Cov::Known {
            ix.coverage = observed.coverage;
        }
        ix.fields.insert(key.into(), observed);
    }
    ix
}
pub(super) fn collect(result: &Value, out: &mut NativeAccountObservations) {
    let outer = result
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array);
    if let Some(items) = outer {
        for (index, v) in items.iter().take(MAX_NATIVE_INSTRUCTIONS).enumerate() {
            out.instructions.push(instruction(v, index as u32, None));
        }
        if items.len() > MAX_NATIVE_INSTRUCTIONS {
            out.instructions_coverage = Cov::Truncated;
            out.note("outer_instructions_truncated");
        }
    } else {
        out.instructions_coverage = if result
            .pointer("/transaction/message/instructions")
            .is_none_or(Value::is_null)
        {
            Cov::Missing
        } else {
            Cov::Invalid
        };
        out.note("outer_instructions_unavailable");
    }
    match result.pointer("/meta/innerInstructions") {
        Some(Value::Array(groups)) => {
            let mut seen = HashSet::new();
            for group in groups.iter().take(MAX_NATIVE_INSTRUCTIONS) {
                let index = group["index"]
                    .as_u64()
                    .filter(|i| outer.is_some_and(|o| *i < (o.len() as u64)))
                    .and_then(|i| u32::try_from(i).ok());
                let Some(index) = index else {
                    out.instructions_coverage = Cov::Invalid;
                    out.note("inner_position_invalid");
                    continue;
                };
                if !seen.insert(index) {
                    out.instructions
                        .retain(|i| i.outer_index != index || i.inner_index.is_none());
                    out.instructions_coverage = Cov::Invalid;
                    out.note("inner_position_duplicate");
                    continue;
                }
                let Some(items) = group["instructions"].as_array() else {
                    out.instructions_coverage = Cov::Invalid;
                    out.note("inner_instructions_invalid");
                    continue;
                };
                for (offset, v) in items.iter().enumerate() {
                    if out.instructions.len() == MAX_NATIVE_INSTRUCTIONS {
                        out.instructions_coverage = Cov::Truncated;
                        out.note("instructions_truncated");
                        break;
                    }
                    out.instructions
                        .push(instruction(v, index, Some(offset as u32)));
                }
            }
            if groups.len() > MAX_NATIVE_INSTRUCTIONS {
                out.instructions_coverage = Cov::Truncated;
                out.note("inner_groups_truncated");
            }
        }
        None | Some(Value::Null) => {
            out.instructions_coverage = Cov::Missing;
            out.note("inner_instructions_not_recorded");
        }
        _ => {
            out.instructions_coverage = Cov::Invalid;
            out.note("inner_instructions_invalid");
        }
    }
    if out.instructions_coverage == Cov::Known {
        if let Some(i) = out.instructions.iter().find(|i| i.coverage != Cov::Known) {
            out.instructions_coverage = i.coverage;
            out.note("parsed_instruction_partial_or_unsupported");
        }
    }
    // Outer/inner indices are RPC positions; missing stack height is preserved.
    // No interleaved CPI order or intermediate account state is inferred.
}
pub(super) fn linked_accounts(out: &NativeAccountObservations) -> HashSet<String> {
    let mut links = HashSet::new();
    for i in &out.instructions {
        if i.instruction_type.value.is_none() {
            continue;
        }
        if !i
            .fields
            .values()
            .any(|v| v.value.as_deref() == Some(&out.wallet_pubkey))
        {
            continue;
        }
        let fields: &[&str] = match i.program_id.value.as_deref() {
            Some(TOKEN | TOKEN_2022) => match i.instruction_type.value.as_deref() {
                Some("transfer" | "transferChecked") => &["source", "destination"],
                // Close destination receives native lamports; that role does not
                // identify a token account. Initialize/sync/authority target account.
                _ => &["account"],
            },
            Some(ATA) => &["account"],
            Some(SYSTEM)
                if i.fields
                    .get("owner")
                    .and_then(|v| v.value.as_deref())
                    .is_some_and(|p| matches!(p, TOKEN | TOKEN_2022)) =>
            {
                &["newAccount"]
            }
            _ => &[],
        };
        for field in fields {
            if let Some(key) = i.fields.get(*field).and_then(|v| v.value.clone()) {
                links.insert(key);
            }
        }
    }
    links
}

/// These native-transfer endpoints still require token identity from receipt
/// rows. sourceBase is a wallet link, not an inferred token owner or endpoint.
pub(super) fn funding_accounts(out: &NativeAccountObservations) -> HashSet<String> {
    let mut links = HashSet::new();
    for i in &out.instructions {
        if i.program_id.value.as_deref() != Some(SYSTEM) {
            continue;
        }
        let roles: &[&str] = match i.instruction_type.value.as_deref() {
            Some("transfer") => &["source", "destination"],
            Some("transferWithSeed") => &["source", "destination", "sourceBase"],
            _ => continue,
        };
        let field = |key: &str| i.fields.get(key).and_then(|v| v.value.as_deref());
        if roles
            .iter()
            .any(|key| field(key) == Some(&out.wallet_pubkey))
        {
            for key in ["source", "destination"] {
                if let Some(account) = field(key).filter(|a| *a != out.wallet_pubkey) {
                    links.insert(account.to_owned());
                }
            }
        }
    }
    links
}
