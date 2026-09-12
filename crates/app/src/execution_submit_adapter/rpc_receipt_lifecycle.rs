use anyhow::{ensure, Result};
use serde_json::Value;
use std::collections::HashSet;

const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// A successful initializeAccount{,2,3} proves an uninitialized account, even
/// when prefunded. A successful closeAccount plus zero final lamports proves
/// closure. Lamports or an ATA create instruction alone prove neither lifecycle.
/// Reinitialization, unavailable CPI metadata and unparsed lifecycle stay pending.
pub(super) fn validate_missing_row(
    result: &Value,
    index: usize,
    wallet: &str,
    mint: &str,
    program: Option<&str>,
    creation: bool,
) -> Result<()> {
    let reason = if creation {
        "receipt_token_creation_unproven"
    } else {
        "receipt_token_closure_unproven"
    };
    ensure!(matches!(program, Some(SPL_TOKEN | TOKEN_2022)), reason);
    let program = program.unwrap();
    // Account keys and native balances have already passed receipt validation.
    let keys = result["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap();
    let key = &keys[index];
    let account = key["pubkey"].as_str().unwrap();
    ensure!(key["writable"] == true, reason);
    ensure!(keys.iter().any(|k| k["pubkey"] == program), reason);
    let after = result["meta"]["postBalances"][index].as_u64().unwrap();
    if creation {
        ensure!(after > 0, reason);
    } else {
        ensure!(
            result["meta"]["preBalances"][index].as_u64().unwrap() > 0 && after == 0,
            reason
        );
    }
    let instructions = lifecycle_instructions(result, reason)?;
    let mut initialized = 0;
    let mut closed = 0;
    for instruction in instructions {
        if instruction["programId"] != program {
            continue;
        }
        // No raw decoder: a partially decoded instruction for this account could
        // hide a close/reinitialize cycle, so it cannot justify a missing row.
        if instruction.get("parsed").is_none() {
            let accounts = instruction["accounts"].as_array();
            ensure!(
                accounts.is_some_and(|a| a.iter().all(|v| v.as_str().is_some()
                    && v != account
                    && keys.iter().any(|k| k["pubkey"] == *v))),
                reason
            );
            continue;
        }
        let parsed = &instruction["parsed"];
        let info = &parsed["info"];
        ensure!(
            info.is_object() && parsed["type"].as_str().is_some(),
            reason
        );
        if info["account"] != account {
            continue;
        }
        match parsed["type"].as_str() {
            Some("initializeAccount" | "initializeAccount2" | "initializeAccount3") => {
                ensure!(info["mint"] == mint && info["owner"] == wallet, reason);
                ensure!(keys.iter().any(|k| k["pubkey"] == mint), reason);
                if parsed["type"] != "initializeAccount3" {
                    ensure!(
                        info["rentSysvar"] == "SysvarRent111111111111111111111111111111111"
                            && keys.iter().any(|k| k["pubkey"] == info["rentSysvar"]),
                        reason
                    );
                }
                initialized += 1;
            }
            Some("closeAccount") => {
                ensure!(info["owner"] == wallet, reason);
                ensure!(
                    keys.iter().any(|k| k["pubkey"] == info["destination"]
                        && k["pubkey"] != account
                        && k["writable"] == true),
                    reason
                );
                closed += 1;
            }
            _ => {}
        }
    }
    ensure!(
        if creation {
            initialized == 1 && closed == 0
        } else {
            closed == 1 && initialized == 0
        },
        reason
    );
    Ok(())
}

fn lifecycle_instructions<'a>(result: &'a Value, reason: &'static str) -> Result<Vec<&'a Value>> {
    let outer = result
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!(reason.to_owned()))?;
    // null/missing means CPI recording is unavailable, not an empty instruction list.
    let inner = result
        .pointer("/meta/innerInstructions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!(reason.to_owned()))?;
    let mut instructions = outer.iter().collect::<Vec<_>>();
    let mut indices = HashSet::new();
    for group in inner {
        let index = group["index"]
            .as_u64()
            .and_then(|v| usize::try_from(v).ok());
        ensure!(
            index.is_some_and(|v| v < outer.len() && indices.insert(v)),
            reason
        );
        let items = group["instructions"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!(reason.to_owned()))?;
        instructions.extend(items);
    }
    Ok(instructions)
}
