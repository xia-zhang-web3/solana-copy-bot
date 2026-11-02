//! Bind the compiled finalized JSON message used by AMM proof to verified wire.
use crate::execution_transaction_wire::DecodedMessage;
use anyhow::{ensure,Context,Result};
use serde_json::Value;
pub(crate) fn verify(value:&Value,wire:&DecodedMessage)->Result<()>{
    let m=&value["transaction"]["message"];
    ensure!(value["version"]=="legacy","real_source_compiled_version");
    let h=&m["header"];
    ensure!(h["numRequiredSignatures"].as_u64()==Some(wire.binding.required_signatures as u64)
        && h["numReadonlySignedAccounts"].as_u64()==Some(wire.binding.readonly_signed as u64)
        && h["numReadonlyUnsignedAccounts"].as_u64()==Some(wire.binding.readonly_unsigned as u64)
        && m["recentBlockhash"]==bs58::encode(wire.recent_blockhash).into_string(),"real_source_compiled_header");
    let keys=m["accountKeys"].as_array().context("real_source_compiled_keys")?;
    ensure!(keys.len()==wire.binding.accounts.len() && keys.iter().zip(&wire.binding.accounts)
        .all(|(v,k)|v.as_str()==Some(bs58::encode(k.pubkey).into_string().as_str())),"real_source_compiled_keys");
    let instructions=m["instructions"].as_array().context("real_source_compiled_instructions")?;
    ensure!(instructions.len()==wire.instructions.len(),"real_source_compiled_instruction_count");
    for (value,ix) in instructions.iter().zip(&wire.instructions) {
        let program=value["programIdIndex"].as_u64().context("real_source_compiled_program")? as usize;
        ensure!(keys.get(program).and_then(Value::as_str)==Some(bs58::encode(ix.program.pubkey).into_string().as_str()),"real_source_compiled_program");
        let accounts=value["accounts"].as_array().context("real_source_compiled_accounts")?;
        ensure!(accounts.len()==ix.accounts.len(),"real_source_compiled_accounts");
        for (value,a) in accounts.iter().zip(&ix.accounts) {
            let index=value.as_u64().context("real_source_compiled_account_index")? as usize;
            ensure!(keys.get(index).and_then(Value::as_str)==Some(bs58::encode(a.pubkey).into_string().as_str()),"real_source_compiled_account_binding");
        }
        ensure!(bs58::decode(value["data"].as_str().context("real_source_compiled_data")?).into_vec()?==ix.data,"real_source_compiled_data");
    }
    ensure!(m.get("addressTableLookups").is_none_or(|v|v.is_null() || v.as_array().is_some_and(Vec::is_empty)),"real_source_compiled_lookup");
    Ok(())
}
