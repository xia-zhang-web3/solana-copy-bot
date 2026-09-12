//! Normalize jsonParsed operands back to the same bounded wire grammar.
//! Reference: solana-transaction-status 2.1.15 parse_system.rs / parse_token.rs.
use super::super::{key, ATA, SYSTEM, TOKEN};
use serde_json::Value;

pub(super) fn decode(ix: &Value) -> Option<(Vec<String>, Vec<u8>)> {
    let program = ix.get("programId")?.as_str()?;
    let parsed = ix.get("parsed")?;
    let kind = parsed.get("type")?.as_str()?;
    let info = parsed.get("info")?.as_object()?;
    let address = |name: &str| key(info.get(name)?.as_str()?);
    let uint = |name: &str| info.get(name)?.as_u64();
    // Multisig variants are outside the first-signer subset.
    if info.contains_key("signers")
        || info.contains_key("multisigAuthority")
        || info.contains_key("multisigOwner")
    {
        return None;
    }
    let mut data = Vec::new();
    let accounts = match (program, kind) {
        (ATA, "createIdempotent") => {
            data.push(1);
            vec![
                address("source")?,
                address("account")?,
                address("wallet")?,
                address("mint")?,
                address("systemProgram")?,
                address("tokenProgram")?,
            ]
        }
        (SYSTEM, "createAccount") => {
            data.extend(0u32.to_le_bytes());
            data.extend(uint("lamports")?.to_le_bytes());
            data.extend(uint("space")?.to_le_bytes());
            data.extend(bs58::decode(address("owner")?).into_vec().ok()?);
            vec![address("source")?, address("newAccount")?]
        }
        (TOKEN, "getAccountDataSize") => {
            let extensions = info.get("extensionTypes")?.as_array()?;
            if extensions.len() != 1 || extensions[0].as_str()? != "immutableOwner" {
                return None;
            }
            data.extend([21, 7, 0]);
            vec![address("mint")?]
        }
        (TOKEN, "initializeImmutableOwner") => {
            data.push(22);
            vec![address("account")?]
        }
        (SYSTEM, "createAccountWithSeed") => {
            let base = address("base")?;
            let seed = info.get("seed")?.as_str()?;
            if seed.len() > 32 {
                return None;
            }
            data.extend(3u32.to_le_bytes());
            data.extend(bs58::decode(&base).into_vec().ok()?);
            data.extend((seed.len() as u64).to_le_bytes());
            data.extend(seed.as_bytes());
            data.extend(uint("lamports")?.to_le_bytes());
            data.extend(uint("space")?.to_le_bytes());
            data.extend(bs58::decode(address("owner")?).into_vec().ok()?);
            vec![address("source")?, address("newAccount")?, base]
        }
        (SYSTEM, "transfer") => {
            data.extend(2u32.to_le_bytes());
            data.extend(uint("lamports")?.to_le_bytes());
            vec![address("source")?, address("destination")?]
        }
        (TOKEN, "initializeAccount3") => {
            data.push(18);
            data.extend(bs58::decode(address("owner")?).into_vec().ok()?);
            vec![address("account")?, address("mint")?]
        }
        (TOKEN, "syncNative") => {
            data.push(17);
            vec![address("account")?]
        }
        (TOKEN, "closeAccount") => {
            data.push(9);
            vec![
                address("account")?,
                address("destination")?,
                address("owner")?,
            ]
        }
        (TOKEN, "transfer") => {
            let amount = info.get("amount")?.as_str()?;
            if amount.is_empty() || !amount.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            data.push(3);
            data.extend(amount.parse::<u64>().ok()?.to_le_bytes());
            vec![
                address("source")?,
                address("destination")?,
                address("authority")?,
            ]
        }
        _ => return None,
    };
    Some((accounts, data))
}
