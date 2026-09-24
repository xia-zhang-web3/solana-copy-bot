//! Strict, bounded /swap-instructions schema. Input operand flags/order are retained.
//! Lookup metadata is retained/validated; legacy compilation never resolves an ALT.
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta, SolanaInstruction};
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(crate) const MAX_RESPONSE_BYTES: usize = 128 * 1024;
const MAX_INSTRUCTIONS: usize = 64;
const MAX_OPERANDS: usize = 1232;
const MAX_DATA: usize = 1232;

#[derive(Debug)]
pub(crate) struct InstructionBundle {
    instructions: Vec<SolanaInstruction>,
    blockhash: PubkeyBytes,
    // These groups are not instructions. Keep their validated content, even though
    // legacy compilation already has every explicit account and needs no ALT reads.
    _lookup_tables: Vec<PubkeyBytes>,
    _lookup_addresses: Option<BTreeMap<PubkeyBytes, Vec<PubkeyBytes>>>,
}
impl InstructionBundle {
    pub(crate) fn parse(value: &Value, wallet: PubkeyBytes) -> Result<Self> {
        let object = object(value)?;
        known_fields(
            object,
            &[
                "tokenLedgerInstruction",
                "computeBudgetInstructions",
                "setupInstructions",
                "swapInstruction",
                "cleanupInstruction",
                "otherInstructions",
                "addressLookupTableAddresses",
                "addressesByLookupTableAddress",
                "blockhashWithMetadata",
                "prioritizationFeeLamports",
                "computeUnitLimit",
                "prioritizationType",
                "simulationSlot",
                "dynamicSlippageReport",
                "simulationError",
                "timeTaken",
                "createAtaTimeTaken",
                "loadedAccountsDataSize",
                "loadedAccountsDataSizeLimit",
                "transactionVersion",
                "error",
            ],
        )?;
        // Reporting metadata never selects instructions, fees or the locally compiled wire.
        for field in [
            "loadedAccountsDataSize",
            "loadedAccountsDataSizeLimit",
            "transactionVersion",
        ] {
            if let Some(value) = object.get(field) {
                ensure!(
                    value.is_null() || value.as_u64().is_some(),
                    "instruction_bundle_metadata_type:{field}"
                );
            }
        }
        let mut budget = ParseBudget::default();
        let ledger = optional_instruction(
            required(object, "tokenLedgerInstruction")?,
            wallet,
            &mut budget,
        )?;
        let compute = instructions(
            required(object, "computeBudgetInstructions")?,
            wallet,
            &mut budget,
        )?;
        let setup = instructions(required(object, "setupInstructions")?, wallet, &mut budget)?;
        let swap = instruction(required(object, "swapInstruction")?, wallet, &mut budget)?;
        let cleanup =
            optional_instruction(required(object, "cleanupInstruction")?, wallet, &mut budget)?;
        let other = instructions(required(object, "otherInstructions")?, wallet, &mut budget)?;
        ensure!(
            ledger.is_none() && other.is_empty(),
            "instruction_bundle_unsupported_instruction_order"
        );
        let lookup_tables = pubkeys(required(object, "addressLookupTableAddresses")?)?;
        let lookup_addresses = match object.get("addressesByLookupTableAddress") {
            None => None, // optional lookup expansion; explicit keys are already present
            Some(Value::Null) => None,
            Some(value) => {
                let entries = self::object(value)?;
                ensure!(entries.len() <= 32, "instruction_bundle_lookup_limit");
                let mut addresses = BTreeMap::new();
                let mut total = 0;
                for (key, values) in entries {
                    let key = pubkey(key)?;
                    ensure!(
                        lookup_tables.contains(&key),
                        "instruction_bundle_lookup_binding"
                    );
                    let keys = pubkeys(values)?;
                    total += keys.len();
                    ensure!(total <= 256, "instruction_bundle_lookup_limit");
                    addresses.insert(key, keys);
                }
                Some(addresses)
            }
        };
        let blockhash = parse_blockhash(required(object, "blockhashWithMetadata")?)?;
        let instructions = compute
            .into_iter()
            .chain(setup)
            .chain([swap])
            .chain(cleanup)
            .collect();
        Ok(Self {
            instructions,
            blockhash,
            _lookup_tables: lookup_tables,
            _lookup_addresses: lookup_addresses,
        })
    }
    pub(crate) fn instructions(&self) -> &[SolanaInstruction] {
        &self.instructions
    }
    pub(crate) fn blockhash(&self) -> PubkeyBytes {
        self.blockhash
    }
}

#[derive(Default)]
struct ParseBudget {
    instructions: usize,
    operands: usize,
    data: usize,
}
fn instruction(
    value: &Value,
    wallet: PubkeyBytes,
    budget: &mut ParseBudget,
) -> Result<SolanaInstruction> {
    budget.instructions += 1;
    ensure!(
        budget.instructions <= MAX_INSTRUCTIONS,
        "instruction_bundle_instruction_limit"
    );
    let value = object(value)?;
    known_fields(value, &["programId", "accounts", "data"])?;
    let program_id = pubkey_value(required(value, "programId")?)?;
    let metas = array(required(value, "accounts")?, 256)?;
    budget.operands += metas.len();
    ensure!(
        budget.operands <= MAX_OPERANDS,
        "instruction_bundle_operand_limit"
    );
    let accounts = metas
        .iter()
        .map(|meta| {
            let meta = object(meta)?;
            known_fields(meta, &["pubkey", "isSigner", "isWritable"])?;
            let pubkey = pubkey_value(required(meta, "pubkey")?)?;
            let is_signer = required(meta, "isSigner")?
                .as_bool()
                .context("instruction_bundle_flag_type")?;
            let is_writable = required(meta, "isWritable")?
                .as_bool()
                .context("instruction_bundle_flag_type")?;
            ensure!(
                !is_signer || pubkey == wallet,
                "instruction_bundle_extra_signer"
            );
            Ok(SolanaAccountMeta {
                pubkey,
                is_signer,
                is_writable,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let encoded = required(value, "data")?
        .as_str()
        .context("instruction_bundle_data_type")?;
    ensure!(encoded.len() <= 1644, "instruction_bundle_data_limit");
    let data = STANDARD
        .decode(encoded)
        .map_err(|_| anyhow::anyhow!("instruction_bundle_data_encoding"))?;
    budget.data += data.len();
    ensure!(budget.data <= MAX_DATA, "instruction_bundle_data_limit");
    Ok(SolanaInstruction {
        program_id,
        accounts,
        data,
    })
}
fn optional_instruction(
    value: &Value,
    wallet: PubkeyBytes,
    budget: &mut ParseBudget,
) -> Result<Option<SolanaInstruction>> {
    if value.is_null() {
        Ok(None)
    } else {
        instruction(value, wallet, budget).map(Some)
    }
}
fn instructions(
    value: &Value,
    wallet: PubkeyBytes,
    budget: &mut ParseBudget,
) -> Result<Vec<SolanaInstruction>> {
    array(value, MAX_INSTRUCTIONS)?
        .iter()
        .map(|v| instruction(v, wallet, budget))
        .collect()
}
fn parse_blockhash(value: &Value) -> Result<PubkeyBytes> {
    let value = object(value)?;
    known_fields(value, &["blockhash", "lastValidBlockHeight", "fetchedAt"])?;
    let bytes = array(required(value, "blockhash")?, 32)?;
    ensure!(bytes.len() == 32, "instruction_bundle_blockhash_length");
    let mut blockhash = [0; 32];
    for (out, value) in blockhash.iter_mut().zip(bytes) {
        *out = u8::try_from(
            value
                .as_u64()
                .context("instruction_bundle_blockhash_byte")?,
        )
        .context("instruction_bundle_blockhash_byte")?;
    }
    required(value, "lastValidBlockHeight")?
        .as_u64()
        .context("instruction_bundle_blockheight_type")?;
    let fetched = object(required(value, "fetchedAt")?)?;
    known_fields(fetched, &["secs_since_epoch", "nanos_since_epoch"])?;
    required(fetched, "secs_since_epoch")?
        .as_u64()
        .context("instruction_bundle_fetched_at_type")?;
    ensure!(
        required(fetched, "nanos_since_epoch")?
            .as_u64()
            .is_some_and(|n| n < 1_000_000_000),
        "instruction_bundle_fetched_at_type"
    );
    Ok(blockhash)
}
pub(crate) fn pubkey(value: &str) -> Result<PubkeyBytes> {
    ensure!(
        (32..=44).contains(&value.len()),
        "instruction_bundle_pubkey_length"
    );
    let mut output = [0; 32];
    let len = bs58::decode(value)
        .onto(&mut output)
        .map_err(|_| anyhow::anyhow!("instruction_bundle_pubkey_encoding"))?;
    ensure!(len == 32, "instruction_bundle_pubkey_length");
    Ok(output)
}
fn pubkey_value(value: &Value) -> Result<PubkeyBytes> {
    pubkey(value.as_str().context("instruction_bundle_pubkey_type")?)
}
fn pubkeys(value: &Value) -> Result<Vec<PubkeyBytes>> {
    array(value, 256)?.iter().map(pubkey_value).collect()
}
fn object(value: &Value) -> Result<&Map<String, Value>> {
    value.as_object().context("instruction_bundle_object_type")
}
fn array(value: &Value, max: usize) -> Result<&Vec<Value>> {
    let values = value.as_array().context("instruction_bundle_array_type")?;
    ensure!(values.len() <= max, "instruction_bundle_array_limit");
    Ok(values)
}
fn required<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value> {
    object
        .get(field)
        .with_context(|| format!("instruction_bundle_required_field:{field}"))
}
fn known_fields(object: &Map<String, Value>, fields: &[&str]) -> Result<()> {
    // Do not silently discard newly introduced instruction-bearing groups or fields.
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        bail!("instruction_bundle_unknown_field");
    }
    Ok(())
}
