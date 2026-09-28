//! Solana's fixed-int little-endian bincode enum layout, not JSON bytes.
//! Variant order follows the public stable ABI in anza-xyz/solana-sdk at
//! fb7dd15cdfd5a0b11dc576f44eb1533f9bc74211 (transaction-error and instruction-error).
//! Yellowstone at 76b14fdba707521ae771d54da212ceae47cc1a82 uses wincode;
//! that SDK's stable ABI explicitly tests equal bincode/wincode wire layouts.
//! Unknown names/payloads fail closed. Original JSON remains broker evidence.
use anyhow::{ensure, Context, Result};
use serde_json::Value;

const TRANSACTION: &[&str] = &[
    "AccountInUse",
    "AccountLoadedTwice",
    "AccountNotFound",
    "ProgramAccountNotFound",
    "InsufficientFundsForFee",
    "InvalidAccountForFee",
    "AlreadyProcessed",
    "BlockhashNotFound",
    "InstructionError",
    "CallChainTooDeep",
    "MissingSignatureForFee",
    "InvalidAccountIndex",
    "SignatureFailure",
    "InvalidProgramForExecution",
    "SanitizeFailure",
    "ClusterMaintenance",
    "AccountBorrowOutstanding",
    "WouldExceedMaxBlockCostLimit",
    "UnsupportedVersion",
    "InvalidWritableAccount",
    "WouldExceedMaxAccountCostLimit",
    "WouldExceedAccountDataBlockLimit",
    "TooManyAccountLocks",
    "AddressLookupTableNotFound",
    "InvalidAddressLookupTableOwner",
    "InvalidAddressLookupTableData",
    "InvalidAddressLookupTableIndex",
    "InvalidRentPayingAccount",
    "WouldExceedMaxVoteCostLimit",
    "WouldExceedAccountDataTotalLimit",
    "DuplicateInstruction",
    "InsufficientFundsForRent",
    "MaxLoadedAccountsDataSizeExceeded",
    "InvalidLoadedAccountsDataSizeLimit",
    "ResanitizationNeeded",
    "ProgramExecutionTemporarilyRestricted",
    "UnbalancedTransaction",
    "ProgramCacheHitMaxLimit",
    "CommitCancelled",
    "BailOut",
];
const INSTRUCTION: &[&str] = &[
    "GenericError",
    "InvalidArgument",
    "InvalidInstructionData",
    "InvalidAccountData",
    "AccountDataTooSmall",
    "InsufficientFunds",
    "IncorrectProgramId",
    "MissingRequiredSignature",
    "AccountAlreadyInitialized",
    "UninitializedAccount",
    "UnbalancedInstruction",
    "ModifiedProgramId",
    "ExternalAccountLamportSpend",
    "ExternalAccountDataModified",
    "ReadonlyLamportChange",
    "ReadonlyDataModified",
    "DuplicateAccountIndex",
    "ExecutableModified",
    "RentEpochModified",
    "NotEnoughAccountKeys",
    "AccountDataSizeChanged",
    "AccountNotExecutable",
    "AccountBorrowFailed",
    "AccountBorrowOutstanding",
    "DuplicateAccountOutOfSync",
    "Custom",
    "InvalidError",
    "ExecutableDataModified",
    "ExecutableLamportChange",
    "ExecutableAccountNotRentExempt",
    "UnsupportedProgramId",
    "CallDepth",
    "MissingAccount",
    "ReentrancyNotAllowed",
    "MaxSeedLengthExceeded",
    "InvalidSeeds",
    "InvalidRealloc",
    "ComputationalBudgetExceeded",
    "PrivilegeEscalation",
    "ProgramEnvironmentSetupFailure",
    "ProgramFailedToComplete",
    "ProgramFailedToCompile",
    "Immutable",
    "IncorrectAuthority",
    "BorshIoError",
    "AccountNotRentExempt",
    "InvalidAccountOwner",
    "ArithmeticOverflow",
    "UnsupportedSysvar",
    "IllegalOwner",
    "MaxAccountsDataAllocationsExceeded",
    "MaxAccountsExceeded",
    "MaxInstructionTraceLengthExceeded",
    "BuiltinProgramsMustConsumeComputeUnits",
    "BailOut",
];
fn variant<'a>(v: &'a Value, names: &[&str]) -> Result<(u32, Option<&'a Value>)> {
    let (name, payload) = match v {
        Value::String(name) => (name.as_str(), None),
        Value::Object(fields) => {
            ensure!(fields.len() == 1, "http_recovery_error_variant_shape");
            let (name, payload) = fields.iter().next().expect("one field");
            (name.as_str(), Some(payload))
        }
        _ => anyhow::bail!("http_recovery_error_variant_shape"),
    };
    let index = names
        .iter()
        .position(|known| *known == name)
        .with_context(|| format!("http_recovery_unknown_error_variant:{name}"))?;
    Ok((index as u32, payload))
}
fn byte(v: &Value) -> Result<u8> {
    Ok(u8::try_from(v.as_u64().context("http_recovery_error_u8")?)?)
}
fn instruction(v: &Value, out: &mut Vec<u8>) -> Result<()> {
    let (index, payload) = variant(v, INSTRUCTION)?;
    out.extend(index.to_le_bytes());
    match (index, payload) {
        (25, Some(value)) => out.extend(
            u32::try_from(value.as_u64().context("http_recovery_custom_error_u32")?)?.to_le_bytes(),
        ),
        // Legacy validators serialized a Borsh error string. Current ABI uses
        // a unit variant. Keep each actual representation, never equate them.
        (44, Some(value)) => {
            let text = value.as_str().context("http_recovery_borsh_error_string")?;
            ensure!(text.len() <= 65_536, "http_recovery_borsh_error_bound");
            out.extend((text.len() as u64).to_le_bytes());
            out.extend(text.as_bytes());
        }
        (25, None) => anyhow::bail!("http_recovery_custom_error_payload_missing"),
        (_, None) => {}
        _ => anyhow::bail!("http_recovery_unexpected_instruction_error_payload"),
    }
    Ok(())
}
pub(super) fn encode(v: &Value) -> Result<Vec<u8>> {
    let (index, payload) = variant(v, TRANSACTION)?;
    let mut out = index.to_le_bytes().to_vec();
    match (index, payload) {
        (8, Some(value)) => {
            let pair = value
                .as_array()
                .context("http_recovery_instruction_error_tuple")?;
            ensure!(pair.len() == 2, "http_recovery_instruction_error_tuple");
            out.push(byte(&pair[0])?);
            instruction(&pair[1], &mut out)?;
        }
        (30, Some(value)) => out.push(byte(value)?),
        (31 | 35, Some(value)) => {
            ensure!(
                value.as_object().is_some_and(|fields| fields.len() == 1),
                "http_recovery_account_error_shape"
            );
            out.push(byte(&value["account_index"])?);
        }
        (8 | 30 | 31 | 35, None) => {
            anyhow::bail!("http_recovery_transaction_error_payload_missing")
        }
        (_, None) => {}
        _ => anyhow::bail!("http_recovery_unexpected_transaction_error_payload"),
    }
    Ok(out)
}
