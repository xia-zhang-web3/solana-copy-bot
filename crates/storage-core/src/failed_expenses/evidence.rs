use serde_json::Value;

/// Bounded supported subset of Solana TransactionError/InstructionError JSON.
/// Arbitrary provider text cannot establish an on-chain failure.
pub fn proven_failure(error: &Value) -> bool {
    if let Some(name) = error.as_str() {
        return matches!(
            name,
            "AccountInUse"
                | "AccountLoadedTwice"
                | "AccountNotFound"
                | "ProgramAccountNotFound"
                | "InsufficientFundsForFee"
                | "InvalidAccountForFee"
                | "AlreadyProcessed"
                | "BlockhashNotFound"
                | "SignatureFailure"
                | "InvalidProgramForExecution"
                | "SanitizeFailure"
                | "InvalidAccountIndex"
                | "CallChainTooDeep"
                | "MissingSignatureForFee"
                | "InvalidRentPayingAccount"
                | "WouldExceedMaxBlockCostLimit"
                | "UnsupportedVersion"
                | "InvalidWritableAccount"
                | "WouldExceedMaxAccountCostLimit"
                | "WouldExceedAccountDataBlockLimit"
                | "TooManyAccountLocks"
                | "AddressLookupTableNotFound"
                | "InvalidAddressLookupTableOwner"
                | "InvalidAddressLookupTableData"
                | "InvalidAddressLookupTableIndex"
                | "InvalidLoadedAccountsDataSizeLimit"
                | "MaxLoadedAccountsDataSizeExceeded"
                | "ResanitizationNeeded"
                | "UnbalancedTransaction"
                | "ProgramCacheHitMaxLimit"
                | "CommitCancelled"
        );
    }
    let Some(object) = error.as_object().filter(|v| v.len() == 1) else {
        return false;
    };
    let Some(parts) = object
        .get("InstructionError")
        .and_then(Value::as_array)
        .filter(|v| v.len() == 2 && v[0].as_u64().is_some_and(|i| u8::try_from(i).is_ok()))
    else {
        return false;
    };
    if let Some(name) = parts[1].as_str() {
        return matches!(
            name,
            "GenericError"
                | "InvalidArgument"
                | "InvalidInstructionData"
                | "InvalidAccountData"
                | "AccountDataTooSmall"
                | "InsufficientFunds"
                | "IncorrectProgramId"
                | "MissingRequiredSignature"
                | "AccountAlreadyInitialized"
                | "UninitializedAccount"
                | "NotEnoughAccountKeys"
                | "AccountBorrowFailed"
                | "AccountBorrowOutstanding"
                | "ComputationalBudgetExceeded"
                | "ProgramFailedToComplete"
                | "ProgramFailedToCompile"
                | "Immutable"
                | "IncorrectAuthority"
                | "ArithmeticOverflow"
                | "InvalidAccountOwner"
                | "UnsupportedSysvar"
                | "IllegalOwner"
                | "InvalidRealloc"
                | "MaxAccountsDataAllocationsExceeded"
                | "MaxAccountsExceeded"
                | "MaxInstructionTraceLengthExceeded"
                | "ReadonlyLamportChange"
                | "ReadonlyDataModified"
                | "ExternalAccountLamportSpend"
                | "ExternalAccountDataModified"
                | "UnbalancedInstruction"
                | "ModifiedProgramId"
                | "AccountNotExecutable"
                | "ExecutableModified"
                | "RentEpochModified"
                | "DuplicateAccountOutOfSync"
                | "InvalidError"
                | "CallDepth"
                | "MissingAccount"
                | "ReentrancyNotAllowed"
                | "PrivilegeEscalation"
        );
    }
    parts[1]
        .as_object()
        .filter(|v| v.len() == 1)
        .and_then(|v| v.get("Custom"))
        .and_then(Value::as_u64)
        .is_some_and(|code| u32::try_from(code).is_ok())
}
