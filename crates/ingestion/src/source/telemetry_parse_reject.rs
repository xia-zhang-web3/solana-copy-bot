pub(in crate::source) fn classify_parse_reject_reason(error: &anyhow::Error) -> &'static str {
    let lowered = error.to_string().to_ascii_lowercase();
    if lowered.contains("missing slot") {
        return "missing_slot";
    }
    if lowered.contains("missing status") {
        return "missing_status";
    }
    if lowered.contains("missing signer") {
        return "missing_signer";
    }
    if lowered.contains("missing program ids") {
        return "missing_program_ids";
    }
    if lowered.contains("missing transaction signature") {
        return "missing_signature";
    }
    if lowered.contains("timestamp") {
        return "invalid_timestamp";
    }
    if lowered.contains("balance") {
        return "invalid_balance_inference";
    }
    if lowered.contains("account key") {
        return "invalid_account_keys";
    }
    "other"
}
