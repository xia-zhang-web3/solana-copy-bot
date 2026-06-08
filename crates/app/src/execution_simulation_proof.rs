pub(crate) fn combined_simulation_proof(
    instructions_proof: Option<String>,
    transaction_proof: Option<String>,
) -> Option<String> {
    match (instructions_proof, transaction_proof) {
        (Some(instructions), Some(transaction)) => Some(format!("{instructions}; {transaction}")),
        (Some(instructions), None) => Some(instructions),
        (None, Some(transaction)) => Some(transaction),
        (None, None) => None,
    }
}
