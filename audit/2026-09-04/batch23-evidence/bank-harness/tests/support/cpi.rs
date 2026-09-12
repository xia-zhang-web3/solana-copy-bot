use solana_program_runtime::declare_process_instruction;
use solana_sdk::{instruction::InstructionError, pubkey::Pubkey, system_instruction};

pub const PROGRAM: Pubkey = Pubkey::new_from_array([91; 32]);

declare_process_instruction!(DebitBuiltin, 1, |invoke_context| {
    let context = &invoke_context.transaction_context;
    let instruction = context.get_current_instruction_context()?;
    let amount = u64::from_le_bytes(
        instruction
            .get_instruction_data()
            .try_into()
            .map_err(|_| InstructionError::InvalidInstructionData)?,
    );
    let from = *context.get_key_of_account_at_index(
        instruction.get_index_of_instruction_account_in_transaction(0)?,
    )?;
    let to = *context.get_key_of_account_at_index(
        instruction.get_index_of_instruction_account_in_transaction(1)?,
    )?;
    // Real System Program CPI. No direct lamport mutation or balance model here.
    invoke_context.native_invoke(system_instruction::transfer(&from, &to, amount).into(), &[])
});
