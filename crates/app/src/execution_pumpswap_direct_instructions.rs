use crate::execution_pumpswap_accounts::{
    associated_token_address, associated_token_program_id, coin_creator_vault_authority_pda,
    compute_budget_program_id, event_authority_pda, global_config_pda,
    global_volume_accumulator_pda, pool_v2_pda, pump_amm_fee_config_pda, pump_amm_program_id,
    pump_fee_program_id, system_program_id, token_program_id, user_volume_accumulator_pda,
    PumpSwapGlobalConfig, PumpSwapPool,
};
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta, SolanaInstruction};

pub(crate) const PUMPSWAP_COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
pub(crate) const PUMPSWAP_POOL_ACCOUNT_NEW_SIZE: usize = 300;

const TOKEN_SYNC_NATIVE: u8 = 17;
const TOKEN_CLOSE_ACCOUNT: u8 = 9;
const ASSOCIATED_TOKEN_CREATE_IDEMPOTENT: u8 = 1;
const PUMPSWAP_SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];
const PUMPSWAP_EXTEND_ACCOUNT_DISCRIMINATOR: [u8; 8] = [234, 102, 194, 203, 150, 72, 62, 229];

pub(crate) struct PumpSwapInstructionInputs {
    pub(crate) user: PubkeyBytes,
    pub(crate) pool_key: PubkeyBytes,
    pub(crate) pool_account_len: usize,
    pub(crate) pool: PumpSwapPool,
    pub(crate) global_config: PumpSwapGlobalConfig,
    pub(crate) base_token_program: PubkeyBytes,
    pub(crate) quote_token_program: PubkeyBytes,
    pub(crate) user_base_ata: PubkeyBytes,
    pub(crate) user_quote_ata: PubkeyBytes,
    pub(crate) amount_in: u64,
    pub(crate) min_output: u64,
    pub(crate) priority_fee_lamports: u64,
}

pub(crate) fn build_buy_with_sol_instructions(
    input: PumpSwapInstructionInputs,
) -> Vec<SolanaInstruction> {
    let mut instructions = shared_prefix_instructions(&input);
    instructions.push(create_ata_idempotent_instruction(
        &input.user,
        &input.user_base_ata,
        &input.user,
        &input.pool.base_mint,
        &input.base_token_program,
    ));
    instructions.push(system_transfer_instruction(
        &input.user,
        &input.user_base_ata,
        input.amount_in,
    ));
    instructions.push(sync_native_instruction(&input.user_base_ata));
    instructions.push(create_ata_idempotent_instruction(
        &input.user,
        &input.user_quote_ata,
        &input.user,
        &input.pool.quote_mint,
        &input.quote_token_program,
    ));
    instructions.push(pumpswap_sell_instruction(&input));
    instructions.push(close_wsol_account_instruction(
        &input.user_base_ata,
        &input.user,
    ));
    instructions
}

pub(crate) fn build_sell_for_sol_instructions(
    input: PumpSwapInstructionInputs,
) -> Vec<SolanaInstruction> {
    let mut instructions = shared_prefix_instructions(&input);
    instructions.push(create_ata_idempotent_instruction(
        &input.user,
        &input.user_base_ata,
        &input.user,
        &input.pool.base_mint,
        &input.base_token_program,
    ));
    instructions.push(pumpswap_buy_exact_quote_in_instruction(&input));
    instructions.push(close_wsol_account_instruction(
        &input.user_base_ata,
        &input.user,
    ));
    instructions
}

fn shared_prefix_instructions(input: &PumpSwapInstructionInputs) -> Vec<SolanaInstruction> {
    let mut instructions = Vec::new();
    instructions.push(compute_unit_limit_instruction(PUMPSWAP_COMPUTE_UNIT_LIMIT));
    instructions.push(compute_unit_price_instruction(priority_micro_lamports(
        input.priority_fee_lamports,
    )));
    if input.pool_account_len < PUMPSWAP_POOL_ACCOUNT_NEW_SIZE {
        instructions.push(extend_pool_account_instruction(
            &input.user,
            &input.pool_key,
        ));
    }
    instructions
}

fn priority_micro_lamports(priority_fee_lamports: u64) -> u64 {
    if priority_fee_lamports == 0 {
        return 0;
    }
    let numerator = u128::from(priority_fee_lamports) * 1_000_000_u128;
    (numerator / u128::from(PUMPSWAP_COMPUTE_UNIT_LIMIT)).min(u128::from(u64::MAX)) as u64
}

fn compute_unit_limit_instruction(units: u32) -> SolanaInstruction {
    let mut data = vec![2];
    data.extend_from_slice(&units.to_le_bytes());
    SolanaInstruction {
        program_id: compute_budget_program_id(),
        accounts: vec![],
        data,
    }
}

fn compute_unit_price_instruction(micro_lamports: u64) -> SolanaInstruction {
    let mut data = vec![3];
    data.extend_from_slice(&micro_lamports.to_le_bytes());
    SolanaInstruction {
        program_id: compute_budget_program_id(),
        accounts: vec![],
        data,
    }
}

fn extend_pool_account_instruction(
    user: &PubkeyBytes,
    pool_key: &PubkeyBytes,
) -> SolanaInstruction {
    SolanaInstruction {
        program_id: pump_amm_program_id(),
        accounts: vec![
            writable(pool_key),
            signer_writable(user),
            readonly(&system_program_id()),
            readonly(&event_authority_pda()),
            readonly(&pump_amm_program_id()),
        ],
        data: PUMPSWAP_EXTEND_ACCOUNT_DISCRIMINATOR.to_vec(),
    }
}

fn create_ata_idempotent_instruction(
    payer: &PubkeyBytes,
    ata: &PubkeyBytes,
    owner: &PubkeyBytes,
    mint: &PubkeyBytes,
    token_program: &PubkeyBytes,
) -> SolanaInstruction {
    SolanaInstruction {
        program_id: associated_token_program_id(),
        accounts: vec![
            signer_writable(payer),
            writable(ata),
            readonly(owner),
            readonly(mint),
            readonly(&system_program_id()),
            readonly(token_program),
        ],
        data: vec![ASSOCIATED_TOKEN_CREATE_IDEMPOTENT],
    }
}

fn system_transfer_instruction(
    from: &PubkeyBytes,
    to: &PubkeyBytes,
    lamports: u64,
) -> SolanaInstruction {
    let mut data = Vec::with_capacity(12);
    data.extend_from_slice(&2_u32.to_le_bytes());
    data.extend_from_slice(&lamports.to_le_bytes());
    SolanaInstruction {
        program_id: system_program_id(),
        accounts: vec![signer_writable(from), writable(to)],
        data,
    }
}

fn sync_native_instruction(account: &PubkeyBytes) -> SolanaInstruction {
    SolanaInstruction {
        program_id: token_program_id(),
        accounts: vec![writable(account)],
        data: vec![TOKEN_SYNC_NATIVE],
    }
}

fn close_wsol_account_instruction(account: &PubkeyBytes, owner: &PubkeyBytes) -> SolanaInstruction {
    SolanaInstruction {
        program_id: token_program_id(),
        accounts: vec![writable(account), writable(owner), signer_writable(owner)],
        data: vec![TOKEN_CLOSE_ACCOUNT],
    }
}

fn pumpswap_sell_instruction(input: &PumpSwapInstructionInputs) -> SolanaInstruction {
    let mut data = PUMPSWAP_SELL_DISCRIMINATOR.to_vec();
    data.extend_from_slice(&input.amount_in.to_le_bytes());
    data.extend_from_slice(&input.min_output.to_le_bytes());
    SolanaInstruction {
        program_id: pump_amm_program_id(),
        accounts: sell_accounts(input),
        data,
    }
}

fn pumpswap_buy_exact_quote_in_instruction(input: &PumpSwapInstructionInputs) -> SolanaInstruction {
    let mut data = PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR.to_vec();
    data.extend_from_slice(&input.amount_in.to_le_bytes());
    data.extend_from_slice(&input.min_output.to_le_bytes());
    data.push(1);
    SolanaInstruction {
        program_id: pump_amm_program_id(),
        accounts: buy_accounts(input),
        data,
    }
}

fn common_swap_accounts(input: &PumpSwapInstructionInputs) -> Vec<SolanaAccountMeta> {
    let protocol_fee_recipient = input.global_config.fee_recipient(input.pool.is_mayhem_mode);
    let protocol_fee_recipient_ata = associated_token_address(
        &protocol_fee_recipient,
        &input.pool.quote_mint,
        &input.quote_token_program,
    );
    let coin_creator_vault_authority = coin_creator_vault_authority_pda(&input.pool.coin_creator);
    let coin_creator_vault_ata = associated_token_address(
        &coin_creator_vault_authority,
        &input.pool.quote_mint,
        &input.quote_token_program,
    );
    vec![
        writable(&input.pool_key),
        signer_writable(&input.user),
        readonly(&global_config_pda()),
        readonly(&input.pool.base_mint),
        readonly(&input.pool.quote_mint),
        writable(&input.user_base_ata),
        writable(&input.user_quote_ata),
        writable(&input.pool.pool_base_token_account),
        writable(&input.pool.pool_quote_token_account),
        readonly(&protocol_fee_recipient),
        writable(&protocol_fee_recipient_ata),
        readonly(&input.base_token_program),
        readonly(&input.quote_token_program),
        readonly(&system_program_id()),
        readonly(&associated_token_program_id()),
        readonly(&event_authority_pda()),
        readonly(&pump_amm_program_id()),
        writable(&coin_creator_vault_ata),
        readonly(&coin_creator_vault_authority),
    ]
}

fn buy_accounts(input: &PumpSwapInstructionInputs) -> Vec<SolanaAccountMeta> {
    let mut accounts = common_swap_accounts(input);
    accounts.push(readonly(&global_volume_accumulator_pda()));
    accounts.push(writable(&user_volume_accumulator_pda(&input.user)));
    accounts.push(readonly(&pump_amm_fee_config_pda()));
    accounts.push(readonly(&pump_fee_program_id()));
    push_buy_remaining_accounts(input, &mut accounts);
    accounts
}

fn sell_accounts(input: &PumpSwapInstructionInputs) -> Vec<SolanaAccountMeta> {
    let mut accounts = common_swap_accounts(input);
    accounts.push(readonly(&pump_amm_fee_config_pda()));
    accounts.push(readonly(&pump_fee_program_id()));
    if input.pool.is_cashback_coin {
        let user_volume = user_volume_accumulator_pda(&input.user);
        let user_volume_ata = associated_token_address(
            &user_volume,
            &input.pool.quote_mint,
            &input.quote_token_program,
        );
        accounts.push(writable(&user_volume_ata));
        accounts.push(writable(&user_volume));
    }
    push_common_remaining_accounts(input, &mut accounts);
    accounts
}

fn push_buy_remaining_accounts(
    input: &PumpSwapInstructionInputs,
    accounts: &mut Vec<SolanaAccountMeta>,
) {
    if input.pool.is_cashback_coin {
        let user_volume = user_volume_accumulator_pda(&input.user);
        accounts.push(writable(&associated_token_address(
            &user_volume,
            &input.pool.quote_mint,
            &input.quote_token_program,
        )));
    }
    push_common_remaining_accounts(input, accounts);
}

fn push_common_remaining_accounts(
    input: &PumpSwapInstructionInputs,
    accounts: &mut Vec<SolanaAccountMeta>,
) {
    if input.pool.coin_creator != [0_u8; 32] {
        accounts.push(readonly(&pool_v2_pda(&input.pool.base_mint)));
    }
    let buyback_fee_recipient = input.global_config.buyback_fee_recipient();
    let buyback_fee_recipient_ata = associated_token_address(
        &buyback_fee_recipient,
        &input.pool.quote_mint,
        &input.quote_token_program,
    );
    accounts.push(readonly(&buyback_fee_recipient));
    accounts.push(writable(&buyback_fee_recipient_ata));
}

fn readonly(pubkey: &PubkeyBytes) -> SolanaAccountMeta {
    SolanaAccountMeta::readonly(*pubkey)
}

fn writable(pubkey: &PubkeyBytes) -> SolanaAccountMeta {
    SolanaAccountMeta::writable(*pubkey)
}

fn signer_writable(pubkey: &PubkeyBytes) -> SolanaAccountMeta {
    SolanaAccountMeta::signer_writable(*pubkey)
}
