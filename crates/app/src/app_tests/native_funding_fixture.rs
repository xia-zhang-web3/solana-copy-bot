use super::execution_pump_fun_direct_builder_contract::{
    pumpswap_global_config_data, pumpswap_pool_data,
};
use crate::execution_pumpswap_accounts::*;
use crate::execution_pumpswap_direct_instructions::*;
use crate::execution_solana_tx::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

pub(super) const WALLET: PubkeyBytes = [11; 32];
pub(super) const PEER: PubkeyBytes = [52; 32];
pub(super) const TOKEN: &str = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";

pub(super) fn payload(instructions: &[SolanaInstruction]) -> Result<String> {
    Ok(STANDARD.encode(serialize_unsigned_legacy_transaction(
        WALLET,
        [9; 32],
        instructions,
    )?))
}
pub(super) fn budget() -> Vec<SolanaInstruction> {
    super::priority_fee_fixture::budget(200_000, 600_000)
}
pub(super) fn transfer(from: PubkeyBytes, to: PubkeyBytes, amount: u64) -> SolanaInstruction {
    SolanaInstruction {
        program_id: system_program_id(),
        accounts: vec![
            SolanaAccountMeta::signer_writable(from),
            SolanaAccountMeta::writable(to),
        ],
        data: [2_u32.to_le_bytes().to_vec(), amount.to_le_bytes().to_vec()].concat(),
    }
}
pub(super) fn direct(buy: bool, extension: bool, amount: u64) -> Result<Vec<SolanaInstruction>> {
    let pool = decode_pool_account(&pumpswap_pool_data(TOKEN))?;
    let input = PumpSwapInstructionInputs {
        user: WALLET,
        pool_key: [42; 32],
        pool_account_len: if extension {
            200
        } else {
            PUMPSWAP_POOL_ACCOUNT_NEW_SIZE
        },
        global_config: decode_global_config_account(&pumpswap_global_config_data())?,
        base_token_program: token_program_id(),
        quote_token_program: token_program_id(),
        user_base_ata: associated_token_address(&WALLET, &pool.base_mint, &token_program_id()),
        user_quote_ata: associated_token_address(&WALLET, &pool.quote_mint, &token_program_id()),
        pool,
        amount_in: amount,
        min_output: 123_456,
        priority_fee_micro_lamports_per_cu: 10_000,
    };
    Ok(if buy {
        build_buy_with_sol_instructions(input)
    } else {
        build_sell_for_sol_instructions(input)
    })
}

pub(super) fn version_zero(payload: &str) -> Result<String> {
    let mut bytes = STANDARD.decode(payload)?;
    assert_eq!(bytes[0], 1);
    bytes.insert(65, 0x80);
    bytes.push(0);
    Ok(STANDARD.encode(bytes))
}

// The serializer intentionally supports one signer only. Promote its second writable key
// for synthetic multi-signer wire cases; these zero signatures are never cryptographic proof.
pub(super) fn foreign_signer_transfer(to: PubkeyBytes, amount: u64) -> Result<String> {
    let mut instructions = budget();
    let mut instruction = transfer(PEER, to, amount);
    instruction.accounts[0].is_signer = false;
    instructions.push(instruction);
    let mut bytes = STANDARD.decode(payload(&instructions)?)?;
    assert_eq!(&bytes[101..133], &PEER);
    bytes[65] = 2;
    bytes[0] = 2;
    bytes.splice(1..1, [0; 64]);
    Ok(STANDARD.encode(bytes))
}
