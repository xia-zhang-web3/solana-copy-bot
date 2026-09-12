//! Exact integer BUY input operands. No metadata float, balance or fee estimates.
use crate::execution_pumpswap_accounts::{parse_pubkey, pump_amm_program_id, system_program_id};
use crate::execution_transaction_wire::DecodedMessage;
use anyhow::{ensure, Result};

pub(super) fn buy(message: &DecodedMessage, wallet: &str) -> Result<u64> {
    let wallet = parse_pubkey(wallet, "tiny_budget_wallet")?;
    let mut transfers = 0_u64;
    let mut swaps = 0_u64;
    let mut swap_count = 0;
    let allowed = [
        "ComputeBudget111111111111111111111111111111",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
    ]
    .into_iter()
    .map(|key| parse_pubkey(key, "tiny_budget_program"))
    .collect::<Result<Vec<_>>>()?;
    for ix in &message.instructions {
        ensure!(
            allowed.contains(&ix.program.pubkey)
                || ix.program.pubkey == system_program_id()
                || ix.program.pubkey == pump_amm_program_id(),
            "tiny_budget_buy_amount_unproven"
        );
        if ix.program.pubkey == pump_amm_program_id() {
            ensure!(
                (ix.data.len() == 8 && ix.data == [234, 102, 194, 203, 150, 72, 62, 229])
                    || (ix.data.len() == 24
                        && ix.data[..8] == [51, 230, 133, 164, 1, 127, 131, 173]),
                "tiny_budget_buy_amount_unproven"
            );
        }
        if ix.program.pubkey == system_program_id()
            && ix.data.len() == 12
            && ix.data[..4] == 2_u32.to_le_bytes()
            && ix.accounts.len() == 2
            && ix.accounts[0].pubkey == wallet
            && ix.accounts[1].pubkey != wallet
        {
            transfers = transfers
                .checked_add(u64::from_le_bytes(ix.data[4..12].try_into()?))
                .ok_or_else(|| anyhow::anyhow!("tiny_budget_buy_amount"))?;
        }
        if ix.program.pubkey == pump_amm_program_id()
            && ix.data.len() >= 24
            && ix.data[..8] == [51, 230, 133, 164, 1, 127, 131, 173]
        {
            let wsol = parse_pubkey(
                "So11111111111111111111111111111111111111112",
                "tiny_budget_wsol",
            )?;
            ensure!(
                ix.accounts.len() >= 6
                    && ix.accounts[1].pubkey == wallet
                    && ix.accounts[3].pubkey == wsol,
                "tiny_budget_buy_amount_binding"
            );
            swaps = u64::from_le_bytes(ix.data[8..16].try_into()?);
            swap_count += 1;
        }
    }
    // Current direct PumpSwap input is wrapped by an exact wallet transfer. Synthetic
    // transfer-only fixtures exercise the boundary without claiming DEX execution.
    ensure!(
        swap_count <= 1 && (swap_count == 0 || swaps == transfers),
        "tiny_budget_buy_amount_binding"
    );
    ensure!(
        (1..=copybot_storage_core::TINY_BUY_LAMPORTS).contains(&transfers),
        "tiny_budget_buy_amount"
    );
    Ok(transfers)
}
