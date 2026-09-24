//! IDL-derived Raydium route V1 fixture. Synthetic keys; never broadcast or a live quote.
//! Layout: jupiter-cpi/12bc5f67b94a2c3edc74d6e721a19442124a0bad/idl.json.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as A, SolanaInstruction};
use anyhow::Result;
use serde_json::{json, Value};
pub(crate) const OUT: u64 = 1_500_001;
pub(crate) use crate::execution_owner_buy_wire::USDC;

pub(crate) fn quote() -> Value {
    json!({"inputMint": crate::execution_quote_canary_helpers::SOL_MINT,
        "outputMint": USDC, "inAmount":"10000000", "outAmount":OUT.to_string(),
        "otherAmountThreshold":"1492500", "swapMode":"ExactIn", "slippageBps":50,
        "instructionVersion":"V1", "platformFee":null, "priceImpactPct":"0.01",
        "routePlan":[{"percent":100,"swapInfo":{"label":"Raydium",
            "ammKey":format_pubkey(&[61;32]),
            "inputMint":crate::execution_quote_canary_helpers::SOL_MINT,"outputMint":USDC,
            "inAmount":"10000000","outAmount":OUT.to_string()}}]})
}
pub(crate) fn instructions(wallet: PubkeyBytes) -> Result<Vec<SolanaInstruction>> {
    let token = token_program_id();
    let mint = parse_pubkey(USDC, "fixture_usdc")?;
    let source = associated_token_address(&wallet, &wsol_mint(), &token);
    let destination = associated_token_address(&wallet, &mint, &token);
    let mut instructions = super::priority_fee_fixture::budget(200_000, 10_000);
    for (ata, mint) in [(source, wsol_mint()), (destination, mint)] {
        instructions.push(SolanaInstruction { program_id: associated_token_program_id(),
            accounts:vec![A::signer_writable(wallet),A::writable(ata),A::readonly(wallet),
                A::readonly(mint),A::readonly(system_program_id()),A::readonly(token)],
            data:vec![1] });
    }
    instructions.push(super::native_funding_fixture::transfer(wallet, source, 10_000_000));
    instructions.push(SolanaInstruction {program_id:token,accounts:vec![A::writable(source)],
        data:vec![17]});
    let jupiter = parse_pubkey(crate::execution_owner_buy_wire::JUPITER, "fixture_jupiter")?;
    // Anchor optional accounts use the program-id sentinel when absent.
    let mut accounts = vec![A::readonly(token),A::signer_writable(wallet),
        A::writable(source),A::writable(destination),A::readonly(jupiter),A::readonly(mint),
        A::readonly(jupiter),A::readonly(pda(&[b"__event_authority"],&jupiter)),A::readonly(jupiter)];
    // Pinned IDL raydiumSwap remaining accounts: exact 18-entry block.
    // AMM/authority/open orders/vaults/market program/market/books/event queue/
    // market vaults/vault signer/user source/user destination/owner. Pool keys synthetic.
    accounts.push(A::readonly(parse_pubkey(
        crate::execution_owner_buy_wire::RAYDIUM, "fixture_raydium")?));
    accounts.push(A::readonly(token));
    for key in 61..74 {
        accounts.push(if [62,66,73].contains(&key) { A::readonly([key;32]) }
            else { A::writable([key;32]) });
    }
    accounts.extend([A::writable(source),A::writable(destination),A::signer_writable(wallet)]);
    let mut data = vec![229,23,203,151,122,227,173,42];
    data.extend(1_u32.to_le_bytes());
    data.extend([7,100,0,1]);
    data.extend(10_000_000_u64.to_le_bytes());
    data.extend(OUT.to_le_bytes());
    data.extend(50_u16.to_le_bytes());
    data.push(0);
    instructions.push(SolanaInstruction {program_id:jupiter,accounts,data});
    instructions.push(SolanaInstruction {program_id:token,
        accounts:vec![A::writable(source),A::writable(wallet),A::signer_writable(wallet)],
        data:vec![9]});
    Ok(instructions)
}
pub(crate) fn payload(wallet: PubkeyBytes, instructions: &[SolanaInstruction]) -> Result<String> {
    Ok(crate::execution_native_floor::prepare_final_native_floor(
        wallet,[9;32],instructions,50_000_001)?.payload().to_owned())
}
