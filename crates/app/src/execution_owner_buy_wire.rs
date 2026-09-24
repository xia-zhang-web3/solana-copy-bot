//! Closed owner SOL→USDC ExactIn profile, independent of native-floor protection.
//! Jupiter V1 route/Swap IDL pinned at:
//! https://github.com/jup-ag/jupiter-cpi/blob/12bc5f67b94a2c3edc74d6e721a19442124a0bad/idl.json
//! Cross-checked: instruction-parser e6f77951377847c579112e6a16d8c17c5c092485.
//! RaydiumV2=105: jup-ag/rfq-v2-sdk f3f30ff2af48f63c11f326a486b8b0eb9611714f,
//! fill-decoder/idls/aggregator.json. AMM V2 accounts: raydium-io/raydium-amm
//! d26944bfb76fb5fa8f91e5d440c2050ed358ef81, program/src/instruction.rs:swap_base_in_v2.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use crate::execution_transaction_wire::{decode_message, DecodedInstruction};
use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;

#[path = "execution_owner_buy_wire_setup.rs"]
mod setup;
pub(crate) const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub(crate) const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub(crate) const RAYDIUM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const AMOUNT: u64 = 10_000_000;
const ROUTE: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];

/// Private construction: an authenticated owner request still needs a byte proof.
pub(crate) struct OwnerBuyWireProof {
    amount: u64,
    message_sha256: String,
}
impl OwnerBuyWireProof {
    pub(crate) fn decoded_amount_lamports(&self) -> u64 { self.amount }
    pub(crate) fn verify_same_payload(&self, payload: &str) -> Result<()> {
        ensure!(payload.len() <= 1644, "owner_buy_wire_size");
        let decoded = decode_message(payload, |_| Ok(()))?;
        ensure!(decoded.binding.message_sha256 == self.message_sha256,
            "owner_buy_wire_changed_after_signing");
        Ok(())
    }
}
pub(crate) fn required(request: &ExecutionSubmitRequest) -> bool {
    request.signal_id.starts_with("owner-buy:") && request.metadata.protected_capital.is_some()
}

pub(crate) fn verify(request: &ExecutionSubmitRequest, payload: &str) -> Result<OwnerBuyWireProof> {
    ensure!(payload.len() <= 1644, "owner_buy_wire_size");
    ensure!(request.signal_id.starts_with("owner-buy:") && request.side == "buy"
        && request.wallet_pubkey == request.wallet_id && request.token == USDC
        && request.buy_size_sol == 0.01 && request.slippage_tolerance_bps <= 50,
        "owner_buy_wire_request");
    let wallet = parse_pubkey(&request.wallet_pubkey, "owner_buy_wire_wallet")?;
    let mint = parse_pubkey(USDC, "owner_buy_wire_mint")?;
    let source = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    let destination = associated_token_address(&wallet, &mint, &token_program_id());
    let quote: Value = serde_json::from_str(request.metadata.quote_response_json.as_deref()
        .context("owner_buy_wire_quote")?)?;
    let output = quote["outAmount"].as_str().context("owner_buy_wire_quote_output")?
        .parse::<u64>()?;
    ensure!(output > 0 && quote["inputMint"] == crate::execution_quote_canary_helpers::SOL_MINT
        && quote["outputMint"] == USDC && quote["inAmount"] == AMOUNT.to_string()
        && request.metadata.quote_in_amount_raw.as_deref() == Some("10000000")
        && request.metadata.quote_out_amount_raw.as_deref() == Some(output.to_string().as_str())
        && quote["swapMode"] == "ExactIn"
        && quote["slippageBps"].as_u64() == Some(request.slippage_tolerance_bps)
        && quote.get("instructionVersion").is_none_or(|v| v == "V1")
        && quote.get("platformFee").is_none_or(|v| v.is_null() || v["feeBps"] == 0),
        "owner_buy_wire_quote_binding");
    let route = quote["routePlan"].as_array().context("owner_buy_wire_quote_route")?;
    ensure!(route.len() == 1 && (route[0]["percent"] == 100 || route[0]["bps"] == 10_000),
        "owner_buy_wire_quote_route");
    let swap = &route[0]["swapInfo"];
    ensure!(swap["inputMint"] == quote["inputMint"] && swap["outputMint"] == quote["outputMint"]
        && swap["inAmount"] == quote["inAmount"] && swap["outAmount"] == quote["outAmount"],
        "owner_buy_wire_quote_step");
    ensure!(swap["label"] == "Raydium", "owner_buy_wire_unsupported_dex");
    let amm = parse_pubkey(swap["ammKey"].as_str().context("owner_buy_wire_quote_amm")?,
        "owner_buy_wire_amm")?;
    let jupiter = parse_pubkey(JUPITER, "owner_buy_wire_program")?;
    let (message, _) = crate::execution_priority_fee_wire::decode_priority_fee_message(payload)?;
    ensure!(message.binding.message_bytes[0] & 0x80 == 0
        && message.binding.signature_count == 1 && message.binding.required_signatures == 1
        && message.binding.accounts.first() == Some(&SolanaAccountMeta::signer_writable(wallet)),
        "owner_buy_wire_payer");
    let mut route_index = None;
    for instruction in &message.instructions {
        if instruction.program.pubkey != jupiter { continue; }
        ensure!(route_index.replace(instruction.index).is_none(), "owner_buy_wire_multiple_routes");
        verify_route(instruction, wallet, source, destination, mint, jupiter,
            amm, output, request.slippage_tolerance_bps)?;
    }
    let route_index = route_index.context("owner_buy_wire_route_missing")?;
    setup::verify(&message.instructions, route_index, wallet, source, destination, mint)?;
    Ok(OwnerBuyWireProof { amount: AMOUNT, message_sha256: message.binding.message_sha256 })
}

fn verify_route(
    instruction: &DecodedInstruction, wallet: PubkeyBytes, source: PubkeyBytes,
    destination: PubkeyBytes, mint: PubkeyBytes, jupiter: PubkeyBytes,
    amm: PubkeyBytes, output: u64, slippage: u64,
) -> Result<()> {
    ensure!(!instruction.program.is_writable && !instruction.program.is_signer,
        "owner_buy_wire_program_role");
    // Both supported Swap variants are fieldless. Select the exact account ABI
    // from the Borsh enum byte, never from a quote label or an account-count guess.
    let mut cursor = Cursor { bytes: &instruction.data, offset: 0 };
    ensure!(cursor.take(8)? == ROUTE && cursor.take(4)? == 1_u32.to_le_bytes(),
        "owner_buy_wire_route_layout");
    let tag = cursor.byte()?;
    let count = match tag {
        7 => 27,
        105 => 18,
        _ => bail!("owner_buy_wire_swap_variant"),
    };
    let a = &instruction.accounts;
    ensure!(a.len() == count && a[0] == SolanaAccountMeta::readonly(token_program_id())
        && a[1] == SolanaAccountMeta::signer_writable(wallet)
        && a[2] == SolanaAccountMeta::writable(source)
        && a[3] == SolanaAccountMeta::writable(destination)
        && (a[4] == SolanaAccountMeta::readonly(jupiter)
            || a[4] == SolanaAccountMeta::writable(destination))
        && a[5] == SolanaAccountMeta::readonly(mint)
        && a[6] == SolanaAccountMeta::readonly(jupiter)
        && a[7] == SolanaAccountMeta::readonly(pda(&[b"__event_authority"], &jupiter))
        && a[8] == SolanaAccountMeta::readonly(jupiter), "owner_buy_wire_route_accounts");
    // Classic Raydium has 18 remaining accounts; RaydiumV2 has program + 8 CPI accounts.
    // The AMM program owns pool semantics; bind its identity, quoted pool and user ends.
    let r = &a[9..];
    let (source_index, destination_index, owner_index) = if tag == 7 { (15,16,17) } else { (6,7,8) };
    ensure!(r[0] == SolanaAccountMeta::readonly(parse_pubkey(RAYDIUM, "owner_buy_wire_dex")?)
        && r[1] == SolanaAccountMeta::readonly(token_program_id())
        && r[2] == SolanaAccountMeta::writable(amm)
        && r[source_index] == SolanaAccountMeta::writable(source)
        && r[destination_index] == SolanaAccountMeta::writable(destination)
        && r[owner_index] == SolanaAccountMeta::signer_writable(wallet),
        "owner_buy_wire_dex_accounts");
    for (index, account) in r[3..source_index].iter().enumerate() {
        let writable = if tag == 7 { ![3, 7, 14].contains(&(index + 3)) } else { index != 0 };
        ensure!(!account.is_signer && account.is_writable == writable,
            "owner_buy_wire_dex_account_role");
    }
    // Parse from the start, including the Vec length and every supported enum field.
    // No suffix extraction: an unknown enum, truncated field or trailing byte rejects.
    ensure!(cursor.take(3)? == [100, 0, 1], "owner_buy_wire_route_topology");
    ensure!(cursor.u64()? == AMOUNT, "owner_buy_wire_exact_input");
    ensure!(cursor.u64()? == output, "owner_buy_wire_exact_output");
    ensure!(cursor.take(2)? == (slippage as u16).to_le_bytes() && cursor.byte()? == 0,
        "owner_buy_wire_slippage_or_fee");
    ensure!(cursor.offset == cursor.bytes.len(), "owner_buy_wire_trailing_bytes");
    Ok(())
}
struct Cursor<'a> { bytes: &'a [u8], offset: usize }
impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(size).context("owner_buy_wire_length")?;
        let value = self.bytes.get(self.offset..end).context("owner_buy_wire_truncated")?;
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    fn u64(&mut self) -> Result<u64> { Ok(u64::from_le_bytes(self.take(8)?.try_into()?)) }
}
