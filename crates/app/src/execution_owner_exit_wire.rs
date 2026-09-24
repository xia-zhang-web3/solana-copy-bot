//! Closed USDC→SOL Raydium route and wallet-owned setup for one owner exit.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as A};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use crate::execution_transaction_wire::{decode_message, DecodedInstruction};
use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;

const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const RAYDIUM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ROUTE: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];
const AMOUNT: u64 = 1_167_085;

pub(crate) struct OwnerExitWireProof { message_sha256: String }
impl OwnerExitWireProof {
    pub(crate) fn verify_same_payload(&self, payload: &str) -> Result<()> {
        ensure!(payload.len() <= 1644, "owner_exit_wire_size");
        ensure!(decode_message(payload, |_| Ok(()))?.binding.message_sha256
            == self.message_sha256, "owner_exit_wire_changed_after_signing");
        Ok(())
    }
}

pub(crate) fn verify(request: &ExecutionSubmitRequest,
    payload: &str) -> Result<OwnerExitWireProof> {
    ensure!(payload.len() <= 1644 && request.signal_id.starts_with("owner-exit:")
        && request.side == "sell"
        && (request.wallet_id.is_empty() || request.wallet_pubkey == request.wallet_id)
        && request.token == USDC && request.slippage_tolerance_bps <= 50,
        "owner_exit_wire_request");
    let wallet = parse_pubkey(&request.wallet_pubkey, "owner_exit_wire_wallet")?;
    let usdc = parse_pubkey(USDC, "owner_exit_wire_mint")?;
    let source = associated_token_address(&wallet, &usdc, &token_program_id());
    let destination = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    let quote: Value = serde_json::from_str(request.metadata.quote_response_json.as_deref()
        .context("owner_exit_wire_quote")?)?;
    let output = quote["outAmount"].as_str()
        .context("owner_exit_wire_output")?.parse::<u64>()?;
    ensure!(output > 0 && quote["inputMint"] == USDC
        && quote["outputMint"] == crate::execution_quote_canary_helpers::SOL_MINT
        && quote["inAmount"] == AMOUNT.to_string()
        && request.metadata.quote_in_amount_raw.as_deref() == Some("1167085")
        && request.metadata.quote_out_amount_raw.as_deref()
            == Some(output.to_string().as_str())
        && quote["swapMode"] == "ExactIn"
        && quote["slippageBps"].as_u64() == Some(request.slippage_tolerance_bps)
        && quote.get("instructionVersion").is_none_or(|v| v == "V1")
        && quote.get("platformFee").is_none_or(|v| v.is_null() || v["feeBps"] == 0),
        "owner_exit_wire_quote_binding");
    let route = quote["routePlan"].as_array().context("owner_exit_wire_quote_route")?;
    ensure!(route.len() == 1 && (route[0]["percent"] == 100 || route[0]["bps"] == 10_000),
        "owner_exit_wire_route_shape");
    let swap = &route[0]["swapInfo"];
    ensure!(swap["inputMint"] == USDC
        && swap["outputMint"] == crate::execution_quote_canary_helpers::SOL_MINT
        && swap["inAmount"] == AMOUNT.to_string()
        && swap["outAmount"] == output.to_string()
        && swap["label"] == "Raydium", "owner_exit_wire_route_step");
    let amm = parse_pubkey(swap["ammKey"].as_str()
        .context("owner_exit_wire_amm")?, "owner_exit_wire_amm")?;
    let jupiter = parse_pubkey(JUPITER, "owner_exit_wire_jupiter")?;
    let (message, _) = crate::execution_priority_fee_wire::decode_priority_fee_message(payload)?;
    ensure!(message.binding.message_bytes[0] & 0x80 == 0
        && message.binding.signature_count == 1
        && message.binding.required_signatures == 1
        && message.binding.accounts.first() == Some(&A::signer_writable(wallet)),
        "owner_exit_wire_payer");
    let mut route_index = None;
    for instruction in &message.instructions {
        if instruction.program.pubkey != jupiter { continue; }
        ensure!(route_index.replace(instruction.index).is_none(),
            "owner_exit_wire_multiple_routes");
        verify_route(instruction, wallet, source, destination, amm, output,
            request.slippage_tolerance_bps)?;
    }
    let route_index = route_index.context("owner_exit_wire_route_missing")?;
    verify_setup(&message.instructions, route_index, wallet, source, destination)?;
    Ok(OwnerExitWireProof { message_sha256: message.binding.message_sha256 })
}

fn verify_route(i: &DecodedInstruction, wallet: PubkeyBytes, source: PubkeyBytes,
    destination: PubkeyBytes, amm: PubkeyBytes, output: u64, slippage: u64) -> Result<()> {
    let jupiter = parse_pubkey(JUPITER, "owner_exit_wire_jupiter")?;
    ensure!(!i.program.is_writable && !i.program.is_signer,
        "owner_exit_wire_program_role");
    let mut cursor = Cursor { bytes: &i.data, offset: 0 };
    ensure!(cursor.take(8)? == ROUTE && cursor.take(4)? == 1_u32.to_le_bytes(),
        "owner_exit_wire_route_layout");
    let tag = cursor.byte()?;
    let count = match tag { 7 => 27, 105 => 18, _ => bail!("owner_exit_wire_variant") };
    let a = &i.accounts;
    ensure!(a.len() == count && a[0] == A::readonly(token_program_id())
        && a[1] == A::signer_writable(wallet)
        && a[2] == A::writable(source) && a[3] == A::writable(destination)
        && (a[4] == A::readonly(jupiter) || a[4] == A::writable(destination))
        && a[5] == A::readonly(wsol_mint())
        && a[6] == A::readonly(jupiter)
        && a[7] == A::readonly(pda(&[b"__event_authority"], &jupiter))
        && a[8] == A::readonly(jupiter), "owner_exit_wire_route_accounts");
    let r = &a[9..];
    let (source_index, destination_index, owner_index) =
        if tag == 7 { (15,16,17) } else { (6,7,8) };
    ensure!(r[0] == A::readonly(parse_pubkey(RAYDIUM, "owner_exit_wire_dex")?)
        && r[1] == A::readonly(token_program_id())
        && r[2] == A::writable(amm)
        && r[source_index] == A::writable(source)
        && r[destination_index] == A::writable(destination)
        && r[owner_index] == A::signer_writable(wallet),
        "owner_exit_wire_dex_accounts");
    for (index, account) in r[3..source_index].iter().enumerate() {
        let writable = if tag == 7 { ![3,7,14].contains(&(index+3)) } else { index != 0 };
        ensure!(!account.is_signer && account.is_writable == writable,
            "owner_exit_wire_dex_account_role");
    }
    ensure!(cursor.take(3)? == [100,0,1] && cursor.u64()? == AMOUNT
        && cursor.u64()? == output
        && cursor.take(2)? == (slippage as u16).to_le_bytes()
        && cursor.byte()? == 0 && cursor.offset == cursor.bytes.len(),
        "owner_exit_wire_amount_or_fee");
    Ok(())
}

fn verify_setup(instructions: &[DecodedInstruction], route: usize, wallet: PubkeyBytes,
    source: PubkeyBytes, destination: PubkeyBytes) -> Result<()> {
    let mut created = false;
    let mut closed = false;
    let mut floor = false;
    for i in instructions {
        if i.index == route { continue; }
        ensure!(!i.program.is_signer && !i.program.is_writable,
            "owner_exit_wire_setup_program_role");
        let a = &i.accounts;
        let before = i.index < route;
        if i.program.pubkey == compute_budget_program_id() {
            ensure!(before && a.is_empty(), "owner_exit_wire_compute_position");
        } else if i.program.pubkey == associated_token_program_id() {
            ensure!(before && !created && i.data == [1] && a.len() == 6
                && a[0] == A::signer_writable(wallet)
                && a[1] == A::writable(destination)
                && a[2] == A::signer_writable(wallet)
                && a[3] == A::readonly(wsol_mint())
                && a[4] == A::readonly(system_program_id())
                && a[5] == A::readonly(token_program_id()),
                "owner_exit_wire_ata_identity");
            created = true;
        } else if i.program.pubkey == token_program_id() {
            ensure!(!before && !closed && i.data == [9]
                && a == &[A::writable(destination), A::signer_writable(wallet),
                    A::signer_writable(wallet)], "owner_exit_wire_close_wsol");
            closed = true;
        } else if i.program.pubkey == system_program_id() {
            ensure!(!before && !floor && i.index + 1 == instructions.len()
                && i.data.len() == 12 && i.data[..4] == 2_u32.to_le_bytes()
                && u64::from_le_bytes(i.data[4..].try_into()?) > 0
                && a == &[A::signer_writable(wallet), A::signer_writable(wallet)],
                "owner_exit_wire_final_floor");
            floor = true;
        } else {
            bail!("owner_exit_wire_unsupported_outer_instruction");
        }
    }
    ensure!(floor && closed && source != destination,
        "owner_exit_wire_close_or_floor_missing");
    Ok(())
}

struct Cursor<'a> { bytes: &'a [u8], offset: usize }
impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(size).context("owner_exit_wire_length")?;
        let value = self.bytes.get(self.offset..end).context("owner_exit_wire_truncated")?;
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    fn u64(&mut self) -> Result<u64> { Ok(u64::from_le_bytes(self.take(8)?.try_into()?)) }
}
