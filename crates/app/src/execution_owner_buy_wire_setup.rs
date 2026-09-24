//! Only wallet-owned wrap/sync/close and classic ATA setup around one exact route.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as A};
use crate::execution_transaction_wire::DecodedInstruction;
use anyhow::{bail, ensure, Result};

pub(super) fn verify(
    instructions: &[DecodedInstruction], route: usize, wallet: PubkeyBytes,
    source: PubkeyBytes, destination: PubkeyBytes, mint: PubkeyBytes,
) -> Result<()> {
    let mut created = [false; 2];
    let mut funded = false;
    let mut synced = false;
    let mut closed = false;
    let mut floor = false;
    for i in instructions {
        if i.index == route {
            ensure!(funded && synced, "owner_buy_wire_unfunded_route");
            continue;
        }
        ensure!(!i.program.is_signer && !i.program.is_writable,
            "owner_buy_wire_setup_program_role");
        let a = &i.accounts;
        let before = i.index < route;
        if i.program.pubkey == compute_budget_program_id() {
            // Fee parser validates exact compute opcode lengths/duplicates and bounds.
            ensure!(before && a.is_empty(), "owner_buy_wire_compute_position");
        } else if i.program.pubkey == associated_token_program_id() {
            ensure!(before && i.data == [1] && a.len() == 6,
                "owner_buy_wire_ata_layout");
            let index = if a[1].pubkey == source { 0 } else { 1 };
            let (ata, asset) = if index == 0 { (source, wsol_mint()) } else { (destination, mint) };
            ensure!(!created[index] && (index != 0 || !funded)
                && a[0] == A::signer_writable(wallet) && a[1] == A::writable(ata)
                && a[2] == A::signer_writable(wallet) && a[3] == A::readonly(asset)
                && a[4] == A::readonly(system_program_id())
                && a[5] == A::readonly(token_program_id()), "owner_buy_wire_ata_identity");
            created[index] = true;
        } else if i.program.pubkey == system_program_id() {
            ensure!(i.data.len() == 12 && i.data[..4] == 2_u32.to_le_bytes()
                && a.len() == 2 && a[0] == A::signer_writable(wallet),
                "owner_buy_wire_transfer_layout");
            let amount = u64::from_le_bytes(i.data[4..].try_into()?);
            if before {
                ensure!(!funded && !synced && a[1] == A::writable(source)
                    && amount == 10_000_000, "owner_buy_wire_wrap_amount_or_destination");
                funded = true;
            } else {
                // Exact reserve is proved independently by the native-floor policy.
                ensure!(i.index + 1 == instructions.len() && !floor && amount > 0
                    && a[1] == A::signer_writable(wallet), "owner_buy_wire_final_floor");
                floor = true;
            }
        } else if i.program.pubkey == token_program_id() {
            match i.data.as_slice() {
                [17] => {
                    ensure!(before && funded && !synced && a == &[A::writable(source)],
                        "owner_buy_wire_sync");
                    synced = true;
                }
                [9] => {
                    ensure!(!before && !closed && !floor
                        && a == &[A::writable(source), A::signer_writable(wallet),
                            A::signer_writable(wallet)], "owner_buy_wire_close");
                    closed = true;
                }
                _ => bail!("owner_buy_wire_unsupported_token_instruction"),
            }
        } else {
            bail!("owner_buy_wire_unsupported_outer_instruction");
        }
    }
    ensure!(floor, "owner_buy_wire_floor_missing");
    Ok(())
}
