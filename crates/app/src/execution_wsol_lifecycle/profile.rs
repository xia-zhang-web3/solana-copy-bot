//! A bijection of one verified canonical WSOL profile. No arbitrary account rewriting.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as A, SolanaInstruction as I};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
pub(crate) const DOMAIN: &str = "copybot-transaction-owned-wsol-v1";
pub(crate) fn temporary(r: &ExecutionSubmitRequest) -> Result<(String, PubkeyBytes)> {
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let seed = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(
            DOMAIN,
            super::order(&r.order_id),
            &r.wallet_pubkey,
            &r.side
        ))?)
    )[..32]
        .to_owned();
    let mut h = Sha256::new();
    h.update(wallet);
    h.update(seed.as_bytes());
    h.update(token_program_id());
    Ok((seed, h.finalize().into()))
}
pub(crate) fn create(wallet: PubkeyBytes, temp: PubkeyBytes, seed: &str, rent: u64) -> I {
    let data = [
        3_u32.to_le_bytes().to_vec(),
        wallet.to_vec(),
        (seed.len() as u64).to_le_bytes().to_vec(),
        seed.as_bytes().to_vec(),
        rent.to_le_bytes().to_vec(),
        165_u64.to_le_bytes().to_vec(),
        token_program_id().to_vec(),
    ]
    .concat();
    // Canonical System Program CreateAccountWithSeed account ABI; base is the
    // same payer key, so the compiled message still requires exactly one signer.
    I {
        program_id: system_program_id(),
        accounts: vec![
            A::signer_writable(wallet),
            A::writable(temp),
            A::signer_writable(wallet),
        ],
        data,
    }
}
pub(crate) fn initialize(wallet: PubkeyBytes, temp: PubkeyBytes) -> I {
    I {
        program_id: token_program_id(),
        accounts: vec![A::writable(temp), A::readonly(wsol_mint())],
        data: [vec![18], wallet.to_vec()].concat(),
    }
}
pub(crate) fn parse_create(
    i: &I,
    wallet: PubkeyBytes,
    temp: PubkeyBytes,
    seed: &str,
) -> Result<u64> {
    ensure!(i.data.len() == 124, "owned_wsol_create_length");
    let rent = u64::from_le_bytes(i.data[76..84].try_into()?);
    ensure!(
        rent > 0
            && rent <= copybot_storage_core::TINY_NATIVE_ALLOWANCE
            && *i == create(wallet, temp, seed, rent),
        "owned_wsol_create_binding"
    );
    Ok(rent)
}
fn ata(wallet: PubkeyBytes, canonical: PubkeyBytes) -> I {
    I {
        program_id: associated_token_program_id(),
        accounts: vec![
            A::signer_writable(wallet),
            A::writable(canonical),
            A::signer_writable(wallet),
            A::readonly(wsol_mint()),
            A::readonly(system_program_id()),
            A::readonly(token_program_id()),
        ],
        data: vec![1],
    }
}
fn rewrite(
    r: &ExecutionSubmitRequest,
    i: &mut I,
    from: PubkeyBytes,
    to: PubkeyBytes,
) -> Result<()> {
    let jup = parse_pubkey(
        crate::execution_owner_buy_wire::JUPITER,
        "owned_wsol_jupiter",
    )?;
    let allowed = if i.program_id == jup {
        let tag = *i.data.get(12).context("owned_wsol_route_tag")?;
        let index = if r.side == "buy" { 2 } else { 3 };
        let nested = match (tag, r.side.as_str()) {
            (7, "buy") => 24,
            (7, "sell") => 25,
            (105, "buy") => 15,
            (105, "sell") => 16,
            _ => anyhow::bail!("owned_wsol_route_variant"),
        };
        ensure!(
            i.accounts.get(index).is_some_and(|a| a.pubkey == from)
                && i.accounts.get(nested).is_some_and(|a| a.pubkey == from),
            "owned_wsol_route_operands"
        );
        if r.side == "sell" && i.accounts.get(4).is_some_and(|a| a.pubkey == from) {
            vec![index, 4, nested]
        } else {
            vec![index, nested]
        }
    } else if i.program_id == system_program_id()
        && i.data.get(..4) == Some(&2_u32.to_le_bytes())
        && r.side == "buy"
    {
        vec![1]
    } else if i.program_id == token_program_id() && matches!(i.data.as_slice(), [17] | [9]) {
        vec![0]
    } else {
        vec![]
    };
    for (n, a) in i.accounts.iter_mut().enumerate() {
        if a.pubkey == from {
            ensure!(allowed.contains(&n), "owned_wsol_ambiguous_operand");
            a.pubkey = to;
        }
    }
    ensure!(
        !i.data.windows(32).any(|a| a == from),
        "owned_wsol_opaque_address"
    );
    Ok(())
}
pub(crate) fn normalize(r: &ExecutionSubmitRequest, original: &[I], rent: u64) -> Result<Vec<I>> {
    ensure!(
        rent > 0 && rent <= copybot_storage_core::TINY_NATIVE_ALLOWANCE,
        "owned_wsol_rent_cap"
    );
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let canonical = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    let (seed, temp) = temporary(r)?;
    let mut out = vec![];
    let mut inserted = false;
    for i in original {
        if i.program_id == associated_token_program_id()
            && i.accounts.get(1).is_some_and(|a| a.pubkey == canonical)
        {
            ensure!(!inserted, "owned_wsol_duplicate_setup");
            out.extend([create(wallet, temp, &seed, rent), initialize(wallet, temp)]);
            inserted = true;
            continue;
        }
        // Canonical provider profile may omit an existing WSOL ATA creation.
        if !inserted && i.accounts.iter().any(|a| a.pubkey == canonical) {
            out.extend([create(wallet, temp, &seed, rent), initialize(wallet, temp)]);
            inserted = true;
        }
        let mut i = i.clone();
        rewrite(r, &mut i, canonical, temp)?;
        out.push(i);
    }
    ensure!(inserted, "owned_wsol_setup_missing");
    Ok(out)
}
pub(super) fn canonical_view(r: &ExecutionSubmitRequest, original: &[I]) -> Result<Vec<I>> {
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let canonical = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    let (seed, temp) = temporary(r)?;
    let mut out = vec![];
    let mut created = false;
    let mut initialized = false;
    let mut used = false;
    for i in original {
        ensure!(
            !i.accounts.iter().any(|a| a.pubkey == canonical),
            "owned_wsol_canonical_operand"
        );
        if i.program_id == system_program_id() && i.data.get(..4) == Some(&3_u32.to_le_bytes()) {
            ensure!(!created && !used, "owned_wsol_create_order");
            parse_create(i, wallet, temp, &seed)?;
            created = true;
            out.push(ata(wallet, canonical));
            continue;
        }
        if i.program_id == token_program_id() && i.data.first() == Some(&18) {
            ensure!(
                created && !initialized && !used && *i == initialize(wallet, temp),
                "owned_wsol_initialize_binding"
            );
            initialized = true;
            continue;
        }
        if i.accounts.iter().any(|a| a.pubkey == temp) {
            ensure!(created && initialized, "owned_wsol_uninitialized_use");
            used = true;
        }
        let mut i = i.clone();
        rewrite(r, &mut i, temp, canonical)?;
        out.push(i);
    }
    ensure!(
        created
            && initialized
            && used
            && original
                .iter()
                .filter(|i| i.program_id == token_program_id()
                    && i.data == [9]
                    && i.accounts.first().is_some_and(|a| a.pubkey == temp))
                .count()
                == 1,
        "owned_wsol_lifecycle_missing"
    );
    Ok(out)
}
