//! Confirmed owner BUY input; transaction-owned WSOL uses the existing wire profile.
use super::{
    count_ix, field, ix_is, ExecutionCanaryReceiptFacts, NativeAccountObservations,
    NativeInstructionObservation, ATA, SYSTEM, TOKEN, TOKEN_2022, WSOL,
};
use sha2::{Digest, Sha256};

pub(super) fn confirmed_input(
    f: &ExecutionCanaryReceiptFacts,
    o: &NativeAccountObservations,
    amount: u64,
) -> bool {
    if amount == 0 || f.token_delta.is_none_or(|d| d.raw <= 0) {
        return false;
    }
    let amount = amount.to_string();
    let funded: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| {
            ix_is(ix, SYSTEM, "transfer")
                && field(ix, "source") == Some(&f.wallet_pubkey)
                && field(ix, "lamports") == Some(&amount)
        })
        .collect();
    if funded.len() != 1 {
        return false;
    }
    let Some(account) = field(funded[0], "destination") else {
        return false;
    };
    let ata = count_ix(o, |ix| {
        ix_is(ix, ATA, "createIdempotent")
            && field(ix, "account") == Some(account)
            && field(ix, "wallet") == Some(&f.wallet_pubkey)
            && field(ix, "mint") == Some(WSOL)
    });
    (ata == 1 || (ata == 0 && temporary(f, o, account, funded[0])))
        && count_ix(o, |ix| {
            matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && ix.instruction_type.value.as_deref() == Some("transfer")
                && field(ix, "source") == Some(account)
                && field(ix, "authority") == Some(&f.wallet_pubkey)
                && field(ix, "amount") == Some(&amount)
        }) == 1
        && count_ix(o, |ix| {
            matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && ix.instruction_type.value.as_deref() == Some("closeAccount")
                && field(ix, "account") == Some(account)
                && field(ix, "destination") == Some(&f.wallet_pubkey)
        }) == 1
}

// The runtime first reconstructs this receipt against the durable dispatch message SHA
// and verifies its canonical lifecycle. Recheck the exact account/owner links here;
// no quote, balance difference or native BUY estimate substitutes for these operands.
fn temporary(
    f: &ExecutionCanaryReceiptFacts,
    o: &NativeAccountObservations,
    account: &str,
    funding: &NativeInstructionObservation,
) -> bool {
    let order = f
        .order_id
        .strip_prefix("exec-canary:")
        .unwrap_or(&f.order_id);
    let Ok(identity) = serde_json::to_vec(&(
        "copybot-transaction-owned-wsol-v1",
        order,
        &f.wallet_pubkey,
        "buy",
    )) else {
        return false;
    };
    let seed = format!("{:x}", Sha256::digest(identity))[..32].to_owned();
    let creates: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| field(ix, "newAccount") == Some(account))
        .collect();
    let initializers: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| {
            field(ix, "account") == Some(account)
                && ix
                    .instruction_type
                    .value
                    .as_deref()
                    .is_some_and(|k| k.starts_with("initializeAccount"))
        })
        .collect();
    let closes: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| {
            field(ix, "account") == Some(account)
                && ix.instruction_type.value.as_deref() == Some("closeAccount")
        })
        .collect();
    if creates.len() != 1 || initializers.len() != 1 || closes.len() != 1 {
        return false;
    }
    let transfers: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| {
            field(ix, "source") == Some(account)
                && ix.instruction_type.value.as_deref() == Some("transfer")
        })
        .collect();
    if transfers.len() != 1 {
        return false;
    }
    let transfer = transfers[0];
    let (create, init, close) = (creates[0], initializers[0], closes[0]);
    [create, init, close, funding, transfer]
        .into_iter()
        .all(known)
        && ix_is(create, SYSTEM, "createAccountWithSeed")
        && field(create, "source") == Some(&f.wallet_pubkey)
        && field(create, "base") == Some(&f.wallet_pubkey)
        && field(create, "owner") == Some(TOKEN)
        && field(create, "space") == Some("165")
        && field(create, "seed") == Some(&seed)
        && field(create, "lamports")
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|rent| rent > 0 && rent <= crate::TINY_NATIVE_ALLOWANCE)
        && ix_is(transfer, TOKEN, "transfer")
        && ix_is(init, TOKEN, "initializeAccount3")
        && field(init, "mint") == Some(WSOL)
        && field(init, "owner") == Some(&f.wallet_pubkey)
        && ix_is(close, TOKEN, "closeAccount")
        && field(close, "destination") == Some(&f.wallet_pubkey)
        && field(close, "owner") == Some(&f.wallet_pubkey)
}
fn known(ix: &NativeInstructionObservation) -> bool {
    let known = crate::ObservationCoverage::Known;
    ix.program_id.coverage == known
        && ix.instruction_type.coverage == known
        && ix
            .fields
            .values()
            .all(|f| f.coverage == known && f.value.is_some())
}
