//! Receipt custody binds the parsed outer message to the immutable submitted SHA.
//! This validates evidence only; it never creates an execution/admission request.
use super::*;
use crate::execution_solana_tx::SolanaAccountMeta as A;
use copybot_storage_core::{ExecutionCanaryReceiptProof, SqliteStore};
use serde_json::Value;
pub(crate) fn verify(
    store: &SqliteStore,
    order_id: &str,
    proof: &ExecutionCanaryReceiptProof,
    value: &Value,
) -> Result<()> {
    let Some((wallet, run, scope)) = store.controlled_receipt_lot_scope()? else {
        return Ok(());
    };
    ensure!(
        wallet == proof.wallet_pubkey && scope.run(&wallet)? == run,
        "owned_wsol_receipt_scope"
    );
    let d = store
        .load_execution_canary_dispatch(order_id)?
        .context("owned_wsol_receipt_dispatch_missing")?;
    ensure!(
        d.order_id == order_id
            && d.wallet == wallet
            && d.side == proof.side
            && d.tx_signature == proof.tx_signature,
        "owned_wsol_receipt_dispatch_identity"
    );
    let payload = reconstruct(&value["result"])?;
    let m = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    ensure!(
        m.binding.message_sha256 == d.message_sha256,
        "owned_wsol_receipt_message_mismatch"
    );
    // Only order/wallet/side are read by this byte profile, no invented quote or signal.
    let identity = ExecutionSubmitRequest {
        order_id: order_id.into(),
        signal_id: d.signal_id,
        client_order_id: d.client_order_id,
        attempt: d.attempt,
        route: d.route,
        wallet_id: wallet.clone(),
        token: proof.token.clone(),
        side: proof.side.clone(),
        buy_size_sol: 0.0,
        slippage_tolerance_bps: 0,
        wallet_pubkey: wallet,
        entry_route_plan_json: None,
        metadata: Default::default(),
    };
    super::canonical_view_scoped(&identity, &payload)?;
    Ok(())
}
fn key(v: &Value) -> Result<PubkeyBytes> {
    parse_pubkey(
        v.as_str().context("owned_wsol_receipt_key")?,
        "owned_wsol_receipt_key",
    )
}
fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .or_else(|| v.as_str()?.parse().ok())
        .context("owned_wsol_receipt_number")
}
pub(super) fn reconstruct(result: &Value) -> Result<String> {
    let message = &result["transaction"]["message"];
    ensure!(
        message
            .get("addressTableLookups")
            .is_none_or(|v| v.is_null() || v.as_array().is_some_and(|a| a.is_empty())),
        "owned_wsol_receipt_lookup"
    );
    let keys = message["accountKeys"]
        .as_array()
        .context("owned_wsol_receipt_keys")?;
    ensure!(keys.len() <= 100, "owned_wsol_receipt_keys_bound");
    let meta = |key: PubkeyBytes| -> Result<A> {
        let rows = keys
            .iter()
            .filter(|v| v["pubkey"].as_str() == Some(bs58::encode(key).into_string().as_str()))
            .collect::<Vec<_>>();
        ensure!(rows.len() == 1, "owned_wsol_receipt_key_ambiguous");
        Ok(A {
            pubkey: key,
            is_signer: rows[0]["signer"]
                .as_bool()
                .context("owned_wsol_receipt_roles")?,
            is_writable: rows[0]["writable"]
                .as_bool()
                .context("owned_wsol_receipt_roles")?,
        })
    };
    let rows = message["instructions"]
        .as_array()
        .context("owned_wsol_receipt_instructions")?;
    ensure!(rows.len() <= 64, "owned_wsol_receipt_instruction_bound");
    let mut instructions = vec![];
    for row in rows {
        let program = key(&row["programId"])?;
        let info = &row["parsed"]["info"];
        let field = |name: &str| -> Result<A> { meta(key(&info[name])?) };
        let (accounts, data) = if row.get("parsed").is_none() {
            let accounts = row["accounts"]
                .as_array()
                .context("owned_wsol_receipt_raw_accounts")?;
            ensure!(
                accounts.len() <= 64,
                "owned_wsol_receipt_raw_accounts_bound"
            );
            let encoded = row["data"]
                .as_str()
                .filter(|s| s.len() <= 1700)
                .context("owned_wsol_receipt_raw_data")?;
            (
                accounts
                    .iter()
                    .map(|v| meta(key(v)?))
                    .collect::<Result<Vec<_>>>()?,
                bs58::decode(encoded).into_vec()?,
            )
        } else {
            match (program, row["parsed"]["type"].as_str()) {
                (p, Some("createAccountWithSeed")) if p == system_program_id() => {
                    let seed = info["seed"]
                        .as_str()
                        .filter(|s| s.len() <= 32)
                        .context("owned_wsol_receipt_seed")?;
                    let base = key(&info["base"])?;
                    (
                        vec![field("source")?, field("newAccount")?, meta(base)?],
                        [
                            3u32.to_le_bytes().to_vec(),
                            base.to_vec(),
                            (seed.len() as u64).to_le_bytes().to_vec(),
                            seed.as_bytes().to_vec(),
                            number(&info["lamports"])?.to_le_bytes().to_vec(),
                            number(&info["space"])?.to_le_bytes().to_vec(),
                            key(&info["owner"])?.to_vec(),
                        ]
                        .concat(),
                    )
                }
                (p, Some("transfer")) if p == system_program_id() => (
                    vec![field("source")?, field("destination")?],
                    [
                        2u32.to_le_bytes().to_vec(),
                        number(&info["lamports"])?.to_le_bytes().to_vec(),
                    ]
                    .concat(),
                ),
                (p, Some("initializeAccount3")) if p == token_program_id() => (
                    vec![field("account")?, field("mint")?],
                    [vec![18], key(&info["owner"])?.to_vec()].concat(),
                ),
                (p, Some("syncNative")) if p == token_program_id() => {
                    (vec![field("account")?], vec![17])
                }
                (p, Some("closeAccount")) if p == token_program_id() => (
                    vec![field("account")?, field("destination")?, field("owner")?],
                    vec![9],
                ),
                (p, Some("createIdempotent")) if p == associated_token_program_id() => (
                    vec![
                        field("source")?,
                        field("account")?,
                        field("wallet")?,
                        field("mint")?,
                        meta(system_program_id())?,
                        field("tokenProgram")?,
                    ],
                    vec![1],
                ),
                _ => anyhow::bail!("owned_wsol_receipt_parsed_profile_unsupported"),
            }
        };
        instructions.push(SolanaInstruction {
            program_id: program,
            accounts,
            data,
        });
    }
    let wallet = key(&keys.first().context("owned_wsol_receipt_payer")?["pubkey"])?;
    Ok(STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            key(&message["recentBlockhash"])?,
            &instructions,
        )?,
    ))
}
/// Signature-status failures and later failed-expense sweeps use the same message
/// binding as confirmed receipts. Missing evidence stays pending with obligations retained.
pub(crate) fn verify_failed(
    store: &SqliteStore,
    task: &copybot_storage_core::FailedExpenseTask,
    value: &Value,
) -> Result<()> {
    if store.controlled_receipt_lot_scope()?.is_none() {
        return Ok(());
    }
    let proof = ExecutionCanaryReceiptProof {
        tx_signature: task.tx_signature.clone(),
        wallet_pubkey: task.wallet.clone(),
        token: task.token.clone(),
        side: task.side.clone(),
        confirmation_status: task.commitment.clone(),
        slot: task.slot,
        confirmed_at: chrono::DateTime::parse_from_rfc3339(&task.detected_at)?
            .with_timezone(&chrono::Utc),
        reason: "failed_message_evidence_only".into(),
    };
    verify(store, &task.order_id, &proof, value)
}
