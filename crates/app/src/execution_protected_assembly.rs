//! Paid/transaction fallback BUY uses the same final guard constructor as the bundle.
use crate::execution_native_floor_policy::{protected, required_for_plan};
use crate::execution_solana_tx::SolanaInstruction;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;

pub(crate) fn prepare(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    result: &mut SwapTransactionDryRunResult,
) -> Result<()> {
    if !protected::enabled(config) || !plan.side.eq_ignore_ascii_case("buy") {
        return Ok(());
    }
    let floor = required_for_plan(config, plan)?
        .ok_or_else(|| anyhow::anyhow!("tiny_capital_mode_inactive"))?;
    let message = crate::execution_transaction_wire::decode_message(
        &result.serialized_transaction_base64,
        |_| Ok(()),
    )?;
    ensure!(
        message.binding.message_bytes[0] & 0x80 == 0 && message.binding.required_signatures == 1,
        "tiny_capital_unsupported_message"
    );
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
        &plan.wallet_pubkey,
        "tiny_capital_wallet",
    )?;
    ensure!(
        message.binding.accounts[0].pubkey == wallet,
        "tiny_capital_wallet_binding"
    );
    let instructions = message
        .instructions
        .into_iter()
        .map(|ix| SolanaInstruction {
            program_id: ix.program.pubkey,
            accounts: ix.accounts,
            data: ix.data,
        })
        .collect::<Vec<_>>();
    let prepared = crate::execution_native_floor::prepare_final_native_floor(
        wallet,
        message.recent_blockhash,
        &instructions,
        floor,
    )?;
    let binding = crate::execution_instruction_bundle_binding::BundleRequest::capture(plan)?;
    crate::execution_priority_fee_proof::prove(
        binding.request(),
        prepared.payload(),
        config.pretrade_max_priority_fee_lamports,
    )?;
    crate::execution_native_floor_policy::verify_signing_payload(
        config,
        binding.request(),
        plan,
        prepared.payload(),
    )?;
    result.serialized_transaction_base64 = prepared.payload().into();
    Ok(())
}
