//! Exact temporary-account initial funding. Snapshot races still fail atomically at Create.
use super::*;
use crate::execution_native_ata_funding::types::ExplicitAtaCoverage;
use crate::execution_native_funding::types::FundingOperation as F;
use crate::execution_native_rpc::{rent_types::ClassicAtaFundingFacts, types::AccountObservation};
pub(crate) fn check_initial(
    r: &ExecutionSubmitRequest,
    payload: &str,
    facts: &ClassicAtaFundingFacts,
) -> Result<u128> {
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    // Binds all accounts, roles, bytes and the fee observation, not just the temp key.
    let plan =
        crate::execution_native_ata_funding::plan_supported_ata_funding(payload, wallet, facts)?;
    let view = super::canonical_view_scoped(r, payload)?;
    super::verify_canonical(r, &view)?;
    ensure!(
        plan.explicit_ata_coverage == ExplicitAtaCoverage::Complete,
        "owned_wsol_ata_funding_unknown"
    );
    let (_, temp) = super::temporary(r)?;
    ensure!(
        facts
            .native()
            .accounts()
            .value
            .iter()
            .any(|a| a.pubkey == temp && a.account == AccountObservation::Absent),
        "owned_wsol_temporary_occupied"
    );
    let mut rent = 0_u64;
    let mut transfers = 0_u128;
    for row in &facts.native().requirements().requirements {
        match &row.operation {
            F::CreateAccountWithSeed {
                account, lamports, ..
            } => {
                ensure!(
                    *account == temp && rent == 0 && *lamports == facts.rent().lamports(),
                    "owned_wsol_rent_changed"
                );
                rent = *lamports;
            }
            F::SystemTransfer {
                from, to, lamports, ..
            } if *from == wallet && *to != wallet => {
                transfers = transfers
                    .checked_add(u128::from(*lamports))
                    .context("owned_wsol_funding_overflow")?;
            }
            _ => {}
        }
    }
    ensure!(rent > 0, "owned_wsol_rent_missing");
    let fee = facts
        .native()
        .fee()
        .value
        .context("owned_wsol_fee_missing")?;
    let required = u128::from(rent)
        .checked_add(transfers)
        .and_then(|n| n.checked_add(u128::from(fee)))
        .and_then(|n| n.checked_add(plan.known_wallet_payer_lamports))
        .context("owned_wsol_funding_overflow")?;
    ensure!(
        required <= u128::from(copybot_storage_core::TINY_NATIVE_ALLOWANCE),
        "receipt_lot_native_spend_cap"
    );
    let payer = facts
        .native()
        .accounts()
        .value
        .first()
        .context("owned_wsol_payer")?;
    let available = match &payer.account {
        AccountObservation::Present {
            lamports,
            owner_program,
            executable,
            data,
        } if payer.pubkey == wallet
            && *owner_program == system_program_id()
            && !executable
            && data.is_empty() =>
        {
            *lamports
        }
        _ => anyhow::bail!("owned_wsol_payer"),
    };
    ensure!(
        u128::from(available) >= required,
        "owned_wsol_insufficient_initial_funds"
    );
    Ok(required)
}
pub(crate) async fn collect(
    http: &reqwest::Client,
    c: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    payload: &str,
) -> Result<()> {
    if c.controlled_receipt_lot.is_none() {
        return Ok(());
    }
    let r = super::bound_request(plan)?;
    let wallet = parse_pubkey(&r.wallet_pubkey, "owned_wsol_wallet")?;
    let endpoint = if c.owned_sell_preparation.is_some() {
        crate::execution_owned_sell_rpc::endpoint(c)?.to_string()
    } else {
        c.submit_adapter_http_url.clone()
    };
    // The client keeps its existing no-redirect/no-retry, byte and total time caps.
    let _ = http;
    let facts = crate::execution_native_rpc::NativeFundingRpcClient::new()?
        .collect_with_supported_ata_rent(
            &endpoint,
            std::time::Duration::from_millis(c.submit_timeout_ms.clamp(1, 30_000)),
            payload,
            wallet,
            None,
        )
        .await?;
    check_initial(&r, payload, &facts)?;
    Ok(())
}
