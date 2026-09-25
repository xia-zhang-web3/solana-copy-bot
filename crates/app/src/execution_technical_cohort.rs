//! The fixed source cohort authority is bound before provider intake starts.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::native_buy::TechnicalCohortAuthority;
use std::collections::HashSet;

pub(crate) fn active(c: &ExecutionConfig) -> bool {
    c.technical_cohort.as_ref().is_some_and(|p| p.activate)
}

pub(crate) fn authority(c: &ExecutionConfig) -> Result<Option<TechnicalCohortAuthority>> {
    let Some(p) = c.technical_cohort.as_ref().filter(|p| p.activate) else {
        return Ok(None);
    };
    let activated_at = DateTime::parse_from_rfc3339(&p.activated_at)
        .context("technical_cohort_activation_time")?.with_timezone(&Utc);
    let deadline = DateTime::parse_from_rfc3339(&p.deadline)
        .context("technical_cohort_deadline")?.with_timezone(&Utc);
    ensure!(activated_at < deadline
        && deadline.signed_duration_since(activated_at)
            == chrono::Duration::seconds(i64::try_from(p.max_wait_seconds)?),
        "technical_cohort_fixed_window");
    Ok(Some(TechnicalCohortAuthority {
        run_id: p.run_id.clone(),
        wallet_ids: p.wallet_ids.clone(),
        mint_policy: p.mint_policy.clone(),
        activated_at,
        deadline,
        max_buy_count: p.max_buy_count,
        policy_identity: crate::execution_native_buy_rpc::policy_identity(c)?,
    }))
}

pub(crate) fn admission_wallets(
    authority: &TechnicalCohortAuthority,
    c: &ExecutionConfig,
) -> Result<HashSet<String>> {
    let cohort = c.technical_cohort.as_ref()
        .filter(|p| p.activate && p.run_id == authority.run_id)
        .context("technical_cohort_admission_authority")?;
    ensure!(cohort.wallet_ids == authority.wallet_ids
        && !c.canary_wallet_pubkey.is_empty()
        && !cohort.wallet_ids.contains(&c.canary_wallet_pubkey),
        "technical_cohort_admission_wallets");
    let mut wallets: HashSet<String> = authority.wallet_ids.iter().cloned().collect();
    wallets.insert(c.canary_wallet_pubkey.clone());
    ensure!(wallets.len() == authority.wallet_ids.len() + 1,
        "technical_cohort_admission_wallets");
    Ok(wallets)
}

pub(crate) fn before_deadline(c: &ExecutionConfig) -> Result<()> {
    if let Some(p) = c.technical_cohort.as_ref().filter(|p| p.activate) {
        let deadline = DateTime::parse_from_rfc3339(&p.deadline)
            .context("technical_cohort_deadline")?.with_timezone(&Utc);
        ensure!(Utc::now() < deadline, "technical_cohort_deadline");
    }
    Ok(())
}
