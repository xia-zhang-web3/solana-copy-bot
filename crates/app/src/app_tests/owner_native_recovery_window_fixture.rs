//! Explicit synthetic history/clock and loopback RPC; no live quote or send proof.
use super::owner_native_recovery_fixture::NativeCase;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{OwnerNativeRecoveryConfig, RecoveryUsdcBaselineConfig};

pub(super) async fn fresh_case() -> Result<NativeCase> {
    let mut c = Box::pin(NativeCase::new()).await?;
    let p = c.config.owner_exit.as_mut().unwrap();
    let activated = DateTime::parse_from_rfc3339(&p.activated_at)?.with_timezone(&Utc);
    let expires = DateTime::parse_from_rfc3339(&p.expires_at)?.with_timezone(&Utc);
    // Only this disposable synthetic graph is aged. Production/history is never edited.
    let sql = c.gate.sql()?;
    let tables = ["native_buy_technical_cohort", "execution_tiny_experiment",
        "native_buy_fence_epochs", "native_buy_decisions", "native_buy_cohort_decisions",
        "execution_canary_receipt_proofs"];
    for table in tables {
        let names=sql.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?1")?
            .query_map([table],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for name in names { sql.execute_batch(&format!("DROP TRIGGER \"{name}\""))?; }
    }
    for (table, column) in [("native_buy_technical_cohort","activated_at"),
        ("native_buy_technical_cohort","deadline"), ("execution_tiny_experiment","activated_at"),
        ("execution_tiny_experiment","deadline"), ("native_buy_fence_epochs","sampled_at"),
        ("native_buy_decisions","admitted_at"), ("native_buy_decisions","finalized_at"),
        ("native_buy_cohort_decisions","admitted_at"), ("execution_canary_receipt_proofs","confirmed_at")] {
        let rows=sql.prepare(&format!("SELECT rowid,{column} FROM {table} WHERE {column} IS NOT NULL"))?
            .query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, at) in rows {
            let old=DateTime::parse_from_rfc3339(&at)?.with_timezone(&Utc)-Duration::hours(5);
            sql.execute(&format!("UPDATE {table} SET {column}=?1 WHERE rowid=?2"),
                rusqlite::params![old.to_rfc3339(),id])?;
        }
    }
    let wallet=crate::execution_pumpswap_accounts::parse_pubkey(&p.wallet_pubkey,"mock_wallet")?;
    let mint=crate::execution_pumpswap_accounts::parse_pubkey(&p.mint,"mock_mint")?;
    let ata=crate::execution_pumpswap_accounts::associated_token_address(&wallet,&mint,
        &crate::execution_pumpswap_accounts::token_program_id());
    p.native_recovery=Some(OwnerNativeRecoveryConfig {
        policy:copybot_config::FRESH_NATIVE_RECOVERY_V1.into(),
        original_activated_at:(activated-Duration::hours(5)).to_rfc3339(),
        original_expires_at:(expires-Duration::hours(5)).to_rfc3339(),
        original_policy_identity:"b".repeat(64),
        protected_bot_usdc_baseline:RecoveryUsdcBaselineConfig {
            policy:copybot_config::RECOVERY_USDC_100_V1.into(),amount_raw:100,
            token_account:bs58::encode(ata).into_string(),
        },
    });
    p.activated_at=(Utc::now()-Duration::seconds(1)).to_rfc3339();
    p.expires_at=(Utc::now()+Duration::minutes(10)).to_rfc3339();
    {let mut s=c.provider.state.lock().unwrap();s.protected_baseline_raw=100;s.protected_balance_slot=Some(122); }
    copybot_config::validate_owner_exit(&c.config)?;
    Ok(c)
}

pub(super) fn intent(c: &NativeCase) -> Result<copybot_storage_core::OwnerExitIntent> {
    Ok(crate::execution_owner_exit_authority::configured(&c.config)?.unwrap())
}
