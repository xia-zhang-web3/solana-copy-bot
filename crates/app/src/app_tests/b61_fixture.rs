use super::{
    b61_buy_fixture::proven_buy, b61_receipt_fixture::*, b61_rpc_fixture::Server,
    source_sell_ingress_fixture::Ingress, source_write_off_fixture::snapshot,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::SwapEvent;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

pub(super) struct Fixture {
    pub f: Ingress,
    pub rpc: Server,
    pub config: ExecutionConfig,
    keyfile: PathBuf,
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        let mut f = Ingress::new()?;
        let signing = ed25519_dalek::SigningKey::from_bytes(&[46; 32]);
        let payer = signing.verifying_key().to_bytes();
        proven_buy(
            &f.store,
            "b61-buy-p",
            &key(LEADER),
            f.now,
            &key(MINT),
            &key(payer),
        )?;
        f.follow_source(&key(LEADER))?;
        f.store
            .deactivate_follow_wallet(&key(LEADER), f.now, "removed")?;
        Arc::make_mut(&mut f.follow).active.clear();
        let keyfile = f.path.with_extension("synthetic-b61-key.json");
        std::fs::write(
            &keyfile,
            serde_json::to_vec(&signing.to_keypair_bytes().to_vec())?,
        )?;
        let rpc = Server::new(f.path.clone(), payer).await?;
        let mut config = super::source_write_off_fixture::config(&rpc.url);
        config.canary_wallet_pubkey = key(payer);
        config.execution_signer_pubkey = key(payer);
        config.execution_signer_keypair_path = keyfile.to_string_lossy().into();
        config.canary_kill_switch_path = f.path.with_extension("stop").to_string_lossy().into();
        config.quote_canary_enabled = true;
        config.priority_fee_canary_enabled = true;
        config.priority_fee_canary_rpc_url = rpc.url.clone();
        config.swap_instructions_dry_run_enabled = true;
        config.canary_entry_submit_enabled = false;
        config.max_submit_attempts = 3;
        config.max_confirm_seconds = 1;
        config.pretrade_max_priority_fee_lamports = 500_000;
        Ok(Self {
            f,
            rpc,
            config,
            keyfile,
        })
    }
    pub fn position(&self) -> Result<String> {
        Ok(self
            .f
            .store
            .load_execution_canary_open_position(&key(MINT))?
            .context("open position")?
            .position_id)
    }
    pub fn sell(&self, signature: &str, source: [u8; 32]) -> SwapEvent {
        let mut e = self.f.sell(signature, &key(source));
        e.token_in = key(MINT);
        e.amount_in = 7.0;
        e.amount_out = GROSS as f64 / 1_000_000_000.0;
        e.exact_amounts.as_mut().unwrap().amount_in_raw = RAW.to_string();
        e.exact_amounts.as_mut().unwrap().amount_out_raw = GROSS.to_string();
        e
    }
    pub fn signal_id(&self, event: &SwapEvent) -> String {
        format!(
            "shadow:{}:{}:sell:{}",
            event.signature, event.wallet, event.token_in
        )
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        tokio::time::timeout(
            Duration::from_secs(8),
            crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone())
                .process_tick(&self.f.store, Utc::now()),
        )
        .await
        .context("bounded runner tick")?
    }
    pub fn economics(&self) -> Result<BTreeMap<String, Vec<String>>> {
        // All economic/proof rows, not only submit counters. Traversal cursors are
        // checked separately because normal repeated ticks may advance them.
        Ok(snapshot(&self.f.conn()?, &[])?
            .into_iter()
            .filter(|(t, _)| {
                matches!(
                    t.as_str(),
                    "positions"
                        | "orders"
                        | "fills"
                        | "copy_signals"
                        | "shadow_lots"
                        | "shadow_closed_trades"
                ) || t.starts_with("execution_canary_")
                    || t.starts_with("execution_source_sell_") && !t.ends_with("cursor")
            })
            .collect())
    }
    pub fn replace_position(&self, now: DateTime<Utc>) -> Result<()> {
        self.f
            .store
            .record_execution_canary_manual_terminal_write_off(
                &key(MINT),
                "tiny",
                "fixture P to Q",
                now,
            )?;
        proven_buy(
            &self.f.store,
            "b61-buy-q",
            &key(LEADER),
            now,
            &key(MINT),
            &self.config.canary_wallet_pubkey,
        )?;
        Ok(())
    }
    pub async fn finish(&mut self) -> Result<()> {
        let ingress = self.f.finish().await;
        let rpc = self.rpc.finish().await;
        ingress?;
        rpc
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.keyfile);
        let _ = std::fs::remove_file(&self.config.canary_kill_switch_path);
    }
}
