use super::source_guard_rpc_fixture::Server;
use super::source_sell_ingress_fixture::Ingress;
use anyhow::{Context, Result};
use copybot_config::ExecutionConfig;
use copybot_core_types::SwapEvent;
use copybot_storage_core::ExecutionSourceSellIntent;
use ed25519_dalek::SigningKey;
use std::{path::PathBuf, sync::Arc};

pub(super) struct Fixture {
    pub f: Ingress,
    pub rpc: Server,
    pub config: ExecutionConfig,
    key: PathBuf,
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        let mut f = Ingress::new()?;
        f.buy("producer-buy", "source-a")?;
        f.follow_source("source-a")?;
        f.store
            .deactivate_follow_wallet("source-a", f.now, "removed")?;
        Arc::make_mut(&mut f.follow).active.remove("source-a");
        let signing = SigningKey::from_bytes(&[46; 32]);
        let payer = signing.verifying_key().to_bytes();
        let key = f.path.with_extension("synthetic-producer-key.json");
        std::fs::write(
            &key,
            serde_json::to_vec(&signing.to_keypair_bytes().to_vec())?,
        )?;
        let rpc = Server::new(f.path.clone(), f.now, payer).await?;
        let mut config = super::source_write_off_fixture::config(&rpc.url);
        config.canary_wallet_pubkey = bs58::encode(payer).into_string();
        config.execution_signer_pubkey = config.canary_wallet_pubkey.clone();
        config.execution_signer_keypair_path = key.to_string_lossy().into();
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
            key,
        })
    }
    pub async fn stage(&mut self, signature: &str) -> Result<ExecutionSourceSellIntent> {
        let event = self.f.sell(signature, "source-a");
        self.stage_event(&event).await
    }
    pub async fn stage_event(&mut self, event: &SwapEvent) -> Result<ExecutionSourceSellIntent> {
        self.f.send(event, true).await?; // Actual ingress and awaited durable observed ACK.
        let row = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(row) = self.f.staged(&event.signature)? {
                    return Ok::<_, anyhow::Error>(row);
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .context("staged commit deadline")??;
        // Drop completion/cache after the commit, without consuming StageCompletion.
        self.f.scheduler.source_sells = crate::source_sell_staging::SourceSellStaging::new();
        Ok(row)
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        let runner = crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone());
        runner.process_tick(&self.f.store, chrono::Utc::now()).await
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
        let _ = std::fs::remove_file(&self.key);
        let _ = std::fs::remove_file(&self.config.canary_kill_switch_path);
    }
}
