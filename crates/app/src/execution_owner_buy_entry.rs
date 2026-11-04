//! Controlled SOURCE BUY retains the session selected by the controller at BOT_READY.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{native_buy::SourceEntryBinding, SqliteStore};

pub(super) fn current(config: &ExecutionConfig, now: DateTime<Utc>) -> Result<()> {
    // Existing standalone owner lanes keep their separate contracts.
    if config.controlled_receipt_lot.is_none() {
        return Ok(());
    }
    let owner = config
        .owner_technical_buy
        .as_ref()
        .context("source_copy_entry_owner")?;
    let gate = owner
        .source_copy_exit_gate
        .as_ref()
        .context("source_copy_entry_gate")?;
    let session = gate
        .expected_native_session
        .as_deref()
        .context("source_copy_entry_session_unbound")?;
    ensure!(
        !std::path::Path::new(&gate.bot_kill_switch_path).exists(),
        "source_copy_entry_bot_stopped"
    );
    let bot = SqliteStore::open_read_only(&gate.bot_state_db_path)?;
    bot.require_source_entry_session(
        &SourceEntryBinding {
            session,
            bot_run_id: &gate.bot_run_id,
            bot_policy_identity: &gate.bot_policy_identity,
            source_wallet: &owner.wallet_pubkey,
            genesis_hash: &owner.genesis_hash,
        },
        now,
    )
}
