//! Read-only SOURCE entry proof. A Reset revokes the pinned BOT session.
use super::{cohort, epoch_selection};
use crate::SqliteStore;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;

pub struct SourceEntryBinding<'a> {
    pub session: &'a str,
    pub bot_run_id: &'a str,
    pub bot_policy_identity: &'a str,
    pub source_wallet: &'a str,
    pub genesis_hash: &'a str,
}

impl SqliteStore {
    /// Read authority, active session and intact epoch chain in one committed
    /// snapshot. This cannot renew a clock, change session or grant a BUY.
    pub fn require_source_entry_session(
        &self,
        binding: &SourceEntryBinding<'_>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let active: Option<String> = tx
            .query_row(
                "SELECT active_session FROM native_buy_session_state WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        ensure!(
            active.as_deref() == Some(binding.session),
            "source_copy_entry_session_changed"
        );
        let authority = cohort::load(&tx)?.context("source_copy_entry_authority_missing")?;
        ensure!(
            authority.run_id == binding.bot_run_id
                && authority.policy_identity == binding.bot_policy_identity
                && authority.wallet_ids == [binding.source_wallet.to_owned()]
                && authority.activated_at <= now
                && now < authority.deadline,
            "source_copy_entry_authority_changed"
        );
        let epoch = epoch_selection::select(&tx, binding.session, u64::MAX, now, &authority)?
            .context("source_copy_entry_fence_not_current")?;
        ensure!(
            epoch.genesis == binding.genesis_hash,
            "source_copy_entry_genesis_changed"
        );
        tx.rollback()?;
        Ok(())
    }
}
