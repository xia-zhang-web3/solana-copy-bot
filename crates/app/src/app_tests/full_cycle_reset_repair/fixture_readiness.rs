//! Old custody unit fixtures need an explicit pre-entry BOT fence, never a BUY outcome.
use anyhow::Result;
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits}, native_buy::NativeBuyFence, SqliteStore,
};

pub(in crate::app_tests) const FIXTURE_SESSION: &str = "LOCAL-pre-source-ready";

pub(in crate::app_tests) fn ready(config: &ExecutionConfig, path: &std::path::Path) -> Result<()> {
    let mut db = SqliteStore::open(path)?;
    db.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let authority = crate::execution_technical_cohort::authority(config)?.expect("active LOCAL BOT");
    let mut inbox = AssociationInbox::open(path.to_str().unwrap(), InboxLimits { count: 100, bytes: 1<<20, busy_ms: 100 })?;
    inbox.register_technical_cohort_authority(&authority)?;
    inbox.record_native_buy_fence_epoch(&NativeBuyFence {
        session: FIXTURE_SESSION.into(), processed_slot: 99, sampled_at: Utc::now(),
        genesis_hash: "11111111111111111111111111111111".into(), policy_identity: authority.policy_identity,
    })?;
    Ok(())
}
