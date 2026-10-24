//! Explicit saved COPY12 durable-consumer replay. No provider, signer or submit client.
use super::Scheduler;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{CandidateGeneration, Delivery};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    ordered_sell_quote::QuoteClaimStep,
    SqliteStore,
};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

#[tokio::test]
#[ignore = "requires pinned private COPY12 stopped BOT snapshot and extracted journal"]
async fn copy12_saved_sell_actual_consumer_commit_wakes_current_quote_claim() -> Result<()> {
    let inputs = PathBuf::from(std::env::var("COPY12_INPUT_DIR")?);
    let snapshot = PathBuf::from(std::env::var("COPY12_BOT_SNAPSHOT")?);
    let source = PathBuf::from(std::env::var("COPY12_SOURCE_SNAPSHOT")?);
    ensure!(
        format!("{:x}", Sha256::digest(std::fs::read(&snapshot)?))
            == "7858b29da4e71ddfa98086ba2dde9d465fc4bff474f5c711c5e2924064a6c66d",
        "actual COPY12 BOT snapshot changed"
    );
    ensure!(
        format!("{:x}", Sha256::digest(std::fs::read(&source)?))
            == "25773fed63c3fe9b9a8800cc5fae6d9c6bad48efaf4f7ff4b2fbfc056e111556",
        "actual COPY12 SOURCE snapshot changed"
    );
    let root = std::env::temp_dir().join(format!(
        "copy12-saved-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir(&root)?;
    struct OwnedRoot(PathBuf);
    impl Drop for OwnedRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = OwnedRoot(root);
    let path = root.0.join("LOCAL-copy12-replay.sqlite");
    std::fs::copy(&snapshot, &path)?;
    let mut permissions = std::fs::metadata(&path)?.permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o600);
    std::fs::set_permissions(&path, permissions)?;
    let sql = rusqlite::Connection::open(&path)?;
    let before: (i64, i64, i64) = sql.query_row("SELECT (SELECT count(*) FROM orders),(SELECT count(*) FROM fills),(SELECT count(*) FROM execution_canary_dispatch)", [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let signature: String = sql.query_row(
        "SELECT signature FROM ordered_source_sell_intents",
        [],
        |r| r.get(0),
    )?;
    let candidate: CandidateGeneration =
        serde_json::from_slice(&std::fs::read(inputs.join("SOURCE_SELL_CANDIDATE.json"))?)?;
    let old: serde_json::Value = serde_json::from_slice(&std::fs::read(
        inputs.join("SOURCE_SELL_ORDERED_INTENT.json"),
    )?)?;
    let observed: chrono::DateTime<chrono::Utc> = chrono::DateTime::from_timestamp(
        old["first"]["app_dequeue_clock"][0]
            .as_i64()
            .context("saved dequeue seconds")?,
        old["first"]["app_dequeue_clock"][1]
            .as_u64()
            .context("saved dequeue nanos")? as u32,
    )
    .context("saved dequeue clock")?;
    // A disposable causal fork only: retain actual financial/receipt/parent witnesses,
    // remove this SELL's derived rows and replay its exact captured deliveries.
    // Reconstruct an earlier prefix in this disposable copy only. Restore the
    // exact immutable schema before any production consumer operation; no guard
    // is disabled during the replay or claim.
    let triggers: Vec<(String, String)> = sql
        .prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, _) in &triggers {
        sql.execute_batch(&format!("DROP TRIGGER \"{}\"", name.replace('"', "\"\"")))?;
    }
    for table in [
        "association_sell_dependencies",
        "association_sell_preparations",
        "association_sell_work",
        "association_parent_dependencies",
        "association_parent_work",
        "ordered_source_sell_intents",
        "source_sell_signature_claims",
    ] {
        sql.execute(&format!("DELETE FROM {table}"), [])?;
    }
    sql.execute(
        "DELETE FROM association_inbox_identities WHERE signature=?1",
        [&signature],
    )?;
    sql.execute(
        "DELETE FROM association_inbox_events WHERE sequence>=430",
        [],
    )?;
    ensure!(
        sql.query_row("SELECT count(*) FROM ordered_sell_quote_results", [], |r| r
            .get::<_, i64>(0))?
            == 0
    );
    // Resolve the container mount to its byte-pinned read-only local copy. The
    // scope's production same_scenario check deliberately excludes physical path.
    let wire: String = sql.query_row(
        "SELECT scope_json FROM controlled_receipt_lot_scope",
        [],
        |r| r.get(0),
    )?;
    let original: copybot_storage_core::controlled_receipt_lot::Scope =
        serde_json::from_str(&wire)?;
    let mut alias = original.clone();
    alias.source_state_db_path = source.to_string_lossy().into_owned();
    ensure!(original.same_scenario(&alias));
    sql.execute(
        "UPDATE controlled_receipt_lot_scope SET scope_json=?1",
        [serde_json::to_string(&alias)?],
    )?;
    for (_, schema) in &triggers {
        sql.execute_batch(schema)?;
    }
    let restored: Vec<(String, String)> = sql
        .prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    ensure!(
        restored == triggers && !restored.is_empty(),
        "all original immutable triggers must be restored before replay"
    );
    let limits = InboxLimits {
        count: 100_000,
        bytes: 256 << 20,
        busy_ms: 1000,
    };
    let mut inbox = AssociationInbox::open_ordered_sell_consumer(&path, limits)?;
    ensure!(!inbox.take_ordered_sell_wake());
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut scheduler = Scheduler {
        interval: tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(15),
            Duration::from_secs(15),
        ),
        wake: Some(wake.clone()),
        hold: Some(Arc::new(AtomicBool::new(false))),
        sell_pending: false,
    };
    let deliveries = std::fs::read_to_string(inputs.join("SOURCE_SELL_TARGET_DELIVERIES.jsonl"))?;
    let mut notifications = 0;
    for line in deliveries.lines() {
        let delivery: Delivery = serde_json::from_str(line)?;
        if delivery.sequence > 432 {
            continue;
        } // Full original checkpoint/stream is not fabricated.
        inbox.persist_at(&delivery, &candidate, observed)?;
        let hint = inbox.take_ordered_sell_wake();
        if delivery.sequence == 430 {
            ensure!(!hint, "Admission without Terminal cannot wake execution");
        }
        if hint {
            notifications += 1;
            wake.notify_one();
        }
    }
    ensure!(
        notifications == 1,
        "terminal creates one verified intent; duplicate cannot rearm"
    );
    let intent_id = format!("source-sell:{signature}");
    ensure!(
        inbox.revalidate_ordered_source_sell_intent(&intent_id)?
            == copybot_storage_core::ordered_source_sell::OrderedSellDecision::ValidatedNow
    );
    ensure!(
        sql.query_row("SELECT count(*) FROM ordered_sell_quote_results", [], |r| r
            .get::<_, i64>(0))?
            == 0,
        "persist alone does not quote"
    );
    tokio::time::timeout(Duration::from_millis(200), scheduler.ready()).await?;
    let store = SqliteStore::open(&path)?;
    let quote = store.claim_strict_sell_quote_for_owned_preparation(
        limits,
        "http://127.0.0.1:9/swap/v1",
        || observed + chrono::Duration::seconds(1),
    )?;
    let QuoteClaimStep::Claimed(claim) = quote else {
        anyhow::bail!("actual saved SELL not quoteable: {quote:?}");
    };
    // The quote names the original SOURCE BUY financial witness; the SELL
    // trigger is bound by intent_id. Preserve both identities independently.
    ensure!(claim.intent_id == intent_id && claim.binding.intent_id == intent_id);
    ensure!(
        claim.binding.source_signature
            == old["first"]["witness"]["Selected"]["source_signature"]
                .as_str()
                .context("saved SOURCE BUY witness")?
    );
    ensure!(claim.binding.raw == 988620);
    let replayed = inbox
        .load_ordered_source_sell_intent_history(&intent_id)?
        .context("replayed actual intent missing")?;
    ensure!(
        serde_json::to_value(&replayed.first)? == old["first"],
        "actual original first binding must remain exact"
    );
    let after: (i64,i64,i64) = sql.query_row("SELECT (SELECT count(*) FROM orders),(SELECT count(*) FROM fills),(SELECT count(*) FROM execution_canary_dispatch)", [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    ensure!(
        before == after && before == (1, 1, 1),
        "offline replay cannot send or alter money"
    );
    ensure!(
        sql.query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
            r.get::<_, i64>(0)
        })? == 0
    );
    if let Ok(root) = std::env::var("COPY12_WAKE_EVIDENCE_DIR") {
        let out = PathBuf::from(root).join("actual-saved-consumer");
        std::fs::create_dir_all(&out)?;
        sql.execute(
            "VACUUM main INTO ?1",
            [out.join("replayed-bot.sqlite").to_string_lossy().as_ref()],
        )?;
        std::fs::write(
            out.join("RESULT.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proof":"ACTUAL_SAVED_DURABLE_ADMISSION_TERMINAL_DUPLICATE_WITH_SAVED_PARENT_WITNESSES",
                "notifications":notifications,"original_first_binding_unchanged":true,"quote_claim_binding":claim.binding,"financial_counts_before":before,"financial_counts_after":after,
                "original_immutable_triggers_restored_before_replay":true,"physical_source_path_alias_only":true,
                "full_original_stream_replay":false,"external_requests":0,"signs":0,"sends":0
            }))?,
        )?;
    }
    Ok(())
}
