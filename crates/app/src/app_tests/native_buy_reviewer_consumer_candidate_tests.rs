//! Independent reproducer: cancellation at the new external fence await.
//! Only external RPC is mocked. Replay uses actual bridge, queue and app consumer.
use crate::association_consumer::AssociationConsumer;
use crate::app_tests::{association_fixture as fixture, b135_fixture};
use crate::execution_owned_sell_rpc::fractional::transport::{Check, Parsed, Pending, Transport};
use anyhow::{bail, Result};
use copybot_config::{NativeFreshBuyConfig, OwnedSellPreparationConfig,
    PROCESSED_SLOT_FENCE_AVAILABILITY_V1, RPC_FINALIZED_OWNED_SELL_V1};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::Notify;

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct BlockFence(Arc<Notify>);
impl Transport for BlockFence {
    fn read<'a>(&'a mut self, request: Value, check: &'a mut Check<'_>) -> Pending<'a> {
        Box::pin(async move {
            check()?;
            assert_eq!(request["method"], "getGenesisHash");
            self.0.notify_one();
            std::future::pending::<Result<Value>>().await
        })
    }
}
fn rpc_response(request: Value) -> Result<Value> {
    let result = match request["method"].as_str() {
        Some("getGenesisHash") => json!("11111111111111111111111111111111"),
        Some("getSlot") => json!(99),
        other => bail!("unexpected reviewer RPC method: {other:?}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
}

#[tokio::test]
async fn reviewer_native_consumer_control_preserves_initial_session_and_fence() -> Result<()> {
    let (start, fence, identities) = run(false).await?;
    assert_eq!(start, 1);
    assert_eq!(fence, 1);
    assert!(identities > 0);
    Ok(())
}

#[tokio::test]
async fn reviewer_native_consumer_cancellation_preserves_initial_session_and_fence() -> Result<()> {
    let (start, fence, identities) = run(true).await?;
    assert!(identities > 0, "same replay source must still reach actual inbox");
    assert_eq!(start, 1, "cancelled dequeue must retain the session");
    assert_eq!(fence, 1, "subsequent poll must reacquire the fence");
    Ok(())
}

async fn run(cancel: bool) -> Result<(i64, i64, i64)> {
    let input = b135_fixture::inputs();
    let metadata: Value = serde_json::from_slice(&std::fs::read(input.join("chain.json"))?)?;
    let mut c = fixture::config(&metadata);
    let e = &mut c.execution;
    e.canary_tiny_submit_enabled = true;
    e.canary_enabled = true;
    e.quote_canary_enabled = true;
    e.swap_instructions_dry_run_enabled = true;
    e.swap_transaction_dry_run_enabled = true;
    e.canary_wallet_pubkey = metadata["our"]["signer"].as_str().unwrap().into();
    e.execution_signer_pubkey = e.canary_wallet_pubkey.clone();
    e.execution_signer_keypair_path = "never-load-reviewer-key".into();
    e.pretrade_max_priority_fee_lamports = 22000;
    e.submit_adapter_http_url = "http://127.0.0.1:1".into();
    e.tiny_experiment.id = Some("reviewer-native-consumer".into());
    e.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: PROCESSED_SLOT_FENCE_AVAILABILITY_V1.into(),
    });
    e.owned_sell_preparation = Some(OwnedSellPreparationConfig {
        policy: RPC_FINALIZED_OWNED_SELL_V1.into(), tiny_dispatch: true,
        fractional_inventory: Some("whole_wallet_parent_program_fraction_v1".into()),
        rpc_url: e.submit_adapter_http_url.clone(),
        genesis_hash: "11111111111111111111111111111111".into(),
        identity: "reviewer-native-consumer".into(),
    });
    copybot_config::validate_association_delivery(&c)?;
    let path = format!("file:reviewer-native-consumer-{}-{}?mode=memory&cache=shared",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let sql = rusqlite::Connection::open(&path)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let mut ingestion = IngestionService::with_replay(&c, receiver, "reviewer-native-session".into())?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut ingestion, &c.ingestion, &c.execution, &path).await?.unwrap();
    let mut ready = Parsed(|request: Value| async move { rpc_response(request) });
    if cancel {
        let reached = Arc::new(Notify::new());
        let mut blocked = BlockFence(reached.clone());
        {
            let poll = consumer.poll_with_transport(&store, Some(&mut blocked));
            tokio::pin!(poll);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::select! {
                    result = &mut poll => bail!("unexpected completion before cancellation: {result:?}"),
                    _ = reached.notified() => Ok(()),
                }
            }).await??;
        } // app-loop select cancellation: poll and dequeued envelope are dropped.
        assert!(consumer.pending.is_none(), "fence still awaits; no writer yet");
        let mut failed = Parsed(|_request: Value| async move {
            bail!("temporary mocked fence RPC failure")
        });
        assert!(consumer.poll_with_transport(&store, Some(&mut failed)).await.is_err());
    } else {
        consumer.poll_with_transport(&store, Some(&mut ready)).await?;
    }
    let producer = tokio::spawn(async move {
        for (offset, name) in [(1, "source"), (2, "block-100")] {
            sender.send(ReplayInput::Update {offset_ns:offset,
                payload:std::fs::read(input.join(format!("{name}.pb")))?}).await?;
        }
        sender.send(ReplayInput::End(3)).await?;
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match consumer.poll_with_transport(&store, Some(&mut ready)).await {
                Ok(()) => {},
                Err(e) if e.to_string() == "association delivery stopped" => break,
                Err(e) => return Err(e),
            }
        }
        Ok::<_, anyhow::Error>(())
    }).await??;
    producer.await??;
    let start = sql.query_row("SELECT count(*) FROM association_inbox_events WHERE delivery LIKE '%StartedContinuityUnknown%'", [], |r|r.get(0))?;
    let fence = sql.query_row("SELECT count(*) FROM native_buy_fences", [], |r|r.get(0))?;
    let identities = sql.query_row("SELECT count(*) FROM association_inbox_identities", [], |r|r.get(0))?;
    Ok((start, fence, identities))
}
