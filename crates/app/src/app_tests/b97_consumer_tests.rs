use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
    b97_fixture as b,
};
use anyhow::Result;
use copybot_core_types::{CopySignalRow, SwapEvent};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::association_inbox::AssociationInbox;
use std::time::Duration;

#[tokio::test]
#[ignore = "hash-bound offline actual consumer retained replay"]
async fn b97_actual_bootstrap_no_provider_messages_then_idle_and_replay_existing() -> Result<()> {
    let (db, m) = b::observed("b97-bootstrap").await?;
    let sig = m["sell"]["signature"].as_str().unwrap();
    let old = p::read(&db, &m)?;
    let before = s::snapshot(&db)?;
    let c = f::config(&m);
    let (mut consumer, tx) = f::start(&db, &c, "bootstrap").await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
    })
    .await?;
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    let saved = b::saved(&db, sig)?.unwrap();
    assert_eq!(saved.first, old.first);
    assert_eq!(s::snapshot(&db)?, before);
    // No provider input and exhausted continuation must park, not return busy Ok.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), consumer.poll(&db.store))
            .await
            .is_err()
    );
    drop(consumer);
    drop(tx);
    let (mut consumer, tx) = f::start(&db, &c, "bootstrap-replay").await?;
    let producer = tokio::spawn(async move {
        for (n, name) in ["sell", "source", "our", "chain-block"].iter().enumerate() {
            tx.send(s::update(name, n as u64 + 1)).await?;
        }
        tx.send(ReplayInput::End(5)).await?;
        Ok::<_, anyhow::Error>(())
    });
    f::drain(&mut consumer, &db).await?;
    producer.await??;
    assert_eq!(b::saved(&db, sig)?, Some(saved));
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(s::snapshot(&db)?, before);
    Ok(())
}

#[tokio::test]
#[ignore = "derived hash-bound origin Info appended after SELL in same block"]
async fn b97_actual_late_shadow_origin_anchor_wakes_existing_sell() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?)
        .parent()
        .unwrap()
        .join("b97");
    let m: serde_json::Value = serde_json::from_slice(&std::fs::read(root.join("meta.json"))?)?;
    let db = f::Db::new("b97-origin");
    let db = db?;
    s::seed(&db, &m)?;
    let o = &m["origin"];
    let text = |k: &str| o[k].as_str().unwrap().to_owned();
    let swap = SwapEvent {
        wallet: text("signer"),
        dex: text("dex_hint"),
        token_in: text("token_in"),
        token_out: text("token_out"),
        amount_in: o["amount_in"].as_f64().unwrap(),
        amount_out: o["amount_out"].as_f64().unwrap(),
        signature: text("signature"),
        slot: o["slot"].as_u64().unwrap(),
        ts_utc: "2026-09-09T00:00:00Z".parse()?,
        exact_amounts: Some(serde_json::from_value(o["exact_amounts"].clone())?),
    };
    let signal = format!(
        "shadow:{}:{}:buy:{}",
        swap.signature, swap.wallet, swap.token_out
    );
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: signal.clone(),
        wallet_id: swap.wallet.clone(),
        token: swap.token_out.clone(),
        side: "buy".into(),
        notional_sol: 0.1,
        notional_lamports: Some(copybot_core_types::Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: swap.ts_utc,
        status: "shadow_recorded".into(),
    })?;
    db.store
        .insert_shadow_buy_lot(&swap, &signal, 1.0, None, 0.1)?;
    let before = s::snapshot(&db)?;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "late-origin").await?;
    let input = tx.clone();
    let root_copy = root.clone();
    let producer = tokio::spawn(async move {
        for (n, name) in ["sell", "source", "our"].iter().enumerate() {
            input.send(s::update(name, n as u64 + 1)).await?;
        }
        input
            .send(ReplayInput::Update {
                offset_ns: 4,
                payload: std::fs::read(root_copy.join("block.pb"))?,
            })
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    b::until(&mut consumer, &db, || {
        Ok(db.sql.query_row(
            "SELECT count(*) FROM association_inbox_identities WHERE terminal IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0),
        )? == 3
            && b::idle(&db)?)
    })
    .await?;
    producer.await??;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    let sig = m["sell"]["signature"].as_str().unwrap();
    let first = p::read(&db, &m)?.first;
    tx.send(ReplayInput::Update {
        offset_ns: 5,
        payload: std::fs::read(root.join("origin.pb"))?,
    })
    .await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
    })
    .await?;
    assert_eq!(b::saved(&db, sig)?.unwrap().first, first);
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(s::snapshot(&db)?, before);
    tx.send(ReplayInput::End(6)).await?;
    f::drain(&mut consumer, &db).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "hash-bound replay startup refuses partial strict schema before intake"]
async fn b97_startup_schema_before_stream_and_legacy_execution_guards_unchanged() -> Result<()> {
    for missing in [
        "DROP TABLE ordered_source_sell_intents",
        "DROP TABLE shadow_lot_origins",
        "DROP INDEX idx_shadow_lots_pair_qty_id",
        "DELETE FROM schema_migrations WHERE version='0072_ordered_source_sell_intents.sql'",
        "DROP TRIGGER source_sell_claim_no_delete",
    ] {
        let db = f::Db::new("b97-schema")?;
        let m = s::meta()?;
        let config = f::config(&m);
        db.sql.execute_batch(missing)?;
        assert!(f::start(&db, &config, "schema").await.is_err(), "{missing}");
        assert_eq!(db.identities()?, 0);
        assert_eq!(b::count(&db, "association_inbox_events")?, 0);
    }
    let config = copybot_config::AppConfig::default();
    assert_eq!(config.ingestion.yellowstone_delivery_mode, "legacy");
    assert!(!config.execution.enabled && !config.execution.canary_tiny_submit_enabled);
    let mut config = f::config(&s::meta()?);
    for tiny in [false, true] {
        config.execution.enabled = !tiny;
        config.execution.canary_tiny_submit_enabled = tiny;
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        assert!(IngestionService::with_replay(&config, rx, "flags".into()).is_err());
    }
    let db = f::Db::new("legacy-start")?;
    let mut config = f::config(&s::meta()?);
    config.ingestion.yellowstone_delivery_mode = "legacy".into();
    // Startup early return leaves DB untouched for legacy, irrespective of strict schema.
    config.ingestion.source = "mock".into();
    let mut service = IngestionService::build_for_app(&config)?;
    assert!(crate::association_consumer::AssociationConsumer::start(
        &mut service,
        &config.ingestion,
        &db.path.to_string_lossy()
    )
    .await?
    .is_none());
    assert_eq!(b::count(&db, "association_inbox_events")?, 0);
    // Explicit API remains observation-only.
    let _ = AssociationInbox::open(&db.path, p::limits())?;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    Ok(())
}
