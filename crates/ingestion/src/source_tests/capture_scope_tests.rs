use super::capture_scope_fixture as f;
use crate::source::{scoped_capture as c, YellowstoneParsedUpdate};
use prost::Message;
use rusqlite::params;

#[tokio::test]
async fn capture_scope_saved_325_durable_before_legacy_writer_and_dedupe() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("capture.db");
    let db = f::create(&path, 1000);
    let runtime = f::runtime(Some(&path));
    let fixtures = f::fixtures();
    if let Ok(directory) = std::env::var("CAPTURE_EXPORT_PROTO") {
        let directory = std::path::Path::new(&directory);
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(
            directory.join("saved_sell.pb"),
            f::update(&fixtures[0]).encode_to_vec(),
        )
        .unwrap();
        let buy = fixtures
            .iter()
            .find(|v| v["expected"]["side"] == "BUY")
            .unwrap();
        std::fs::write(
            directory.join("saved_buy.pb"),
            f::update(buy).encode_to_vec(),
        )
        .unwrap();
    }
    let wallets: std::collections::BTreeSet<String> = fixtures
        .iter()
        .map(|v| v["expected"]["wallet"].as_str().unwrap().into())
        .collect();
    f::request(&db, &wallets.into_iter().collect::<Vec<_>>());
    c::restore(runtime.clone()).await.unwrap();
    c::refresh(runtime.clone()).await.unwrap();
    let mut buys = 0;
    let mut sells = 0;
    for fixture in &fixtures {
        let update = f::update(fixture);
        let parsed = c::process(update.clone(), runtime.clone())
            .await
            .unwrap()
            .unwrap();
        let Some(YellowstoneParsedUpdate::Observation(raw)) = parsed else {
            panic!("decode {}", fixture["signature"]);
        };
        assert_eq!(raw.signature, fixture["signature"].as_str().unwrap());
        assert_eq!(raw.signer, fixture["expected"]["wallet"].as_str().unwrap());
        let (stage, payload): (String, String) = db
            .query_row(
                "SELECT stage,event_json FROM capture_events WHERE signature=?",
                [&raw.signature],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(stage, "DURABLE");
        let event: copybot_core_types::SwapEvent = serde_json::from_str(&payload).unwrap();
        assert_eq!(event.slot, raw.slot);
        let mint = fixture["expected"]["mint"].as_str().unwrap();
        if fixture["expected"]["side"] == "BUY" {
            buys += 1;
            assert_eq!(event.token_out, mint);
        } else {
            sells += 1;
            assert_eq!(event.token_in, mint);
        }
        // Same actual handler runs twice; capture signature dedupe is durable.
        c::process(update, runtime.clone()).await.unwrap().unwrap();
    }
    let count: i64 = db
        .query_row("SELECT count(*) FROM capture_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, fixtures.len() as i64);
    assert_eq!(count, 325);
    assert!(buys > 0 && sells > 258);
    println!("actual capture replay: {count} distinct DURABLE; BUY={buys} SELL={sells}; 325 duplicate deliveries; historical receive UNKNOWN");
}

#[tokio::test]
async fn capture_scope_publication_ack_demotion_restart_pending_and_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("capture.db");
    let db = f::create(&path, 1000);
    let runtime = f::runtime(Some(&path));
    let fixtures = f::fixtures();
    let item = &fixtures[0];
    let wallet = item["expected"]["wallet"].as_str().unwrap();
    c::restore(runtime.clone()).await.unwrap();
    f::request(&db, &[wallet.into()]);
    // Publication is pending: frame passes real parser, cannot become an admitted BUY.
    c::process(f::update(item), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT state FROM capture_requests", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "PENDING"
    );
    c::refresh(runtime.clone()).await.unwrap();
    c::process(f::update(item), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    db.execute(
        "INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(1,?,?)",
        params![wallet, item["expected"]["mint"].as_str().unwrap()],
    )
    .unwrap();
    f::request(&db, &[]);
    c::refresh(runtime.clone()).await.unwrap();
    // Demoted wallet retains its unresolved pin, even for undecodable transaction.
    let other = fixtures
        .iter()
        .find(|x| x["expected"]["wallet"] == wallet && x["signature"] != item["signature"])
        .unwrap();
    let mut rejected = f::update(other);
    rejected.created_at = None;
    c::process(rejected.clone(), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT stage FROM capture_events WHERE signature=?",
            [other["signature"].as_str().unwrap()],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "REJECTED"
    );
    // Simulate actual crash boundary after received commit, before parse/finish.
    db.execute(
        "UPDATE capture_events SET stage='RECEIVED',event_json=NULL WHERE seq=2",
        [],
    )
    .unwrap();
    drop(runtime);
    let restarted = f::runtime(Some(&path));
    assert!(
        c::refresh(restarted.clone()).await.is_err(),
        "cannot ack before restoring pending"
    );
    // A failed refresh fences that consumer; use a new process-equivalent instance.
    let restarted = f::runtime(Some(&path));
    c::restore(restarted.clone()).await.unwrap();
    assert_eq!(
        db.query_row("SELECT stage FROM capture_events WHERE seq=2", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap(),
        "REJECTED"
    );
    assert_eq!(
        db.query_row("SELECT state FROM capture_obligations", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "PENDING"
    );
    c::process(rejected, restarted).await.unwrap().unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn capture_scope_capacity_fails_closed_and_default_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("capture.db");
    let db = f::create(&path, 1);
    let runtime = f::runtime(Some(&path));
    let fixtures = f::fixtures();
    let wallet = fixtures[0]["expected"]["wallet"].as_str().unwrap();
    f::request(&db, &[wallet.into()]);
    c::restore(runtime.clone()).await.unwrap();
    c::refresh(runtime.clone()).await.unwrap();
    let items: Vec<_> = fixtures
        .iter()
        .filter(|x| x["expected"]["wallet"] == wallet)
        .take(2)
        .collect();
    c::process(f::update(items[0]), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(c::process(f::update(items[1]), runtime.clone())
        .await
        .is_err());
    assert!(runtime.capture.as_ref().unwrap().healthy().is_err());
    assert_eq!(
        db.query_row("SELECT status FROM capture_meta", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "failed"
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(c::process(f::update(items[1]), f::runtime(None))
        .await
        .unwrap()
        .unwrap()
        .is_some());
    let cfg = copybot_config::AppConfig::default();
    assert!(!cfg.execution.enabled && !cfg.execution.canary_tiny_submit_enabled);
}

#[tokio::test]
async fn capture_scope_restart_preserves_invalid_timestamp_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("capture.db");
    let db = f::create(&path, 10);
    let item = &f::fixtures()[0];
    let mut update = f::update(item);
    update.created_at.as_mut().unwrap().nanos = 1_000_000_000;
    let mut store = copybot_storage_core::capture_scope::CaptureStore::open(&path).unwrap();
    store.start().unwrap();
    f::request(&db, &[item["expected"]["wallet"].as_str().unwrap().into()]);
    store.accept_pending(1.0).unwrap();
    store
        .receive(
            Some(item["signature"].as_str().unwrap()),
            item["expected"]["wallet"].as_str().unwrap(),
            item["slot"].as_u64().unwrap(),
            &update.encode_to_vec(),
            "invalid-time-fixture",
            2.0,
            Some(3.0),
        )
        .unwrap();
    drop(store);
    let runtime = f::runtime(Some(&path));
    c::restore(runtime.clone()).await.unwrap();
    assert_eq!(
        db.query_row("SELECT stage FROM capture_events", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "REJECTED"
    );
    assert!(db
        .query_row("SELECT event_json FROM capture_events", [], |r| r
            .get::<_, Option<String>>(0))
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn capture_scope_duplicate_delivery_metadata_keeps_original_receipt() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("capture.db");
    let db = f::create(&path, 10);
    let runtime = f::runtime(Some(&path));
    let item = &f::fixtures()[0];
    f::request(&db, &[item["expected"]["wallet"].as_str().unwrap().into()]);
    c::restore(runtime.clone()).await.unwrap();
    c::refresh(runtime.clone()).await.unwrap();
    let original = f::update(item);
    let mut duplicate = original.clone();
    duplicate.created_at.as_mut().unwrap().nanos += 1;
    duplicate.filters = vec!["different-delivery-filter".into()];
    c::process(original.clone(), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    c::process(duplicate, runtime.clone())
        .await
        .unwrap()
        .unwrap();
    runtime.capture.as_ref().unwrap().healthy().unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT raw FROM capture_events", [], |r| r
            .get::<_, Vec<u8>>(0))
            .unwrap(),
        original.encode_to_vec()
    );
}

#[test]
fn capture_scope_config_explicitly_forbids_each_financial_flag() {
    let mut cfg = copybot_config::AppConfig::default();
    cfg.ingestion.source = "yellowstone_grpc".into();
    cfg.ingestion.capture_scope_db = Some("separate-capture.db".into());
    assert!(copybot_config::validate_association_delivery(&cfg).is_ok());
    cfg.execution.enabled = true;
    assert!(copybot_config::validate_association_delivery(&cfg).is_err());
    cfg.execution.enabled = false;
    cfg.execution.canary_tiny_submit_enabled = true;
    assert!(copybot_config::validate_association_delivery(&cfg).is_err());
    cfg.execution.canary_tiny_submit_enabled = false;
    cfg.execution.tiny_experiment.activate = true;
    assert!(copybot_config::validate_association_delivery(&cfg).is_err());
    cfg.execution.tiny_experiment.activate = false;
    cfg.ingestion.source = "mock".into();
    assert!(copybot_config::validate_association_delivery(&cfg).is_err());
}
