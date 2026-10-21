//! Actual loopback transport -> association -> SQLite commit/readback ACK.
//! SYNTHETIC reconstructed bodies, no provider, real transaction or market proof.
use super::*;
mod fixture;
mod lifecycle;

fn identity_snapshot(path: &std::path::Path) -> Value {
    let sql = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        sql.query_row(
            "SELECT COUNT(*) FROM association_inbox_identities",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
    let value = sql.query_row("SELECT admission,candidate,first_session,first_sequence,conflict FROM association_inbox_identities", [], |r|
        Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,u64>(3)?,r.get::<_,bool>(4)?))).unwrap();
    let admission: AdmissionFacts = serde_json::from_str(&value.0).unwrap();
    assert!(matches!(
        admission.message_time,
        MessageTime::RecoveredBlock { .. }
    ));
    assert!(!value.4, "replayed exact Info cannot create a conflict");
    for table in [
        "native_buy_decisions",
        "native_buy_cohort_decisions",
        "copy_signals",
        "orders",
        "fills",
        "positions",
        "association_sell_work",
        "association_shadow_sell_work",
    ] {
        assert_eq!(
            sql.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0,
            "replay never dispatches business work: {table}"
        );
    }
    json!({"admission":value.0,"candidate":value.1,"first_session":value.2,"first_sequence":value.3,"conflict":value.4})
}

fn settings(server: &Servers) -> IngestionConfig {
    let mut c = config(server);
    c.yellowstone_program_ids = vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".into()];
    c.subscribe_program_ids = c.yellowstone_program_ids.clone();
    c
}

#[tokio::test]
async fn saved03_synthetic_parent_gap_transport_storage_ack() {
    let baseline = std::env::var("COPYBOT_PARENT_GAP_EXPECT_FATAL").as_deref() == Ok("1");
    let server = fixture::start("valid").await;
    let c = settings(&server);
    let wallets = fixture::wallets();
    let tmp = tempfile::tempdir().unwrap();
    let (mut db, scope) = initialize_empty(&tmp.path().join("gap.sqlite"), &c, &wallets);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "synthetic-readonly03".into(),
        wallets,
        None,
        None,
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    let mut slots = vec![];
    let mut admissions = 0;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let e = match r.next().await {
                Ok(Some(e)) => e,
                Err(error) => break Err(error),
                Ok(None) => panic!("transport ended without diagnostic"),
            };
            if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event {
                slots.push(p.observation.child.slot);
                if p.observation.child.slot == fixture::FIRST + 2 {
                    assert!(hold.load(Ordering::Acquire), "HTTP anchor not yet ACKed");
                }
            }
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                assert!(matches!(a.message_time, MessageTime::RecoveredBlock { .. }));
                admissions += 1;
            }
            persist(&mut db, &r, &e, &scope);
            if slots.last() == Some(&(fixture::FIRST + 3)) {
                break Ok(());
            }
        }
    })
    .await
    .unwrap();
    if baseline {
        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("replay_parent_gap"));
        assert_eq!(slots, [fixture::FIRST]);
        assert!(server.calls.lock().unwrap().is_empty());
        assert!(hold.load(Ordering::Acquire));
    } else {
        result.unwrap();
        assert_eq!(slots.last(), Some(&(fixture::FIRST + 3)));
        assert!(slots.contains(&(fixture::FIRST + 1)));
        assert!(slots.contains(&(fixture::FIRST + 2)));
        assert!(server
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["method"] == "getBlock" && v["params"][0] == fixture::FIRST + 2));
        let head = db.replay_checkpoint(&scope).unwrap().unwrap();
        assert_eq!(head.block.observation.child.slot, fixture::FIRST + 3);
        assert_eq!(
            admissions, 1,
            "one recovered admission, never fresh or doubled"
        );
        assert!(
            !hold.load(Ordering::Acquire),
            "exact ACK and active Reader release hold"
        );
    }
    r.stop();
    assert!(hold.load(Ordering::Acquire));
}

#[tokio::test]
async fn typed_live_parent_gap_integrity_http_and_ack_refusals_stay_held() {
    for mode in [
        "wrong-hash",
        "anchor",
        "missing",
        "null",
        "info",
        "unavailable",
        "stall",
        "live-hash",
        "incomplete",
        "ack",
        "bootstrap",
        "overlap",
        "parent",
        "cap",
    ] {
        let server = fixture::start(mode).await;
        let mut c = settings(&server);
        c.yellowstone_http_recovery.as_mut().unwrap().timeout_ms = 150;
        if mode == "cap" {
            c.yellowstone_http_recovery
                .as_mut()
                .unwrap()
                .max_response_bytes = 500;
        }
        let wallets = fixture::wallets();
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, scope) = initialize_empty(&tmp.path().join("negative.sqlite"), &c, &wallets);
        let mut r = DeliveryReceiver::start_recovering_labeled(
            &c,
            format!("synthetic-{mode}"),
            wallets,
            None,
            None,
        )
        .unwrap();
        let hold = r.http_continuity_hold().unwrap();
        let mut completed = vec![];
        let error = tokio::time::timeout(Duration::from_secs(3), async {
            loop { match r.next().await {
                Ok(Some(e)) => {
                    if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event { completed.push(p.observation.child.slot); }
                    // Deliberately withhold 573's durable readback ACK, retaining572.
                    if matches!(mode, "ack" | "bootstrap") && matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST) {
                        continue;
                    }
                    persist(&mut db, &r, &e, &scope);
                }
                Err(error) => break error,
                Ok(None) => panic!("{mode}: silently completed"),
            }}
        }).await.unwrap();
        let expected = match mode {
            "wrong-hash" | "missing" | "parent" | "live-hash" => "replay_parent_gap",
            "anchor" | "info" => "http_recovery_live_anchor_conflict",
            "null" => "http_recovery_block_unavailable_or_unconfirmed",
            "unavailable" => "http_recovery_http_status 503",
            "stall" => "timeout=true",
            "incomplete" => "replay_incomplete_block",
            "ack" | "bootstrap" => "http_recovery_checkpoint_ack_timeout",
            "overlap" => "replay_history_unavailable",
            "cap" => "http_recovery_response_limit",
            _ => unreachable!(),
        };
        assert!(
            format!("{error:#}").contains(expected),
            "{mode}: wrong refusal: {error:#}"
        );
        eprintln!("SYNTHETIC_PARENT_GAP_REFUSAL mode={mode} expected={expected} hold={} HTTP_calls={} admitted_anchor=false", hold.load(Ordering::Acquire), server.calls.lock().unwrap().len());
        assert!(hold.load(Ordering::Acquire), "{mode}: {error:#}");
        assert!(
            !completed.contains(&(fixture::FIRST + 2)),
            "{mode}: invalid anchor emitted"
        );
        if matches!(mode, "live-hash" | "incomplete" | "ack" | "bootstrap") {
            assert!(
                server.calls.lock().unwrap().is_empty(),
                "{mode}: must not recover this refusal"
            );
        }
        if mode == "ack" {
            assert_eq!(
                r.ingress_snapshot().last_durably_stored_parent_slot,
                fixture::FIRST - 1
            );
        }
        if mode == "bootstrap" {
            assert!(db.replay_checkpoint(&scope).unwrap().is_none());
        }
        assert!(
            server.calls.lock().unwrap().len() <= 7,
            "no retry on failed HTTP replay"
        );
        r.stop();
    }
}

#[tokio::test]
async fn typed_live_parent_gap_skipped_slot_and_second_gap_are_bounded() {
    for mode in ["skipped", "repeat"] {
        let server = fixture::start(mode).await;
        let c = settings(&server);
        let wallets = fixture::wallets();
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, scope) = initialize_empty(&tmp.path().join("repeat.sqlite"), &c, &wallets);
        let mut r =
            DeliveryReceiver::start_recovering_labeled(&c, mode.into(), wallets, None, None)
                .unwrap();
        let target = fixture::FIRST + if mode == "repeat" { 5 } else { 3 };
        let mut admissions = 0;
        let mut first_identity = None;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let e = r.next().await.unwrap().unwrap();
                if let DeliveryEvent::Admission(a) = &e.delivery.event {
                    assert!(matches!(a.message_time, MessageTime::RecoveredBlock { .. }));
                    admissions += 1;
                }
                persist(&mut db, &r, &e, &scope);
                if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == fixture::FIRST+1) {
                    first_identity.get_or_insert_with(|| identity_snapshot(&tmp.path().join("repeat.sqlite")));
                }
                if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == target) { break; }
            }
        }).await.unwrap();
        if mode == "skipped" {
            assert!(server.calls.lock().unwrap().is_empty());
            assert_eq!(admissions, 0);
        } else {
            assert_eq!(
                admissions, 2,
                "two transport envelopes, one immutable durable admission"
            );
            assert_eq!(
                first_identity.unwrap(),
                identity_snapshot(&tmp.path().join("repeat.sqlite"))
            );
        }
        r.stop();
    }
}
