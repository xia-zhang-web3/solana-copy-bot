//! Actual LOCAL SQLite commit/readback -> changed ACK path -> bounded wire proof.
//! No app, provider, daemon, signer, transaction sender or financial authority.
use super::super::*;
use crate::source::durable::clock_qualification_tests::local_clock::LocalClock;
use crate::source::durable::{bridge::Bridge, queue, recovery::RecoveryCursor, DeliveryReceiver};
use copybot_config::{AssociationDeliveryConfig, DeliveryBudget, IngestionConfig};
use copybot_core_types::{association_delivery::*, association_recovery::*};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};
mod fixture;
mod negatives;
mod trace;
use fixture::*;

#[tokio::test]
async fn ack_observation_real_sqlite_natural_and_managed_first_distinct_pair() {
    let _clock = LocalClock::enter();
    let trace = trace::Trace::default();
    let dispatch = tracing::Dispatch::new(trace.clone());
    let _trace = tracing::dispatcher::set_default(&dispatch);
    for mode in ["natural", "managed"] {
        let trace_start = trace.0.lock().unwrap().len();
        let tmp = tempfile::tempdir().unwrap();
        let config = config();
        let wallets = wallets();
        let scope = crate::replay_scope(&config, &wallets).unwrap();
        let path = tmp.path().join("live_runtime.db");
        let mut inbox = database(&path, &scope);
        let (receiver, mut bridge, mut reader, capture) = runtime(&config, &scope).await;
        let mut receiver = receiver;
        initial(&mut bridge, &mut receiver, &mut inbox, &scope, &capture).await;
        reader.scope.frontier(104);
        let from = if mode == "natural" {
            let error = bridge.update(120, &update(104)).await.unwrap_err();
            let gap = error
                .downcast_ref::<crate::source::durable::recovery::LiveParentGap>()
                .unwrap();
            bridge
                .restart_live_gap(121, gap, std::time::Duration::from_millis(100))
                .await
                .unwrap()
        } else {
            let task = reader.task.take().unwrap();
            task.abort();
            let _ = task.await;
            drop(reader);
            bridge.reset(121).await.unwrap();
            bridge.reset(122).await.unwrap();
            let from = bridge.begin_replay().unwrap().unwrap();
            let new_capture = bridge.begin_capture().unwrap();
            reader = live_reader(receiver.telemetry.clone(), new_capture);
            reader.scope.frontier(104);
            from
        };
        assert_eq!(from, 99);
        drain_sessions(&mut receiver, &mut inbox).await;
        for slot in 99..=104 {
            bridge.http_block(200 + slot, &block(slot)).await.unwrap();
            persist_next(&mut receiver, &mut inbox, &scope).await;
        }
        reader.clear_verified();
        let clear = receiver.telemetry.continuity.snapshot().last_clear.unwrap();
        assert_eq!(
            clear.context_at_record.checkpoint.as_ref().unwrap().slot,
            104
        );
        if mode == "natural" {
            assert_eq!(clear.reader.reader_id, 1);
            assert!(clear
                .context_at_record
                .checkpoint
                .as_ref()
                .unwrap()
                .reader_id
                .is_none());
        } else {
            assert_eq!(clear.reader.reader_id, 2);
            assert_eq!(clear.reader.session, "LOCAL-ack-source:2");
        }
        for slot in 105..=106 {
            reader.scope.frontier(slot);
            bridge.update(400 + slot, &update(slot)).await.unwrap();
            persist_next(&mut receiver, &mut inbox, &scope).await;
        }
        let (current, completed) = receiver.telemetry.continuity.ack_snapshot();
        let proof = completed.unwrap();
        assert!(proof.completed && !proof.diagnostic_invalid);
        assert_eq!(proof.samples.len(), 2);
        assert_eq!(proof.samples[0].commit_identity.slot, 105);
        assert_eq!(proof.samples[1].commit_identity.slot, 106);
        assert_eq!(current.as_ref(), Some(&proof));
        let bytes = serde_json::to_vec(&proof).unwrap();
        // A third real ACK and STOP cannot rewrite the completed first pair.
        bridge.update(507, &update(107)).await.unwrap();
        persist_next(&mut receiver, &mut inbox, &scope).await;
        assert_eq!(
            serde_json::to_vec(&receiver.telemetry.continuity.ack_snapshot().1.unwrap()).unwrap(),
            bytes
        );
        receiver.stop();
        drop(reader);
        drop(capture);
        assert_eq!(
            serde_json::to_vec(&receiver.telemetry.continuity.ack_snapshot().1.unwrap()).unwrap(),
            bytes
        );
        let events = trace.0.lock().unwrap()[trace_start..].to_vec();
        assert_eq!(
            events.len(),
            2,
            "first ACK and completed pair only; third ACK silent"
        );
        export(mode, &path, &proof, &scope, &events);
    }
}
