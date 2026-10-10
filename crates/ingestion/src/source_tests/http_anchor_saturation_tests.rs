//! Three complete captures already exist; the fourth still needs comparison and ACK.
use super::*;
use crate::source::http_recovery::anchor_evidence_io_tests as diagnostic;
use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path};

fn snapshots(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    for pair in fs::read_dir(root).unwrap() {
        for entry in fs::read_dir(pair.unwrap().path()).unwrap() {
            let file = entry.unwrap().path();
            files.insert(
                file.strip_prefix(root).unwrap().to_str().unwrap().into(),
                fs::read(file).unwrap(),
            );
        }
    }
    files
}
fn fill(root: &Path) {
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let anchor = block(48);
    for _ in 0..3 {
        assert!(diagnostic::compare(
            Some(root.to_str().unwrap()),
            &anchor,
            &anchor,
            b"independent HTTP fixture"
        )
        .unwrap());
    }
}
#[test]
fn saturated_fourth_valid_anchor_keeps_three_full_pairs_immutable() {
    let root = tempfile::tempdir().unwrap();
    fill(root.path());
    let before = snapshots(root.path());
    let anchor = block(48);
    for _ in 0..12 {
        diagnostic::admit(
            Some(root.path().to_str().unwrap()),
            &anchor,
            &anchor,
            b"fourth and later",
        )
        .unwrap();
    }
    assert_eq!(snapshots(root.path()), before);
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 3);
}
#[test]
fn saturated_fourth_mismatch_still_refuses_exact_comparator() {
    let root = tempfile::tempdir().unwrap();
    fill(root.path());
    let before = snapshots(root.path());
    let anchor = block(48);
    let mut bad = anchor.clone();
    bad.blockhash = hash(99);
    let error = diagnostic::admit(
        Some(root.path().to_str().unwrap()),
        &anchor,
        &bad,
        b"changed fourth",
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("http_recovery_live_anchor_conflict"));
    assert_eq!(snapshots(root.path()), before);
}
#[tokio::test]
async fn saturated_fourth_valid_recovery_reaches_real_sqlite_ack() {
    route("valid", true).await;
}
#[tokio::test]
async fn saturated_fourth_mutated_recovery_never_acks_anchor() {
    route("anchor-mismatch", false).await;
}
#[tokio::test]
async fn saturated_recovery_http_and_normalization_errors_never_ack() {
    for mode in ["anchor-malformed", "anchor-envelope"] {
        route(mode, false).await;
    }
}
async fn route(mode: &'static str, valid: bool) {
    let server = servers(mode, 48, Arc::new(AtomicBool::new(false))).await;
    let mut c = config(&server);
    let root = tempfile::tempdir().unwrap();
    let evidence = root.path().join("anchor-pairs");
    fs::create_dir(&evidence).unwrap();
    fill(&evidence);
    let before = snapshots(&evidence);
    c.yellowstone_http_recovery
        .as_mut()
        .unwrap()
        .anchor_evidence_dir = Some(evidence.to_str().unwrap().into());
    let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
    let (mut db, scope) = initialize(&root.path().join("ACK.sqlite"), &c, &wallets);
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "saturated-fourth".into(),
        wallets,
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let mut anchor_ack = false;
    let mut rejected = false;
    tokio::time::timeout(Duration::from_secs(4),async {
        loop {
            match receiver.next().await {
                Ok(Some(event)) => {
                    let anchor=matches!(&event.delivery.event,DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot==48);
                    if anchor {assert!(valid,"mismatched fourth anchor reached ACK");}
                    if matches!(&event.delivery.event,DeliveryEvent::Session(SessionGap::Rejected(s)) if s=="HttpRecoveryRefused") {rejected=true;}
                    persist(&mut db,&receiver,&event,&scope);
                    if anchor {anchor_ack=true;break;}
                }
                Err(error) => {assert!(!valid,"valid fourth recovery refused: {error:#}");assert!(rejected);
                    if mode=="anchor-mismatch" {assert!(format!("{error:#}").contains("http_recovery_live_anchor_conflict"));}else {assert!(format!("{error:#}").contains("confirmed_http_recovery_refused"));}break;}
                Ok(None) => panic!("unexpected closed route"),
            }
        }
        if valid {
            while !receiver.ingress_snapshot().processing.http_recovery.caught_up_to_anchor {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }).await.unwrap();
    assert_eq!(anchor_ack, valid);
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        if valid { 48 } else { 45 }
    );
    assert_eq!(
        receiver
            .http_continuity_hold()
            .unwrap()
            .load(Ordering::Acquire),
        !valid
    );
    assert_eq!(snapshots(&evidence), before);
    receiver.stop();
}
