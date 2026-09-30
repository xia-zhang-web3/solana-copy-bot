//! Changed pre-gate evidence only; local transports and committed SQLite ACKs.
use super::*;
use crate::source::http_recovery::{anchor_evidence_io_tests as anchor_diagnostic, identity};
use prost::Message as _;
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

fn private() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
fn manifest(root: &Path, number: u8) -> Value {
    serde_json::from_slice(&fs::read(root.join(format!("pair-{number:02}/manifest.json"))).unwrap())
        .unwrap()
}
pub(super) fn transaction_raw() -> Value {
    let mut v = raw(48);
    let key = bs58::encode([1; 32]).into_string();
    v["transactions"] = json!((1..=3).map(|n| json!({"version":"legacy",
        "transaction":{"signatures":[bs58::encode([n;64]).into_string()],"message":{
            "accountKeys":[key],"header":{"numRequiredSignatures":1,"numReadonlySignedAccounts":0,
                "numReadonlyUnsignedAccounts":0},"recentBlockhash":key,"instructions":[]}},
        "meta":{"err":null,"fee":0,"preBalances":[1],"postBalances":[1],"innerInstructions":[],
            "logMessages":[],"preTokenBalances":[],"postTokenBalances":[],"rewards":[],
            "loadedAddresses":{"writable":[],"readonly":[]}}
    })).collect::<Vec<_>>());
    v
}
pub(super) fn unordered_block() -> SubscribeUpdateBlock {
    let mut b =
        crate::source::http_recovery::normalize_confirmed_http_block(48, &transaction_raw())
            .unwrap();
    b.transactions.rotate_right(1);
    b
}
#[test]
fn anchor_diagnostic_scalar_and_nested_float_preserve_exact_typed_sides() {
    let dir = private();
    let grpc = block(48);
    let mut http = grpc.clone();
    http.parent_slot += 1;
    assert!(!anchor_diagnostic::compare(
        Some(dir.path().to_str().unwrap()),
        &grpc,
        &http,
        b"raw independent HTTP"
    )
    .unwrap());
    let m = manifest(dir.path(), 1);
    assert_eq!(
        m["first_mismatch"],
        json!({"path":"parent_slot","grpc":45,"http":46})
    );
    assert_eq!(m["grpc_provenance"], "REENCODED_TYPED_PROTO_12_6");
    assert!(!m["ack_proven"].as_bool().unwrap());
    assert_eq!(
        fs::read(dir.path().join("pair-01/http_response.json")).unwrap(),
        b"raw independent HTTP"
    );
    for (file, metadata) in m["files"].as_object().unwrap() {
        let p = dir.path().join("pair-01").join(file);
        let bytes = fs::read(&p).unwrap();
        assert_eq!(bytes.len() as u64, metadata["bytes"].as_u64().unwrap());
        use sha2::{Digest, Sha256};
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), metadata["sha256"]);
        assert_eq!(fs::metadata(p).unwrap().permissions().mode() & 0o777, 0o600);
    }
    let mut grpc = unordered_block();
    grpc.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances
        .push(TokenBalance {
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: -0.0,
                ..Default::default()
            }),
            ..Default::default()
        });
    let mut http = grpc.clone();
    http.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = 0.0;
    assert!(!identity::block_equivalent(&grpc, &http));
    assert!(!anchor_diagnostic::compare(
        Some(dir.path().to_str().unwrap()),
        &grpc,
        &http,
        b"float HTTP"
    )
    .unwrap());
    let m = manifest(dir.path(), 2);
    assert_eq!(
        m["first_mismatch"]["path"],
        "transactions[2].meta.pre_token_balances[0].ui_token_amount.ui_amount"
    );
    assert_eq!(
        m["first_mismatch"]["grpc"],
        json!({"bits":"0x8000000000000000","decimal":"-0"})
    );
    assert_eq!(
        m["first_mismatch"]["http"],
        json!({"bits":"0x0000000000000000","decimal":"0"})
    );
    let restored = fs::read(dir.path().join("pair-02/grpc_ui_amount_bits.bin")).unwrap();
    assert_eq!(restored.len(), 17);
    assert_eq!(&restored[..9], &[0u8; 9]);
    assert_eq!(
        u64::from_le_bytes(restored[9..17].try_into().unwrap()),
        (-0.0f64).to_bits()
    );
    let http_bits = fs::read(dir.path().join("pair-02/http_ui_amount_bits.bin")).unwrap();
    assert_eq!(
        u64::from_le_bytes(http_bits[9..17].try_into().unwrap()),
        0.0f64.to_bits()
    );
    let encoded = fs::read(dir.path().join("pair-02/grpc_typed.pb")).unwrap();
    let decoded = SubscribeUpdateBlock::decode(encoded.as_slice()).unwrap();
    assert_eq!(
        decoded.transactions[0]
            .meta
            .as_ref()
            .unwrap()
            .pre_token_balances[0]
            .ui_token_amount
            .as_ref()
            .unwrap()
            .ui_amount
            .to_bits(),
        0
    );
}
#[test]
fn anchor_diagnostic_unchanged_predicate_private_exclusive_and_three_pair_cap() {
    let dir = private();
    let grpc = block(48);
    let mut http = grpc.clone();
    http.rewards = None;
    assert!(identity::block_equivalent(&grpc, &http));
    assert!(anchor_diagnostic::compare(None, &grpc, &http, b"").unwrap());
    for number in 1..=3 {
        assert!(anchor_diagnostic::compare(
            Some(dir.path().to_str().unwrap()),
            &grpc,
            &http,
            b"bounded raw"
        )
        .unwrap());
        assert_eq!(manifest(dir.path(), number)["comparison"], "MATCH");
    }
    let before = fs::read(dir.path().join("pair-01/manifest.json")).unwrap();
    let e = anchor_diagnostic::admit(
        Some(dir.path().to_str().unwrap()),
        &grpc,
        &http,
        b"replacement",
    )
    .unwrap_err();
    assert!(e.to_string().contains("three_pair_cap"));
    assert_eq!(
        before,
        fs::read(dir.path().join("pair-01/manifest.json")).unwrap()
    );
    let unsafe_dir = private();
    fs::set_permissions(unsafe_dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        anchor_diagnostic::admit(Some(unsafe_dir.path().to_str().unwrap()), &grpc, &http, b"")
            .is_err()
    );
    let links = private();
    let link = links.path().join("symlink");
    std::os::unix::fs::symlink(dir.path(), &link).unwrap();
    assert!(anchor_diagnostic::admit(Some(link.to_str().unwrap()), &grpc, &http, b"").is_err());
}
#[tokio::test]
async fn anchor_diagnostic_io_and_parse_failure_never_ack_live_anchor() {
    for mode in ["valid", "anchor-malformed", "anchor-envelope"] {
        let server = servers(mode, 48, Arc::new(AtomicBool::new(false))).await;
        let mut c = config(&server);
        let tmp = private();
        let evidence = tmp.path().join("anchor-pairs");
        if mode != "valid" {
            fs::create_dir(&evidence).unwrap();
            fs::set_permissions(&evidence, fs::Permissions::from_mode(0o700)).unwrap();
        }
        c.yellowstone_http_recovery
            .as_mut()
            .unwrap()
            .anchor_evidence_dir = Some(evidence.to_str().unwrap().into());
        let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
        let (mut db, scope) = initialize(&tmp.path().join("state.sqlite"), &c, &wallets);
        let mut r = DeliveryReceiver::start_recovering_labeled(
            &c,
            format!("diagnostic-{mode}"),
            wallets,
            None,
            db.replay_checkpoint(&scope).unwrap(),
        )
        .unwrap();
        let hold = r.http_continuity_hold().unwrap();
        tokio::time::timeout(Duration::from_secs(4),async {
            let mut rejected=false;
            loop { match r.next().await {
                Ok(Some(e))=> { if matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==48) {panic!("failed diagnostic anchor advanced ACK");}
                    persist(&mut db,&r,&e,&scope);
                    if matches!(&e.delivery.event,DeliveryEvent::Session(SessionGap::Rejected(s))if s=="HttpRecoveryRefused") {rejected=true;}
                }
                Err(e)=> {assert!(rejected);assert!(e.to_string().contains("confirmed_http_recovery_refused"));break;}
                Ok(None)=>panic!("refusal silently accepted"),
            }}
        }).await.unwrap();
        assert!(hold.load(Ordering::Acquire));
        assert_eq!(
            db.replay_checkpoint(&scope)
                .unwrap()
                .unwrap()
                .block
                .observation
                .child
                .slot,
            45
        );
        assert!(
            !r.ingress_snapshot()
                .processing
                .http_recovery
                .caught_up_to_anchor
        );
        r.stop();
        if mode == "anchor-malformed" {
            let m = manifest(&evidence, 1);
            assert_eq!(m["comparison"], "NOT_EVALUATED");
            assert_eq!(m["complete"], false);
            assert_eq!(m["refusal"]["stage"], "http_normalization");
            let dir = evidence.join("pair-01");
            let raw: Value =
                serde_json::from_slice(&fs::read(dir.join("http_response.json")).unwrap()).unwrap();
            assert_eq!(raw["result"]["rewards"], Value::Null);
            assert!(dir.join("grpc_typed.pb").exists());
            assert!(dir.join("grpc_ui_amount_bits.bin").exists());
            assert!(dir.join("http_attempt_1.json").exists());
            assert!(!dir.join("http_normalized.pb").exists());
        } else if mode == "anchor-envelope" {
            let m = manifest(&evidence, 1);
            assert_eq!(m["complete"], false);
            assert_eq!(m["comparison"], "NOT_EVALUATED");
            assert!(m["first_mismatch"].is_null());
            assert_eq!(m["refusal"]["stage"], "http_response");
            let dir = evidence.join("pair-01");
            assert_eq!(
                fs::read(dir.join("http_attempt_1.json")).unwrap(),
                b"{invalid original JSON"
            );
            assert!(dir.join("grpc_typed.pb").exists());
            assert!(dir.join("grpc_ui_amount_bits.bin").exists());
            assert!(!dir.join("http_normalized.pb").exists());
        } else {
            assert!(!evidence.exists());
        }
    }
}

#[tokio::test]
async fn anchor_diagnostic_matching_unordered_pair_precedes_durable_anchor_ack() {
    let server = servers("anchor-order", 48, Arc::new(AtomicBool::new(false))).await;
    let mut c = config(&server);
    let tmp = private();
    let evidence = tmp.path().join("anchor-pairs");
    fs::create_dir(&evidence).unwrap();
    fs::set_permissions(&evidence, fs::Permissions::from_mode(0o700)).unwrap();
    c.yellowstone_http_recovery
        .as_mut()
        .unwrap()
        .anchor_evidence_dir = Some(evidence.to_str().unwrap().into());
    let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
    let (mut db, scope) = initialize(&tmp.path().join("state.sqlite"), &c, &wallets);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "diagnostic-match".into(),
        wallets,
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(4),async {
        loop {
            let e=r.next().await.unwrap().unwrap();
            let anchor=matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==48);
            if anchor {
                let m=manifest(&evidence,1);
                assert_eq!(m["comparison"],"MATCH");
                assert_eq!(m["complete"],true);
                assert!(m["first_mismatch"].is_null());
                assert!(evidence.join("pair-01/http_attempt_1.json").exists());
                for (file,order) in [("grpc_typed.pb",vec![2,0,1]),("http_normalized.pb",vec![0,1,2])] {
                    let saved=SubscribeUpdateBlock::decode(fs::read(evidence.join("pair-01").join(file)).unwrap().as_slice()).unwrap();
                    assert_eq!(saved.transactions.iter().map(|tx|tx.index).collect::<Vec<_>>(),order);
                }
            }
            persist(&mut db,&r,&e,&scope);
            if anchor { break; }
        }
        while !r.ingress_snapshot().processing.http_recovery.caught_up_to_anchor {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        48
    );
    assert!(!r.http_continuity_hold().unwrap().load(Ordering::Acquire));
    r.stop();
}
