//! The explicit saturated state requires three private, complete bounded captures.
use super::{
    anchor_capture::Capture, anchor_diagnostic, anchor_evidence_io_tests as diagnostic,
    RecoveredBlock,
};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{symlink, PermissionsExt},
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

fn filled() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let block = SubscribeUpdateBlock::default();
    for _ in 0..3 {
        diagnostic::compare(Some(root.path().to_str().unwrap()), &block, &block, b"raw").unwrap();
    }
    root
}
#[test]
fn capture_saturation_refuses_incomplete_unsafe_or_unreadable_retained_pairs() {
    for case in [
        "missing_manifest",
        "incomplete",
        "invalid_json",
        "oversize",
        "missing_side",
        "size",
        "pending",
        "pair_file",
        "pair_link",
        "side_link",
        "permissions",
    ] {
        let root = filled();
        let pair = root.path().join("pair-02");
        let manifest = pair.join("manifest.json");
        match case {
            "missing_manifest" => {
                fs::remove_file(&manifest).unwrap();
            }
            "incomplete" => {
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
                v["complete"] = false.into();
                fs::write(&manifest, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            "invalid_json" => {
                fs::write(&manifest, b"{bad").unwrap();
            }
            "oversize" => {
                fs::write(&manifest, vec![b' '; 65537]).unwrap();
            }
            "missing_side" => {
                fs::remove_file(pair.join("http_normalized.pb")).unwrap();
            }
            "size" => {
                fs::write(pair.join("http_response.json"), b"different size").unwrap();
            }
            "pending" => {
                fs::write(pair.join("manifest.pending"), b"not complete").unwrap();
            }
            "pair_file" => {
                fs::remove_dir_all(&pair).unwrap();
                fs::write(&pair, b"occupied is not completed").unwrap();
            }
            "pair_link" => {
                fs::remove_dir_all(&pair).unwrap();
                symlink(root.path().join("pair-01"), &pair).unwrap();
            }
            "side_link" => {
                let side = pair.join("http_normalized.pb");
                fs::remove_file(&side).unwrap();
                symlink(pair.join("grpc_typed.pb"), side).unwrap();
            }
            "permissions" => {
                fs::set_permissions(&manifest, fs::Permissions::from_mode(0o000)).unwrap();
            }
            _ => unreachable!(),
        }
        let error = Capture::begin(Some(root.path().to_str().unwrap())).unwrap_err();
        assert!(
            format!("{error:#}").contains("anchor_evidence_"),
            "case {case}: {error:#}"
        );
        assert!(!root.path().join("pair-04").exists());
    }
}
#[test]
fn capture_configured_but_missing_state_never_becomes_saturation() {
    let root = filled();
    let block = SubscribeUpdateBlock::default();
    let mut recovered = RecoveredBlock {
        block: block.clone(),
        raw_response: vec![],
        anchor_evidence: Capture::Disabled,
    };
    assert_eq!(
        anchor_diagnostic::admit_recovered(
            Some(root.path().to_str().unwrap()),
            &block,
            &mut recovered
        )
        .unwrap_err()
        .to_string(),
        "anchor_evidence_missing"
    );
}
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
impl tracing::Subscriber for Events {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
struct Fields(BTreeMap<String, String>);
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
#[test]
fn capture_saturated_telemetry_is_bounded_and_never_claims_capture_or_ack() {
    let root = filled();
    let events = Events::default();
    let block = SubscribeUpdateBlock {
        slot: 48,
        parent_slot: 45,
        blockhash: "a".repeat(1024 * 1024),
        parent_blockhash: "b".repeat(1024 * 1024),
        ..Default::default()
    };
    tracing::subscriber::with_default(events.clone(), || {
        for matching in [true, false] {
            let mut http = block.clone();
            if !matching {
                http.slot += 1;
            }
            let mut recovered = RecoveredBlock {
                block: http,
                raw_response: vec![],
                anchor_evidence: Capture::begin(Some(root.path().to_str().unwrap())).unwrap(),
            };
            assert_eq!(
                anchor_diagnostic::admit_recovered(
                    Some(root.path().to_str().unwrap()),
                    &block,
                    &mut recovered
                )
                .is_ok(),
                matching
            );
        }
    });
    let rows = events.0.lock().unwrap();
    assert_eq!(rows.len(), 2);
    for (row, comparison) in rows.iter().zip(["\"MATCH\"", "\"MISMATCH\""]) {
        assert_eq!(row["capture_state"], "\"SATURATED\"");
        assert_eq!(row["comparison"], comparison);
        assert_eq!(row["evidence_saved"], "false");
        assert_eq!(row["ack_proven"], "false");
        assert_eq!(row["slot"], "48");
        assert_eq!(row["parent_slot"], "45");
        assert_eq!(row["blockhash_sha256"].len(), 64);
        assert_eq!(row["parent_blockhash_sha256"].len(), 64);
        assert!(row.values().map(String::len).sum::<usize>() < 256);
    }
}
