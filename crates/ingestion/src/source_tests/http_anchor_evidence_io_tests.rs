//! A concrete mid-pair write failure leaves no completed manifest or admission.
use super::{anchor_diagnostic, anchor_evidence::Pair, identity};
use anyhow::{ensure, Result};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

#[test]
fn anchor_diagnostic_partial_write_failure_preserves_existing_file_without_complete_manifest() {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let mut pair = Pair::create(root.path()).unwrap();
    let dir = root.path().join("pair-01");
    fs::write(
        dir.join("http_response.json"),
        b"exclusive existing evidence",
    )
    .unwrap();
    let block = SubscribeUpdateBlock::default();
    pair.grpc(&block).unwrap();
    let result = pair.http_raw(b"replacement");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("anchor_evidence_file_create"));
    assert!(dir.join("grpc_typed.pb").exists());
    assert!(dir.join("grpc_ui_amount_bits.bin").exists());
    assert_eq!(
        fs::read(dir.join("http_response.json")).unwrap(),
        b"exclusive existing evidence"
    );
    assert!(!dir.join("http_normalized.pb").exists());
    assert!(!dir.join("manifest.json").exists());
}

pub(crate) fn compare(
    directory: Option<&str>,
    grpc: &SubscribeUpdateBlock,
    http: &SubscribeUpdateBlock,
    raw_http: &[u8],
) -> Result<bool> {
    let Some(directory) = directory else {
        return Ok(identity::block_equivalent(http, grpc));
    };
    let mut pair = Pair::create(Path::new(directory))?;
    pair.grpc(grpc)?;
    pair.http_raw(raw_http)?;
    pair.http_normalized(http)?;
    anchor_diagnostic::compare_preserved(pair, grpc, http)
}
pub(crate) fn admit(
    directory: Option<&str>,
    grpc: &SubscribeUpdateBlock,
    http: &SubscribeUpdateBlock,
    raw_http: &[u8],
) -> Result<()> {
    ensure!(
        compare(directory, grpc, http, raw_http)?,
        "http_recovery_live_anchor_conflict"
    );
    Ok(())
}
