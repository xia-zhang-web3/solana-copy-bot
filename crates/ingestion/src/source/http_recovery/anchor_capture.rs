//! Explicit bounded capture allocation; saturation never substitutes for a missing pair.
use super::anchor_evidence::Pair;
use anyhow::{ensure, Context, Result};
use serde_json::Value;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::{fs, fs::Metadata, io::Read, path::Path};

#[derive(Debug)]
pub(super) enum Capture {
    Disabled,
    Full(Pair),
    Saturated,
}
impl Capture {
    pub fn begin(directory: Option<&str>) -> Result<Self> {
        match directory {
            None => Ok(Self::Disabled),
            Some(root) => Ok(match Pair::create(Path::new(root))? {
                Some(pair) => Self::Full(pair),
                None => Self::Saturated,
            }),
        }
    }
}
fn private(path: &Path, directory: bool, owner: &Metadata) -> Result<Metadata> {
    let meta = fs::symlink_metadata(path).context("anchor_evidence_retained_missing")?;
    ensure!(
        !meta.file_type().is_symlink()
            && if directory {
                meta.is_dir()
            } else {
                meta.is_file()
            },
        "anchor_evidence_retained_type"
    );
    #[cfg(unix)]
    ensure!(
        meta.uid() == owner.uid()
            && meta.permissions().mode() & 0o7777 == if directory { 0o700 } else { 0o600 },
        "anchor_evidence_retained_not_private"
    );
    Ok(meta)
}
/// Inspect bounded completion metadata, never reload/hash the retained full bodies.
/// This is an allocation guard, not a new historical comparison or ACK proof.
pub(super) fn completed(dir: &Path, owner: &Metadata) -> Result<()> {
    private(dir, true, owner)?;
    let manifest = dir.join("manifest.json");
    let meta = private(&manifest, false, owner)?;
    const LIMIT: u64 = 64 * 1024;
    ensure!(meta.len() <= LIMIT, "anchor_evidence_manifest_limit");
    let mut bytes = Vec::new();
    fs::File::open(manifest)
        .context("anchor_evidence_manifest_read")?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .context("anchor_evidence_manifest_read")?;
    ensure!(
        bytes.len() as u64 == meta.len(),
        "anchor_evidence_manifest_changed"
    );
    let value: Value =
        serde_json::from_slice(&bytes).context("anchor_evidence_manifest_invalid")?;
    ensure!(
        value["schema_version"] == 1
            && value["complete"] == true
            && matches!(
                value["comparison"].as_str(),
                Some("MATCH" | "MISMATCH" | "HTTP_SELECTED")
            )
            && value["ack_proven"] == false
            && value["refusal"].is_null()
            && if value["comparison"] == "HTTP_SELECTED" {
                value["comparison_scope"] == "TRUSTED_HTTP_COMPLETE_REPRESENTATION"
                    && value["comparison_kind"] == "TRUSTED_COMPLETE_HTTP"
                    && value["selected_input"] == "HTTP"
                    && value["hidden_payload_equality_claimed"] == false
            } else {
                value["comparison_scope"] == "VERIFIED_EXECUTION_INDEX_RUNTIME_BLOCK_EQUIVALENT"
            },
        "anchor_evidence_retained_incomplete"
    );
    let files = value["files"]
        .as_object()
        .context("anchor_evidence_manifest_files")?;
    const REQUIRED: [&str; 5] = [
        "grpc_typed.pb",
        "grpc_ui_amount_bits.bin",
        "http_response.json",
        "http_normalized.pb",
        "http_ui_amount_bits.bin",
    ];
    ensure!(
        files.len() >= 5 && files.len() <= 8,
        "anchor_evidence_manifest_files"
    );
    ensure!(
        REQUIRED.iter().all(|name| files.contains_key(*name)),
        "anchor_evidence_retained_incomplete"
    );
    for (name, record) in files {
        ensure!(
            REQUIRED.contains(&name.as_str())
                || matches!(
                    name.as_str(),
                    "http_attempt_1.json" | "http_attempt_2.json" | "http_attempt_3.json"
                ),
            "anchor_evidence_manifest_filename"
        );
        let meta = private(&dir.join(name), false, owner)?;
        ensure!(
            record["bytes"].as_u64() == Some(meta.len()),
            "anchor_evidence_retained_size"
        );
        ensure!(record["sha256"].as_str().is_some_and(|hash| hash.len() == 64
            && hash.bytes().all(|b| b.is_ascii_hexdigit())), "anchor_evidence_manifest_hash");
    }
    let count = fs::read_dir(dir)
        .context("anchor_evidence_retained_read")?
        .take(10)
        .collect::<std::io::Result<Vec<_>>>()?
        .len();
    ensure!(
        count == files.len() + 1,
        "anchor_evidence_retained_incomplete"
    );
    Ok(())
}
