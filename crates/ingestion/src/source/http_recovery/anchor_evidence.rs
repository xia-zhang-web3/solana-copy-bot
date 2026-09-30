//! Private, exclusive snapshots. Three pair attempts match the probe connection cap.
use anyhow::{ensure, Context, Result};
use prost::Message;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

#[derive(Debug)]
pub(super) struct Pair {
    dir: PathBuf,
    pub name: String,
    files: Map<String, Value>,
}
struct Output {
    file: File,
    hash: Sha256,
    bytes: u64,
}
impl Output {
    fn new(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(path).context("anchor_evidence_file_create")?;
        Ok(Self {
            file,
            hash: Sha256::new(),
            bytes: 0,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all(bytes)
            .context("anchor_evidence_write")?;
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        Ok(())
    }
    fn finish(self) -> Result<Value> {
        self.file.sync_all().context("anchor_evidence_file_sync")?;
        Ok(json!({"sha256":format!("{:x}",self.hash.finalize()),"bytes":self.bytes}))
    }
}
impl Pair {
    pub fn create(root: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(root).context("anchor_evidence_root_missing")?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "anchor_evidence_root_type"
        );
        #[cfg(unix)]
        ensure!(
            metadata.permissions().mode() & 0o7777 == 0o700,
            "anchor_evidence_root_not_private"
        );
        for number in 1..=3 {
            let name = format!("pair-{number:02}");
            let dir = root.join(&name);
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            builder.mode(0o700);
            match builder.create(&dir) {
                Ok(()) => {
                    #[cfg(unix)]
                    ensure!(
                        metadata.uid() == fs::metadata(&dir)?.uid(),
                        "anchor_evidence_root_owner"
                    );
                    File::open(root)?
                        .sync_all()
                        .context("anchor_evidence_root_sync")?;
                    return Ok(Self {
                        dir,
                        name,
                        files: Map::new(),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => anyhow::bail!("anchor_evidence_pair_create"),
            }
        }
        anyhow::bail!("anchor_evidence_three_pair_cap")
    }
    fn bytes(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        let mut output = Output::new(&self.dir.join(name))?;
        output.append(bytes)?;
        self.files.insert(name.into(), output.finish()?);
        Ok(())
    }
    fn floats(&mut self, name: &str, block: &SubscribeUpdateBlock) -> Result<()> {
        let mut output = Output::new(&self.dir.join(name))?;
        for (position, tx) in block.transactions.iter().enumerate() {
            let position = u32::try_from(position).context("anchor_evidence_float_position")?;
            if let Some(meta) = &tx.meta {
                for (side, rows) in [
                    (0u8, &meta.pre_token_balances),
                    (1, &meta.post_token_balances),
                ] {
                    for (row, token) in rows.iter().enumerate() {
                        if let Some(amount) = &token.ui_token_amount {
                            let row = u32::try_from(row).context("anchor_evidence_float_row")?;
                            output.append(&position.to_le_bytes())?;
                            output.append(&[side])?;
                            output.append(&row.to_le_bytes())?;
                            output.append(&amount.ui_amount.to_bits().to_le_bytes())?;
                        }
                    }
                }
            }
        }
        self.files.insert(name.into(), output.finish()?);
        Ok(())
    }
    pub fn grpc(&mut self, grpc: &SubscribeUpdateBlock) -> Result<()> {
        // One temporary encoding buffer at a time; never clone the full block.
        self.bytes("grpc_typed.pb", &grpc.encode_to_vec())?;
        self.floats("grpc_ui_amount_bits.bin", grpc)?;
        self.sync_sides()
    }
    pub fn http_attempt(&mut self, attempt: u8, raw: &[u8]) -> Result<()> {
        self.bytes(&format!("http_attempt_{attempt}.json"), raw)?;
        self.sync_sides()
    }
    pub fn http_raw(&mut self, raw: &[u8]) -> Result<()> {
        self.bytes("http_response.json", raw)?;
        self.sync_sides()
    }
    pub fn http_normalized(&mut self, http: &SubscribeUpdateBlock) -> Result<()> {
        self.bytes("http_normalized.pb", &http.encode_to_vec())?;
        self.floats("http_ui_amount_bits.bin", http)?;
        self.sync_sides()
    }
    fn sync_sides(&self) -> Result<()> {
        File::open(&self.dir)?
            .sync_all()
            .context("anchor_evidence_sides_sync")?;
        Ok(())
    }
    pub fn complete(
        &self,
        grpc: &SubscribeUpdateBlock,
        matches: bool,
        difference: Value,
    ) -> Result<()> {
        self.publish(
            grpc,
            true,
            if matches { "MATCH" } else { "MISMATCH" },
            difference,
            Value::Null,
        )
    }
    pub fn refused(
        &self,
        grpc: &SubscribeUpdateBlock,
        stage: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        // Only private diagnostic evidence contains the bounded existing decoder context.
        let reason: String = format!("{error:#}").chars().take(2048).collect();
        self.publish(
            grpc,
            false,
            "NOT_EVALUATED",
            Value::Null,
            json!({"stage":stage,"reason":reason}),
        )
    }
    fn publish(
        &self,
        grpc: &SubscribeUpdateBlock,
        complete: bool,
        comparison: &str,
        difference: Value,
        refusal: Value,
    ) -> Result<()> {
        let manifest = json!({"schema_version":1,"complete":complete,"comparison":comparison,
            "slot":grpc.slot,"blockhash":grpc.blockhash,"parent_slot":grpc.parent_slot,
            "parent_blockhash":grpc.parent_blockhash,"transactions":grpc.transactions.len(),
            "grpc_provenance":"REENCODED_TYPED_PROTO_12_6",
            "http_provenance":"ORIGINAL_JSON_RPC_RESPONSE",
            "comparison_scope":"UNCHANGED_RUNTIME_BLOCK_EQUIVALENT",
            "transaction_order":"ORIGINAL_DELIVERY_VECTOR_ORDER",
            "float_sidecar":{"record_bytes":17,"key":"original_vector_position_u32_le,side_u8,row_u32_le",
                "value":"ieee754_u64_le","authoritative":true,
                "protobuf_default_encoding_can_omit_signed_zero":true},
            "ack_proven":false,"first_mismatch":difference,"refusal":refusal,"files":self.files});
        let mut output = Output::new(&self.dir.join("manifest.pending"))?;
        serde_json::to_writer(&mut output.file, &manifest)
            .context("anchor_evidence_manifest_write")?;
        output
            .file
            .sync_all()
            .context("anchor_evidence_manifest_sync")?;
        fs::rename(
            self.dir.join("manifest.pending"),
            self.dir.join("manifest.json"),
        )
        .context("anchor_evidence_manifest_publish")?;
        if File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .is_err()
        {
            let _ = fs::remove_file(self.dir.join("manifest.json"));
            anyhow::bail!("anchor_evidence_manifest_directory_sync");
        }
        Ok(())
    }
}
