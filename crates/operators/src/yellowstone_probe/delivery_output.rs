use super::{
    capture_hash::sha256,
    config::ProbeConfig,
    delivery_report::{DeliveryReport, Sample},
};
use base64::Engine;
use serde::Serialize;
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::PathBuf,
};
use yellowstone_grpc_proto::{
    prelude::{SubscribeRequest, SubscribeUpdate},
    prost::Message,
};

const METADATA_BYTES: usize = 65_536;

pub(crate) struct DeliveryFiles {
    root: PathBuf,
}

fn write_new(path: PathBuf, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn metadata(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() >= METADATA_BYTES {
        return Err(io::Error::other("delivery_metadata_limit"));
    }
    bytes.push(b'\n');
    Ok(bytes)
}

impl DeliveryFiles {
    pub fn create(root: PathBuf) -> io::Result<Self> {
        fs::create_dir(&root)?;
        Ok(Self { root })
    }
    pub fn request(
        &self,
        config: &ProbeConfig,
        request: &SubscribeRequest,
        report: &mut DeliveryReport,
    ) -> io::Result<()> {
        if request.encoded_len() > METADATA_BYTES {
            return Err(io::Error::other("delivery_request_limit"));
        }
        let bytes = request.encode_to_vec();
        report.request_sha256 = Some(sha256(&bytes));
        report.endpoint_sha256 = Some(sha256(config.grpc_url.as_bytes()));
        report.x_token_sha256 = Some(sha256(config.x_token.as_bytes()));
        let value = json!({"schema":1,"commitment":"confirmed","program_ids":config.program_ids,
            "transactions":{"copybot-swaps":{"vote":false,"failed":false,"signature":null,
                "account_include":config.program_ids,"account_exclude":[],"account_required":[],
                "cuckoo_account_include":null,"token_accounts":null}},
            "accounts":{},"slots":{},"transactions_status":{},"blocks":{},"blocks_meta":{},
            "entry":{},"accounts_data_slice":[],"ping":null,"from_slot":null,
            "subscribe_request_base64":base64::engine::general_purpose::STANDARD.encode(bytes),
            "request_sha256":report.request_sha256,"endpoint_sha256":report.endpoint_sha256,
            "x_token_sha256":report.x_token_sha256,"wire_byte_identity_with_hashset_order_claimed":false});
        write_new(self.root.join("request.json"), &metadata(&value)?)
    }
    pub fn progress(&self, report: &mut DeliveryReport) -> io::Result<()> {
        report.progress_sequence += 1;
        report.last_progress_elapsed_ms = report.elapsed_ms;
        let bytes = metadata(report)?;
        let temporary = self.root.join("progress.tmp");
        write_new(temporary.clone(), &bytes)?;
        fs::rename(temporary, self.root.join("progress.json"))
    }
    pub fn final_report(&self, report: &DeliveryReport) -> io::Result<()> {
        write_new(self.root.join("final.json"), &metadata(report)?)
    }
    pub fn sample(&self, message: &SubscribeUpdate, report: &mut DeliveryReport) -> io::Result<()> {
        if report.limits.sample_transactions == 0 {
            return Ok(());
        }
        let size = message.encoded_len() as u64;
        if report.samples.len() as u64 >= report.limits.sample_transactions
            || report.sample_bytes.saturating_add(size) > report.limits.max_sample_bytes
        {
            report.samples_omitted_by_limits += 1;
            return Ok(());
        }
        let bytes = message.encode_to_vec();
        let name = format!("transaction-{:06}.pb", report.samples.len() + 1);
        write_new(self.root.join(&name), &bytes)?;
        report.sample_bytes += size;
        report.samples.push(Sample {
            file: name,
            bytes: size,
            sha256: sha256(&bytes),
        });
        Ok(())
    }
}
