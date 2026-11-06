use super::{capture_config::CaptureArgs, config::ProbeMode};
use anyhow::{ensure, Result};
use serde::Serialize;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DeliveryConfig {
    #[serde(skip)]
    pub output: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_file: Option<PathBuf>,
    pub duration_ms: u64,
    pub progress_ms: u64,
    pub max_messages: u64,
    pub max_message_bytes: usize,
    pub max_decoded_bytes: u64,
    pub sample_transactions: u64,
    pub max_sample_bytes: u64,
}

#[derive(Default)]
pub(crate) struct DeliveryArgs(BTreeMap<String, String>);

impl DeliveryArgs {
    pub fn accepts(&self, flag: &str) -> bool {
        matches!(
            flag,
            "--stop-file"
                | "--progress-ms"
                | "--max-decoded-bytes"
                | "--sample-transactions"
                | "--max-sample-bytes"
        )
    }
    pub fn set(&mut self, flag: &str, value: Option<String>) -> Result<()> {
        ensure!(!self.0.contains_key(flag), "duplicate delivery flag");
        self.0.insert(
            flag.into(),
            value.ok_or_else(|| anyhow::anyhow!("missing delivery value"))?,
        );
        Ok(())
    }
    pub fn finish(
        self,
        mode: ProbeMode,
        capture: &mut CaptureArgs,
    ) -> Result<Option<DeliveryConfig>> {
        if mode != ProbeMode::TransactionDelivery {
            ensure!(
                self.0.is_empty(),
                "delivery flags require transaction-delivery mode"
            );
            return Ok(None);
        }
        let mut values = std::mem::take(&mut capture.0);
        values.extend(self.0);
        let number = |name: &str, default: Option<u64>, low: u64, high: u64| -> Result<u64> {
            let value = match values.get(name) {
                Some(raw) => raw
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("invalid delivery integer"))?,
                None => default
                    .ok_or_else(|| anyhow::anyhow!("required delivery limit missing: {name}"))?,
            };
            ensure!(
                (low..=high).contains(&value),
                "delivery limit outside bounds: {name}"
            );
            Ok(value)
        };
        ensure!(
            values.keys().all(|k| matches!(
                k.as_str(),
                "--output-dir"
                    | "--stop-file"
                    | "--duration-ms"
                    | "--max-messages"
                    | "--max-message-bytes"
                    | "--progress-ms"
                    | "--max-decoded-bytes"
                    | "--sample-transactions"
                    | "--max-sample-bytes"
            )),
            "unsupported delivery flag"
        );
        let output = PathBuf::from(
            values
                .get("--output-dir")
                .ok_or_else(|| anyhow::anyhow!("delivery output directory required"))?,
        );
        ensure!(
            !output.as_os_str().is_empty(),
            "delivery output directory required"
        );
        let result = DeliveryConfig {
            output,
            stop_file: values.get("--stop-file").map(PathBuf::from),
            duration_ms: number("--duration-ms", None, 1, 600_000)?,
            progress_ms: number("--progress-ms", Some(1_000), 1_000, 600_000)?,
            max_messages: number("--max-messages", Some(8_000_000), 1, 10_000_000)?,
            max_message_bytes: number("--max-message-bytes", Some(4_194_304), 1, 4_194_304)?
                as usize,
            max_decoded_bytes: number(
                "--max-decoded-bytes",
                Some(8_589_934_592),
                1,
                8_589_934_592,
            )?,
            sample_transactions: number("--sample-transactions", Some(0), 0, 8)?,
            max_sample_bytes: number("--max-sample-bytes", Some(0), 0, 16_777_216)?,
        };
        ensure!(
            (result.sample_transactions == 0) == (result.max_sample_bytes == 0),
            "sample count and byte limit must both be enabled or zero"
        );
        Ok(Some(result))
    }
}
