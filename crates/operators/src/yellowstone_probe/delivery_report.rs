use super::delivery_config::DeliveryConfig;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stage {
    Config,
    Artifact,
    Connect,
    SubscribeOpen,
    InitialSend,
    Stream,
    PingReply,
    Terminal,
}

#[derive(Debug, Serialize)]
pub(crate) struct Failure {
    pub stage: Stage,
    #[serde(rename = "code")]
    pub kind: &'static str,
    pub grpc_code: Option<i32>,
    pub provider_error_text_exported: bool,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct Counts {
    pub decoded_updates: u64,
    #[serde(rename = "transaction")]
    pub transactions: u64,
    #[serde(rename = "ping")]
    pub pings: u64,
    #[serde(rename = "pong")]
    pub pongs: u64,
    pub other: u64,
    pub ping_replies: u64,
    pub decoded_encoded_bytes: u64,
    pub stream_errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProviderCreatedAt {
    pub seconds: i64,
    pub nanos: i32,
}

#[derive(Debug, Serialize)]
pub(crate) struct Sample {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct DeliveryReport {
    pub schema: u8,
    pub mode: &'static str,
    pub stage: Stage,
    pub terminal: bool,
    pub stop_reason: &'static str,
    pub reason_class: &'static str,
    pub started_host_unix_ms: u64,
    pub observed_host_unix_ms: u64,
    pub elapsed_ms: u64,
    pub host_timestamp_basis: &'static str,
    pub elapsed_basis: &'static str,
    pub progress_sequence: u64,
    #[serde(skip)]
    pub last_progress_elapsed_ms: u64,
    pub connect_started: bool,
    pub connect_completed: bool,
    pub subscribe_started: bool,
    pub subscribe_completed: bool,
    pub initial_request_send_completed: bool,
    #[serde(rename = "update_counts")]
    pub counts: Counts,
    pub last_message_elapsed_ms: Option<u64>,
    pub last_transaction_elapsed_ms: Option<u64>,
    pub first_transaction_elapsed_ms: Option<u64>,
    pub first_transaction_slot: Option<u64>,
    pub first_transaction_provider_created_at: Option<ProviderCreatedAt>,
    pub last_transaction_slot: Option<u64>,
    pub last_transaction_provider_created_at: Option<ProviderCreatedAt>,
    pub provider_created_at_basis: &'static str,
    pub byte_counter_basis: &'static str,
    pub endpoint_sha256: Option<String>,
    pub x_token_sha256: Option<String>,
    pub request_sha256: Option<String>,
    pub failure: Option<Failure>,
    pub artifact_errors: Vec<&'static str>,
    pub samples: Vec<Sample>,
    pub sample_bytes: u64,
    pub samples_omitted_by_limits: u64,
    pub full_window_observed: bool,
    pub possible_update_loss: bool,
    pub production_green: bool,
    pub limits: DeliveryConfig,
}

pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

impl DeliveryReport {
    pub fn new(limits: &DeliveryConfig) -> Self {
        Self {
            schema: 1,
            mode: "transaction-delivery",
            stage: Stage::Config,
            terminal: false,
            stop_reason: "starting",
            reason_class: "transaction_delivery_pending",
            started_host_unix_ms: unix_ms(),
            observed_host_unix_ms: unix_ms(),
            elapsed_ms: 0,
            host_timestamp_basis: "host UTC Unix milliseconds; not provider clock",
            elapsed_basis: "process operator entry monotonic elapsed; not paired provider window",
            progress_sequence: 0,
            last_progress_elapsed_ms: 0,
            connect_started: false,
            connect_completed: false,
            subscribe_started: false,
            subscribe_completed: false,
            initial_request_send_completed: false,
            counts: Counts::default(),
            last_message_elapsed_ms: None,
            last_transaction_elapsed_ms: None,
            first_transaction_elapsed_ms: None,
            first_transaction_slot: None,
            first_transaction_provider_created_at: None,
            last_transaction_slot: None,
            last_transaction_provider_created_at: None,
            provider_created_at_basis:
                "raw SubscribeUpdate.created_at provider message timestamp; not chain block time",
            byte_counter_basis:
                "sum of decoded SubscribeUpdate.encoded_len; not transport wire bytes",
            endpoint_sha256: None,
            x_token_sha256: None,
            request_sha256: None,
            failure: None,
            artifact_errors: Vec::new(),
            samples: Vec::new(),
            sample_bytes: 0,
            samples_omitted_by_limits: 0,
            full_window_observed: false,
            possible_update_loss: false,
            production_green: false,
            limits: limits.clone(),
        }
    }
    pub fn refresh(&mut self, started: Instant) {
        self.elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        self.observed_host_unix_ms = unix_ms();
    }
    pub fn fail(&mut self, stage: Stage, kind: &'static str, grpc_code: Option<i32>) {
        if matches!(stage, Stage::Stream | Stage::PingReply) {
            self.possible_update_loss = true;
        }
        if self.failure.is_none() {
            self.failure = Some(Failure {
                stage,
                kind,
                grpc_code,
                provider_error_text_exported: false,
            });
            if self.stop_reason == "starting" {
                self.stop_reason = kind;
            }
        }
    }
    pub fn reason_class(&self) -> &'static str {
        if !self.artifact_errors.is_empty() {
            "transaction_delivery_artifact_failed"
        } else if self.failure.is_some() {
            "transaction_delivery_stage_failed"
        } else if self.stop_reason == "external_stop" && self.counts.transactions > 0 {
            "transaction_delivery_observed_external_stop"
        } else if self.stop_reason == "external_stop" {
            "transaction_delivery_no_transactions"
        } else if !self.full_window_observed {
            "transaction_delivery_incomplete_window"
        } else if self.counts.transactions == 0 {
            "transaction_delivery_no_transactions"
        } else {
            "transaction_delivery_observed"
        }
    }
}
