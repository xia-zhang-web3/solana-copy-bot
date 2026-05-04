mod client;
mod config;
mod report;
mod request;

use std::env;
use std::time::Instant;

pub use config::ProbeMode;
use report::ProbeReport;

const REASON_CONNECT_FAILED: &str = "yellowstone_connect_failed";
const REASON_CONFIG_MALFORMED: &str = "yellowstone_source_probe_config_malformed";
const REASON_CONFIG_MISSING_YELLOWSTONE: &str =
    "yellowstone_source_probe_config_missing_yellowstone";
const REASON_CONFIG_UNREADABLE: &str = "yellowstone_source_probe_config_unreadable";
const REASON_FIRST_MESSAGE_RECEIVED: &str = "yellowstone_first_message_received";
const REASON_OK: &str = "yellowstone_source_probe_subscribe_opened";

pub async fn run_from_env() -> i32 {
    let started = Instant::now();
    let report = match config::parse_args_from(env::args().skip(1)) {
        Ok(cli) => {
            if !cli.json {
                let mut report = ProbeReport::failed(
                    "yellowstone_source_probe_json_required",
                    Some("--json is required for operator output".to_string()),
                    report::elapsed_ms(started),
                );
                report.probe_mode = cli.mode.as_str().to_string();
                report
            } else {
                run(cli, started).await
            }
        }
        Err(error) => ProbeReport::failed(
            "yellowstone_source_probe_cli_error",
            Some(error.to_string()),
            report::elapsed_ms(started),
        ),
    };

    println!(
        "{}",
        serde_json::to_string(&report).expect("probe report must serialize")
    );
    report.exit_code()
}

async fn run(cli: config::Cli, started: Instant) -> ProbeReport {
    let loaded = match config::load_probe_config(&cli) {
        Ok(config) => config,
        Err(error) => {
            let mut report = ProbeReport::failed(
                classify_config_error(&error.to_string()),
                Some(report::redact_error(&error.to_string(), "", "")),
                report::elapsed_ms(started),
            );
            report.probe_mode = cli.mode.as_str().to_string();
            return report;
        }
    };

    match client::run_yellowstone_probe(&loaded, started).await {
        Ok(report) => report,
        Err(error) => {
            let mut report = ProbeReport::failed(
                REASON_CONNECT_FAILED,
                Some(report::redact_error(
                    &error.to_string(),
                    &loaded.grpc_url,
                    &loaded.x_token,
                )),
                report::elapsed_ms(started),
            );
            report.probe_mode = loaded.mode.as_str().to_string();
            report.config_loaded = true;
            report.endpoint_host_redacted = Some(report::redacted_endpoint_host(&loaded.grpc_url));
            report
        }
    }
}

fn classify_config_error(message: &str) -> &'static str {
    if message.contains(REASON_CONFIG_MISSING_YELLOWSTONE) {
        REASON_CONFIG_MISSING_YELLOWSTONE
    } else if message.contains("failed to parse TOML") {
        REASON_CONFIG_MALFORMED
    } else {
        REASON_CONFIG_UNREADABLE
    }
}

#[cfg(test)]
mod tests;
