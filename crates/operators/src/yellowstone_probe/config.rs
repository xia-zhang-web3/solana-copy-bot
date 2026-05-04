use anyhow::{anyhow, Context, Result};
use copybot_config::{load_from_path, IngestionConfig};
use std::path::PathBuf;

use super::REASON_CONFIG_MISSING_YELLOWSTONE;

#[derive(Debug, Clone)]
pub(crate) struct Cli {
    pub(crate) config_path: PathBuf,
    pub(crate) json: bool,
    pub(crate) mode: ProbeMode,
    pub(crate) connect_timeout_ms: Option<u64>,
    pub(crate) subscribe_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProbeConfig {
    pub(crate) grpc_url: String,
    pub(crate) x_token: String,
    pub(crate) connect_timeout_ms: u64,
    pub(crate) subscribe_timeout_ms: u64,
    pub(crate) program_ids: Vec<String>,
    pub(crate) mode: ProbeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeMode {
    TransactionFilter,
    SlotsOnly,
    BlocksMeta,
    EmptyThenSend,
}

impl ProbeMode {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "transaction-filter" => Ok(Self::TransactionFilter),
            "slots-only" => Ok(Self::SlotsOnly),
            "blocks-meta" => Ok(Self::BlocksMeta),
            "empty-then-send" => Ok(Self::EmptyThenSend),
            other => Err(anyhow!(
                "--mode must be one of transaction-filter, slots-only, blocks-meta, empty-then-send; got {other}"
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TransactionFilter => "transaction-filter",
            Self::SlotsOnly => "slots-only",
            Self::BlocksMeta => "blocks-meta",
            Self::EmptyThenSend => "empty-then-send",
        }
    }
}

pub(crate) fn load_probe_config(cli: &Cli) -> Result<ProbeConfig> {
    let app_config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed to load config: {}", cli.config_path.display()))?;
    build_probe_config(
        &app_config.ingestion,
        cli.mode,
        cli.connect_timeout_ms,
        cli.subscribe_timeout_ms,
    )
}

pub(crate) fn build_probe_config(
    ingestion: &IngestionConfig,
    mode: ProbeMode,
    connect_timeout_override_ms: Option<u64>,
    subscribe_timeout_override_ms: Option<u64>,
) -> Result<ProbeConfig> {
    let source = ingestion.source.trim().to_ascii_lowercase();
    if source != "yellowstone" && source != "yellowstone_grpc" {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.source must be yellowstone_grpc"
        ));
    }

    let grpc_url = ingestion.yellowstone_grpc_url.trim();
    let x_token = ingestion.yellowstone_x_token.trim();
    if grpc_url.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_grpc_url is empty"
        ));
    }
    if grpc_url.contains("REPLACE_ME")
        || !(grpc_url.starts_with("http://") || grpc_url.starts_with("https://"))
    {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_grpc_url must be a real http(s) endpoint"
        ));
    }
    if x_token.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_x_token is empty"
        ));
    }
    if x_token.contains("REPLACE_ME") {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_x_token is not configured"
        ));
    }

    let program_ids = if !ingestion.yellowstone_program_ids.is_empty() {
        ingestion.yellowstone_program_ids.clone()
    } else if !ingestion.subscribe_program_ids.is_empty() {
        ingestion.subscribe_program_ids.clone()
    } else {
        let mut fallback = ingestion.raydium_program_ids.clone();
        fallback.extend(ingestion.pumpswap_program_ids.iter().cloned());
        fallback
    };
    if mode == ProbeMode::TransactionFilter && program_ids.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: no yellowstone program filters configured"
        ));
    }

    Ok(ProbeConfig {
        grpc_url: grpc_url.to_string(),
        x_token: x_token.to_string(),
        connect_timeout_ms: connect_timeout_override_ms
            .unwrap_or(ingestion.yellowstone_connect_timeout_ms)
            .max(1),
        subscribe_timeout_ms: subscribe_timeout_override_ms
            .unwrap_or(ingestion.yellowstone_subscribe_timeout_ms)
            .max(1),
        program_ids,
        mode,
    })
}

pub(crate) fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut json = false;
    let mut mode = ProbeMode::TransactionFilter;
    let mut connect_timeout_ms = None;
    let mut subscribe_timeout_ms = None;
    let mut iter = args.into_iter().map(Into::into);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--json" => json = true,
            "--mode" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--mode requires a value"))?;
                mode = ProbeMode::parse(&value)?;
            }
            "--connect-timeout-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--connect-timeout-ms requires a value"))?;
                connect_timeout_ms = Some(parse_positive_u64("--connect-timeout-ms", &value)?);
            }
            "--subscribe-timeout-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--subscribe-timeout-ms requires a value"))?;
                subscribe_timeout_ms = Some(parse_positive_u64("--subscribe-timeout-ms", &value)?);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    Ok(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("--config is required"))?,
        json,
        mode,
        connect_timeout_ms,
        subscribe_timeout_ms,
    })
}

fn parse_positive_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow!("{name} must be greater than zero"));
    }
    Ok(parsed)
}
