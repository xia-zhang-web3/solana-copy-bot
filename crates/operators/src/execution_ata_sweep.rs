use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use reqwest::blocking::Client;
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

use crate::execution_ata_sweep_rpc::{
    decode_pubkey, execute_sweep_batch, fetch_zero_token_accounts, SweepBatchReport,
    SweepCandidate, SweepSigner,
};
use crate::execution_canary_quote_pnl_context::runtime_root_from_db_path;
use crate::execution_signer_keypair_preflight::read_solana_cli_keypair_bytes;

const REASON_OK: &str = "execution_ata_sweep_loaded";
const REASON_NOOP: &str = "execution_ata_sweep_noop";
const REASON_PARTIAL: &str = "execution_ata_sweep_partial";
const REASON_ERROR: &str = "execution_ata_sweep_error";
const DEFAULT_MAX_ACCOUNTS: usize = 40;
const DEFAULT_BATCH_SIZE: usize = 8;
const HARD_MAX_ACCOUNTS: usize = 500;
const HARD_MAX_BATCH_SIZE: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config_path: PathBuf,
    pub json: bool,
    pub commit: bool,
    pub max_accounts: usize,
    pub batch_size: usize,
}

#[derive(Debug, Serialize)]
pub struct AtaSweepReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub commit: bool,
    pub wallet_pubkey: Option<String>,
    pub candidate_zero_accounts: u64,
    pub selected_accounts: u64,
    pub selected_lamports: u64,
    pub selected_sol: f64,
    pub submitted_batches: u64,
    pub confirmed_batches: u64,
    pub confirmed_closed_accounts: u64,
    pub confirmed_recovered_lamports: u64,
    pub confirmed_recovered_sol: f64,
    pub failed_accounts: u64,
    pub batches: Vec<SweepBatchReport>,
}

impl AtaSweepReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            commit: false,
            wallet_pubkey: None,
            candidate_zero_accounts: 0,
            selected_accounts: 0,
            selected_lamports: 0,
            selected_sol: 0.0,
            submitted_batches: 0,
            confirmed_batches: 0,
            confirmed_closed_accounts: 0,
            confirmed_recovered_lamports: 0,
            confirmed_recovered_sol: 0.0,
            failed_accounts: 0,
            batches: Vec::new(),
        }
    }

    fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK | REASON_NOOP => 0,
            _ => 1,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => AtaSweepReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => AtaSweepReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("ATA sweep report must serialize")
    );
    report.exit_code()
}

pub fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut json = false;
    let mut commit = false;
    let mut max_accounts = DEFAULT_MAX_ACCOUNTS;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?)),
            "--json" => json = true,
            "--commit" => commit = true,
            "--max-accounts" => {
                max_accounts = parse_usize(&next_value(&mut iter, "--max-accounts")?)?;
            }
            "--batch-size" => {
                batch_size = parse_usize(&next_value(&mut iter, "--batch-size")?)?;
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    let config_path = config_path.ok_or_else(|| anyhow!("--config is required"))?;
    if max_accounts == 0 || max_accounts > HARD_MAX_ACCOUNTS {
        anyhow::bail!("--max-accounts must be between 1 and {HARD_MAX_ACCOUNTS}");
    }
    if batch_size == 0 || batch_size > HARD_MAX_BATCH_SIZE {
        anyhow::bail!("--batch-size must be between 1 and {HARD_MAX_BATCH_SIZE}");
    }
    Ok(Cli {
        config_path,
        json,
        commit,
        max_accounts,
        batch_size,
    })
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> AtaSweepReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => AtaSweepReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<AtaSweepReport> {
    let config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed to load config {}", cli.config_path.display()))?;
    let execution = &config.execution;
    let wallet = execution.canary_wallet_pubkey.trim();
    let signer_pubkey = execution.execution_signer_pubkey.trim();
    if wallet.is_empty() || wallet != signer_pubkey {
        anyhow::bail!("canary wallet must match execution signer pubkey");
    }
    decode_pubkey(wallet).context("invalid canary wallet pubkey")?;
    let rpc_url = execution.submit_adapter_http_url.trim();
    if rpc_url.is_empty() {
        anyhow::bail!("execution.submit_adapter_http_url is required for sweep");
    }
    let runtime_root = runtime_root_from_db_path(Path::new(&config.sqlite.path));
    let signer_path = resolve_runtime_path(
        &execution.execution_signer_keypair_path,
        runtime_root.as_deref(),
    );
    let keypair = read_solana_cli_keypair_bytes(&signer_path)
        .map_err(|error| anyhow!("failed reading signer keypair: {error}"))?;
    let signer = SweepSigner::from_keypair_bytes(&keypair, signer_pubkey)?;
    let client = Client::builder()
        .timeout(StdDuration::from_millis(
            u64::from(execution.quote_canary_timeout_ms).clamp(500, 10_000),
        ))
        .build()
        .context("failed to build HTTP client")?;

    let candidates = fetch_zero_token_accounts(&client, rpc_url, wallet)?;
    let candidate_count = candidates.len() as u64;
    let selected: Vec<_> = candidates.into_iter().take(cli.max_accounts).collect();
    let batches = process_selected(
        &client,
        rpc_url,
        &signer,
        &selected,
        cli.batch_size,
        cli.commit,
    );
    let totals = totals(cli.commit, &selected, &batches);
    Ok(AtaSweepReport {
        as_of,
        reason_class: totals.reason_class,
        error: None,
        commit: cli.commit,
        wallet_pubkey: Some(wallet.to_string()),
        candidate_zero_accounts: candidate_count,
        selected_accounts: selected.len() as u64,
        selected_lamports: selected.iter().map(|candidate| candidate.lamports).sum(),
        selected_sol: lamports_to_sol(selected.iter().map(|candidate| candidate.lamports).sum()),
        submitted_batches: totals.submitted_batches,
        confirmed_batches: totals.confirmed_batches,
        confirmed_closed_accounts: totals.confirmed_closed_accounts,
        confirmed_recovered_lamports: totals.confirmed_recovered_lamports,
        confirmed_recovered_sol: lamports_to_sol(totals.confirmed_recovered_lamports),
        failed_accounts: totals.failed_accounts,
        batches,
    })
}

fn process_selected(
    client: &Client,
    rpc_url: &str,
    signer: &SweepSigner,
    selected: &[SweepCandidate],
    batch_size: usize,
    commit: bool,
) -> Vec<SweepBatchReport> {
    selected
        .chunks(batch_size)
        .map(|batch| execute_sweep_batch(client, rpc_url, signer, batch, commit))
        .collect()
}

struct SweepTotals {
    reason_class: String,
    submitted_batches: u64,
    confirmed_batches: u64,
    confirmed_closed_accounts: u64,
    confirmed_recovered_lamports: u64,
    failed_accounts: u64,
}

fn totals(commit: bool, selected: &[SweepCandidate], batches: &[SweepBatchReport]) -> SweepTotals {
    let submitted_batches = batches.iter().filter(|batch| batch.submitted).count() as u64;
    let confirmed_batches = batches
        .iter()
        .filter(|batch| batch.confirmation_status.is_some())
        .count() as u64;
    let confirmed_recovered_lamports = batches
        .iter()
        .filter(|batch| batch.confirmation_status.is_some())
        .map(|batch| batch.lamports)
        .sum();
    let confirmed_closed_accounts = batches
        .iter()
        .filter(|batch| batch.confirmation_status.is_some())
        .map(|batch| batch.token_accounts.len() as u64)
        .sum();
    let failed_accounts = batches
        .iter()
        .filter(|batch| batch.simulation_error.is_some() || batch.error.is_some())
        .map(|batch| batch.token_accounts.len() as u64)
        .sum();
    let reason_class = if selected.is_empty() {
        REASON_NOOP
    } else if failed_accounts > 0 || (commit && confirmed_closed_accounts < selected.len() as u64) {
        REASON_PARTIAL
    } else {
        REASON_OK
    };
    SweepTotals {
        reason_class: reason_class.to_string(),
        submitted_batches,
        confirmed_batches,
        confirmed_closed_accounts,
        confirmed_recovered_lamports,
        failed_accounts,
    }
}

fn resolve_runtime_path(raw_path: &str, runtime_root: Option<&Path>) -> PathBuf {
    let path = PathBuf::from(raw_path.trim());
    if path.is_absolute() {
        return path;
    }
    runtime_root
        .map(|root| root.join(path))
        .unwrap_or_else(|| PathBuf::from(raw_path.trim()))
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_usize(value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("invalid integer: {value}"))
}

fn lamports_to_sol(lamports: u64) -> f64 {
    lamports as f64 / 1_000_000_000.0
}
