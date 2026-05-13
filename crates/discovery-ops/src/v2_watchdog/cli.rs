use super::render::render_human;
use super::run::run_watchdog;
use super::types::{WatchdogConfig, WatchdogState, USAGE};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use std::env;
use std::path::PathBuf;

pub fn main_entry() -> Result<i32> {
    let Some(config) = parse_args(env::args().skip(1))? else {
        println!("{USAGE}");
        return Ok(0);
    };
    let json = config.json;
    let fail_on_warn = config.fail_on_warn;
    let output = run_watchdog(config)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output).context("failed serializing watchdog output")?
        );
    } else {
        println!("{}", render_human(&output));
    }
    Ok(exit_code(output.state, fail_on_warn))
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<WatchdogConfig>> {
    let mut args = args.into_iter();
    let mut config_path = None;
    let mut db_path = None;
    let mut min_active_wallets = None;
    let mut json = false;
    let mut fail_on_warn = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(take_string_arg(&mut args, "--config")?))
            }
            "--db-path" => db_path = Some(PathBuf::from(take_string_arg(&mut args, "--db-path")?)),
            "--min-active-wallets" => {
                min_active_wallets = Some(parse_usize(&take_string_arg(
                    &mut args,
                    "--min-active-wallets",
                )?)?)
            }
            "--json" => json = true,
            "--fail-on-warn" => fail_on_warn = true,
            "--now" => bail!("--now is not accepted by production watchdog; use wall clock"),
            "-h" | "--help" => return Ok(None),
            other => bail!("unknown argument {other}\n{USAGE}"),
        }
    }

    Ok(Some(WatchdogConfig {
        config_path: config_path.context("--config is required")?,
        db_path,
        min_active_wallets,
        json,
        fail_on_warn,
        now: Utc::now(),
    }))
}

fn exit_code(state: WatchdogState, fail_on_warn: bool) -> i32 {
    match state {
        WatchdogState::Ok => 0,
        WatchdogState::Warn if fail_on_warn => 1,
        WatchdogState::Warn => 0,
        WatchdogState::Critical => 2,
    }
}

fn take_string_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    let raw = args
        .next()
        .with_context(|| format!("{flag} requires a value"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_usize(raw: &str) -> Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("invalid unsigned integer: {raw}"))
}
