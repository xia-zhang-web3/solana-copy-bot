use super::run::run_command;
use super::types::{Command, ExportConfig, USAGE};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use std::env;
use std::path::PathBuf;

pub fn main_entry() -> Result<()> {
    let command = parse_args(env::args().skip(1))?;
    let output = run_command(command)?;
    println!("{output}");
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command> {
    let mut config_path = None;
    let mut db_path = None;
    let mut output_path = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config_path = Some(take_path_arg(&mut args, "--config")?),
            "--db-path" => db_path = Some(take_path_arg(&mut args, "--db-path")?),
            "--output" => output_path = Some(take_path_arg(&mut args, "--output")?),
            "--scheduled" => scheduled = true,
            "--force" => force = true,
            "--json" => json = true,
            "--now" => bail!("--now is not accepted by production runtime export; use wall clock"),
            "-h" | "--help" => bail!("{USAGE}"),
            other => bail!("unknown argument {other}\n{USAGE}"),
        }
    }

    if scheduled == output_path.is_some() {
        bail!("exactly one of --scheduled or --output <path> is required\n{USAGE}");
    }
    Ok(Command::Export(ExportConfig {
        config_path: config_path.context("--config is required")?,
        db_path,
        output_path,
        scheduled,
        force,
        json,
        now: Utc::now(),
    }))
}

fn take_path_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(take_string_arg(args, flag)?))
}

fn take_string_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{flag} requires a value"))
}
