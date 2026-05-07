use super::{live_service_control_wrapper_contract, Config, Mode};
use anyhow::{anyhow, bail, Result};
use std::{env, path::PathBuf};

pub(super) fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

pub(super) fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut output_path: Option<PathBuf> = None;
    let mut wrapper_path: Option<PathBuf> = None;
    let mut backend_command = "systemctl".to_string();
    let mut timeout_ms = live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--path" => {
                wrapper_path = Some(PathBuf::from(parse_string_arg("--path", args.next())?))
            }
            "--backend-command" => {
                backend_command = parse_string_arg("--backend-command", args.next())?
            }
            "--timeout-ms" => timeout_ms = parse_u64_arg("--timeout-ms", args.next())?,
            "--json" => json = true,
            "--render-wrapper" => set_mode(&mut mode, Mode::RenderWrapper, "--render-wrapper")?,
            "--install-wrapper" => set_mode(&mut mode, Mode::InstallWrapper, "--install-wrapper")?,
            "--verify-wrapper" => set_mode(&mut mode, Mode::VerifyWrapper, "--verify-wrapper")?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode"))?;
    match mode {
        Mode::RenderWrapper | Mode::InstallWrapper => {
            if output_path.is_none() {
                bail!("missing required --output for wrapper render/install");
            }
        }
        Mode::VerifyWrapper => {
            if wrapper_path.is_none() {
                bail!("missing required --path for --verify-wrapper");
            }
        }
    }

    Ok(Some(Config {
        mode,
        output_path,
        wrapper_path,
        backend_command,
        timeout_ms,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => bail!("missing value for {flag}"),
    }
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .map_err(|error| anyhow!("invalid value for {flag}: {error}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes supplied; {flag} conflicts with an earlier mode");
    }
    Ok(())
}
