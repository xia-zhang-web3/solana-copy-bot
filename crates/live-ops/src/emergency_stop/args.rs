use super::{Config, Mode, DEFAULT_OPERATOR_EMERGENCY_STOP_PATH, EMERGENCY_STOP_FILE_ENV, USAGE};
use anyhow::{anyhow, bail, Result};
use std::{env, path::PathBuf};

pub(super) fn parse_args() -> Result<Config> {
    parse_args_from(env::args().skip(1))
}

pub(super) fn parse_args_from<I>(args: I) -> Result<Config>
where
    I: IntoIterator<Item = String>,
{
    let mut mode: Option<Mode> = None;
    let mut path: Option<PathBuf> = None;
    let mut reason: Option<String> = None;
    let mut confirm_clear: Option<String> = None;
    let mut force = false;
    let mut json = false;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--status" => set_mode(&mut mode, Mode::Status)?,
            "--activate" => set_mode(&mut mode, Mode::Activate)?,
            "--clear" => set_mode(&mut mode, Mode::Clear)?,
            "--path" => path = Some(PathBuf::from(parse_string_arg("--path", args.next())?)),
            "--reason" => reason = Some(parse_string_arg("--reason", args.next())?),
            "--confirm-clear" => {
                confirm_clear = Some(parse_string_arg("--confirm-clear", args.next())?)
            }
            "--force" => force = true,
            "--json" => json = true,
            "--help" | "-h" => {
                bail!("{USAGE}");
            }
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("one mode is required\n{USAGE}"))?;
    if force && mode != Mode::Activate {
        bail!("--force is only valid with --activate");
    }
    if reason.is_some() && mode != Mode::Activate {
        bail!("--reason is only valid with --activate");
    }
    if confirm_clear.is_some() && mode != Mode::Clear {
        bail!("--confirm-clear is only valid with --clear");
    }
    let path = resolve_path(path)?;
    if mode == Mode::Activate && reason.as_deref().is_none_or(str::is_empty) {
        bail!("copybot_operator_emergency_stop_activate_missing_reason: --reason is required");
    }

    Ok(Config {
        mode,
        path,
        reason,
        force,
        confirm_clear,
        json,
    })
}

fn set_mode(mode: &mut Option<Mode>, next: Mode) -> Result<()> {
    if mode.replace(next).is_some() {
        bail!("exactly one mode is allowed\n{USAGE}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn resolve_path(override_path: Option<PathBuf>) -> Result<PathBuf> {
    let path = override_path
        .or_else(|| env::var(EMERGENCY_STOP_FILE_ENV).ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OPERATOR_EMERGENCY_STOP_PATH));
    if path.as_os_str().is_empty() {
        bail!("copybot_operator_emergency_stop_empty_path: path cannot be empty");
    }
    Ok(path)
}
