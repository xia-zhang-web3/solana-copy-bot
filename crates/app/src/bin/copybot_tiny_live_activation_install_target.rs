use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
mod tiny_live_activation_live_execute;

type ServiceControlWrapperVerificationSummary =
    tiny_live_activation_live_execute::ServiceControlWrapperVerificationSummary;
type LoadedRenderedConfigArtifact =
    tiny_live_activation_live_execute::tiny_live_activation_execute::LoadedRenderedConfigArtifact;

const USAGE: &str = "usage: copybot_tiny_live_activation_install_target --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--activation-config-source <path>] [--rollback-config-source <path>] [--output <path>] [--json] (--plan-install-target | --render-install-script | --verify-install-target | --install-target)";
const INSTALL_TARGET_METADATA_VERSION: &str = "1";
const WRAPPER_RELATIVE_PATH: &str = "usr/local/bin/copybot-live-service-control";
const TARGET_CONFIG_RELATIVE_PATH: &str = "etc/solana-copy-bot/live.server.toml";
const ACTIVATION_CONFIG_RELATIVE_PATH: &str =
    "var/lib/solana-copy-bot/tiny-live/rendered.activation.toml";
const ROLLBACK_CONFIG_RELATIVE_PATH: &str =
    "var/lib/solana-copy-bot/tiny-live/rendered.rollback.toml";
const RUNTIME_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/runtime";
const BACKUP_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/backups";
const SESSION_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/sessions";
const INSTALL_METADATA_RELATIVE_PATH: &str =
    "var/lib/solana-copy-bot/tiny-live/install-target.json";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    install_root: PathBuf,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    activation_config_source_path: Option<PathBuf>,
    rollback_config_source_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanInstallTarget,
    RenderInstallScript,
    VerifyInstallTarget,
    InstallTarget,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveInstallTargetVerdict {
    TinyLiveInstallTargetPlanReady,
    TinyLiveInstallTargetScriptRendered,
    TinyLiveInstallTargetVerifyOk,
    TinyLiveInstallTargetVerifyInvalid,
    TinyLiveInstallTargetInstallCompleted,
    TinyLiveInstallTargetInstallRefused,
    TinyLiveInstallTargetWrapperInvalid,
    TinyLiveInstallTargetPathMismatch,
}

#[derive(Debug, Clone)]
struct InstallTargetPaths {
    install_root: PathBuf,
    wrapper_path: PathBuf,
    target_config_path: PathBuf,
    activation_config_path: PathBuf,
    activation_metadata_path: PathBuf,
    rollback_config_path: PathBuf,
    rollback_metadata_path: PathBuf,
    runtime_dir: PathBuf,
    backup_dir: PathBuf,
    session_dir: PathBuf,
    install_metadata_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstallTargetMetadata {
    metadata_version: String,
    installed_at: DateTime<Utc>,
    install_root: String,
    wrapper_path: String,
    target_config_path: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    activation_config_path: String,
    activation_metadata_path: String,
    rollback_config_path: String,
    rollback_metadata_path: String,
    runtime_dir: String,
    backup_dir: String,
    session_dir: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstallTargetVerificationSummary {
    wrapper_present: bool,
    wrapper_verified: Option<bool>,
    install_metadata_present: bool,
    install_metadata_matches_expected: Option<bool>,
    target_config_present: bool,
    target_config_parseable: Option<bool>,
    current_target_fingerprint_sha256: Option<String>,
    rendered_source_fingerprint_sha256: Option<String>,
    activation_artifact_present: bool,
    activation_artifact_verified: Option<bool>,
    rollback_artifact_present: bool,
    rollback_artifact_verified: Option<bool>,
    runtime_dir_present: bool,
    backup_dir_present: bool,
    session_dir_present: bool,
    wrapper_contract_ok: bool,
    path_contract_ok: bool,
    wrapper_verification: Option<ServiceControlWrapperVerificationSummary>,
    mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstallTargetReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveInstallTargetVerdict,
    reason: String,
    install_root: String,
    wrapper_path: String,
    target_config_path: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    activation_config_source_path: Option<String>,
    rollback_config_source_path: Option<String>,
    installed_activation_config_path: String,
    installed_activation_metadata_path: String,
    installed_rollback_config_path: String,
    installed_rollback_metadata_path: String,
    runtime_dir: String,
    backup_dir: String,
    session_dir: String,
    install_metadata_path: String,
    install_command_summary: Option<String>,
    live_execute_verify_command_summary: String,
    watch_live_command_summary: String,
    cutover_plan_command_summary: String,
    wrapper_verification: Option<ServiceControlWrapperVerificationSummary>,
    verification: Option<InstallTargetVerificationSummary>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct InstallCommandSummaries {
    install_command_summary: String,
    live_execute_verify_command_summary: String,
    watch_live_command_summary: String,
    cutover_plan_command_summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstallTargetPackageSummary {
    pub(crate) install_root: String,
    pub(crate) wrapper_path: String,
    pub(crate) target_config_path: String,
    pub(crate) target_service_name: String,
    pub(crate) backend_command: String,
    pub(crate) wrapper_timeout_ms: u64,
    pub(crate) installed_activation_config_path: String,
    pub(crate) installed_activation_metadata_path: String,
    pub(crate) installed_rollback_config_path: String,
    pub(crate) installed_rollback_metadata_path: String,
    pub(crate) runtime_dir: String,
    pub(crate) backup_dir: String,
    pub(crate) session_dir: String,
    pub(crate) install_metadata_path: String,
    pub(crate) live_execute_verify_command_summary: String,
    pub(crate) watch_live_command_summary: String,
    pub(crate) cutover_plan_command_summary: String,
    pub(crate) explicit_statement: String,
}

#[derive(Debug, Clone)]
struct SourceInstallContext {
    activation: LoadedRenderedConfigArtifact,
    rollback: LoadedRenderedConfigArtifact,
}

#[derive(Debug, Clone, Copy)]
enum PathKind {
    File,
    Directory,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut install_root: Option<PathBuf> = None;
    let mut target_service_name: Option<String> = None;
    let mut backend_command: Option<String> = None;
    let mut wrapper_timeout_ms =
        tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS;
    let mut activation_config_source_path: Option<PathBuf> = None;
    let mut rollback_config_source_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--install-root" => {
                install_root = Some(PathBuf::from(parse_string_arg(
                    "--install-root",
                    args.next(),
                )?))
            }
            "--target-service" => {
                target_service_name = Some(parse_string_arg("--target-service", args.next())?)
            }
            "--backend-command" => {
                backend_command = Some(parse_string_arg("--backend-command", args.next())?)
            }
            "--wrapper-timeout-ms" => {
                wrapper_timeout_ms = parse_u64_arg("--wrapper-timeout-ms", args.next())?
            }
            "--activation-config-source" => {
                activation_config_source_path = Some(PathBuf::from(parse_string_arg(
                    "--activation-config-source",
                    args.next(),
                )?))
            }
            "--rollback-config-source" => {
                rollback_config_source_path = Some(PathBuf::from(parse_string_arg(
                    "--rollback-config-source",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--json" => json = true,
            "--plan-install-target" => {
                set_mode(&mut mode, Mode::PlanInstallTarget, "--plan-install-target")?
            }
            "--render-install-script" => set_mode(
                &mut mode,
                Mode::RenderInstallScript,
                "--render-install-script",
            )?,
            "--verify-install-target" => set_mode(
                &mut mode,
                Mode::VerifyInstallTarget,
                "--verify-install-target",
            )?,
            "--install-target" => set_mode(&mut mode, Mode::InstallTarget, "--install-target")?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode"))?;
    let install_root = install_root.ok_or_else(|| anyhow!("missing required --install-root"))?;
    let target_service_name =
        target_service_name.ok_or_else(|| anyhow!("missing required --target-service"))?;
    let backend_command =
        backend_command.ok_or_else(|| anyhow!("missing required --backend-command"))?;

    match mode {
        Mode::PlanInstallTarget | Mode::RenderInstallScript | Mode::InstallTarget => {
            if activation_config_source_path.is_none() {
                bail!("missing required --activation-config-source");
            }
            if rollback_config_source_path.is_none() {
                bail!("missing required --rollback-config-source");
            }
        }
        Mode::VerifyInstallTarget => {}
    }
    if matches!(mode, Mode::RenderInstallScript) && output_path.is_none() {
        bail!("missing required --output for --render-install-script");
    }

    Ok(Some(Config {
        mode,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
        activation_config_source_path,
        rollback_config_source_path,
        output_path,
        json,
    }))
}

fn set_mode(mode: &mut Option<Mode>, next: Mode, flag: &str) -> Result<()> {
    if mode.replace(next).is_some() {
        bail!("multiple modes provided; {flag} conflicts with earlier mode");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("failed parsing {flag} as u64"))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanInstallTarget => match plan_install_target_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallRefused,
                error.to_string(),
            ),
        },
        Mode::RenderInstallScript => match render_install_script_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallRefused,
                error.to_string(),
            ),
        },
        Mode::VerifyInstallTarget => match verify_install_target_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveInstallTargetVerdict::TinyLiveInstallTargetVerifyInvalid,
                error.to_string(),
            ),
        },
        Mode::InstallTarget => match install_target_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallRefused,
                error.to_string(),
            ),
        },
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_install_target_report(config: &Config) -> Result<InstallTargetReport> {
    let paths = derive_paths(&config.install_root);
    let source_context = validate_source_install_context(config, &paths)?;
    let summaries = build_command_summaries(config, &paths);
    Ok(base_report(
        config,
        &paths,
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetPlanReady,
        format!(
            "live-target install contract is ready under {} with wrapper/runtime/backup/session/layout bound to deterministic repo-managed paths",
            paths.install_root.display()
        ),
        Some(summaries),
        None,
        None,
        Some(&source_context),
    ))
}

fn render_install_script_report(config: &Config) -> Result<InstallTargetReport> {
    let paths = derive_paths(&config.install_root);
    let source_context = validate_source_install_context(config, &paths)?;
    let summaries = build_command_summaries(config, &paths);
    let output_path = config.output_path.as_ref().expect("checked render output");
    ensure_new_output_path(output_path)?;
    let script = render_install_script_contents(config)?;
    write_text_file(output_path, &script)?;
    Ok(base_report(
        config,
        &paths,
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetScriptRendered,
        format!(
            "install-target script rendered to {} without mutating the live target",
            output_path.display()
        ),
        Some(summaries),
        None,
        None,
        Some(&source_context),
    ))
}

fn verify_install_target_report(config: &Config) -> Result<InstallTargetReport> {
    validate_static_install_contract_inputs(config)?;
    let paths = derive_paths(&config.install_root);
    let verification = inspect_install_target(config, &paths)?;
    let verdict = determine_verify_verdict(&verification);
    let reason = if verification.mismatches.is_empty() {
        format!(
            "install target under {} matches the deterministic wrapper/path/runtime/backup/session contract",
            paths.install_root.display()
        )
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "install target verification failed".to_string())
    };
    Ok(base_report(
        config,
        &paths,
        verdict,
        reason,
        Some(build_command_summaries(config, &paths)),
        verification.wrapper_verification.clone(),
        Some(verification),
        None,
    ))
}

fn install_target_report(config: &Config) -> Result<InstallTargetReport> {
    let paths = derive_paths(&config.install_root);
    let source_context = validate_source_install_context(config, &paths)?;
    ensure_install_targets_are_new(&paths)?;
    prepare_install_directories(&paths)?;
    install_wrapper(config, &paths)?;
    install_rendered_artifact(&source_context.activation, &paths.activation_config_path)?;
    install_rendered_artifact(&source_context.rollback, &paths.rollback_config_path)?;
    write_install_metadata(config, &paths)?;
    let verification = inspect_install_target(config, &paths)?;
    let verdict = if verification.mismatches.is_empty() {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallCompleted
    } else {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallRefused
    };
    let reason = if verification.mismatches.is_empty() {
        format!(
            "install target layout rendered under {} and verified against the repo-managed contract",
            paths.install_root.display()
        )
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "install target verification failed after install".to_string())
    };
    Ok(base_report(
        config,
        &paths,
        verdict,
        reason,
        Some(build_command_summaries(config, &paths)),
        verification.wrapper_verification.clone(),
        Some(verification),
        Some(&source_context),
    ))
}

fn base_report(
    config: &Config,
    paths: &InstallTargetPaths,
    verdict: TinyLiveInstallTargetVerdict,
    reason: String,
    summaries: Option<InstallCommandSummaries>,
    wrapper_verification: Option<ServiceControlWrapperVerificationSummary>,
    verification: Option<InstallTargetVerificationSummary>,
    source_context: Option<&SourceInstallContext>,
) -> InstallTargetReport {
    let summaries = summaries.unwrap_or_else(|| build_command_summaries(config, paths));
    InstallTargetReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        install_root: paths.install_root.display().to_string(),
        wrapper_path: paths.wrapper_path.display().to_string(),
        target_config_path: paths.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        activation_config_source_path: config
            .activation_config_source_path
            .as_ref()
            .map(|path| path.display().to_string()),
        rollback_config_source_path: config
            .rollback_config_source_path
            .as_ref()
            .map(|path| path.display().to_string()),
        installed_activation_config_path: paths.activation_config_path.display().to_string(),
        installed_activation_metadata_path: paths.activation_metadata_path.display().to_string(),
        installed_rollback_config_path: paths.rollback_config_path.display().to_string(),
        installed_rollback_metadata_path: paths.rollback_metadata_path.display().to_string(),
        runtime_dir: paths.runtime_dir.display().to_string(),
        backup_dir: paths.backup_dir.display().to_string(),
        session_dir: paths.session_dir.display().to_string(),
        install_metadata_path: paths.install_metadata_path.display().to_string(),
        install_command_summary: if source_context.is_some() {
            Some(summaries.install_command_summary)
        } else {
            None
        },
        live_execute_verify_command_summary: summaries.live_execute_verify_command_summary,
        watch_live_command_summary: summaries.watch_live_command_summary,
        cutover_plan_command_summary: summaries.cutover_plan_command_summary,
        wrapper_verification,
        verification,
        activation_authorized: false,
        explicit_statement:
            "this install-target contract only manages bounded live-target assets, wrapper path, runtime dirs, backup dirs, and service integration; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLiveInstallTargetVerdict,
    reason: String,
) -> InstallTargetReport {
    let paths = derive_paths(&config.install_root);
    base_report(
        config,
        &paths,
        verdict,
        reason,
        Some(build_command_summaries(config, &paths)),
        None,
        None,
        None,
    )
}

fn determine_verify_verdict(
    verification: &InstallTargetVerificationSummary,
) -> TinyLiveInstallTargetVerdict {
    if verification.mismatches.is_empty() {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetVerifyOk
    } else if !verification.wrapper_contract_ok {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetWrapperInvalid
    } else if !verification.path_contract_ok {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetPathMismatch
    } else {
        TinyLiveInstallTargetVerdict::TinyLiveInstallTargetVerifyInvalid
    }
}

fn validate_source_install_context(
    config: &Config,
    paths: &InstallTargetPaths,
) -> Result<SourceInstallContext> {
    validate_static_install_contract_inputs(config)?;
    validate_existing_path(
        config
            .activation_config_source_path
            .as_ref()
            .ok_or_else(|| anyhow!("missing activation config source path"))?,
        "activation config source",
        PathKind::File,
        true,
    )?;
    validate_existing_path(
        config
            .rollback_config_source_path
            .as_ref()
            .ok_or_else(|| anyhow!("missing rollback config source path"))?,
        "rollback config source",
        PathKind::File,
        true,
    )?;
    validate_path_under_root(
        &paths.install_root,
        &paths.target_config_path,
        "target config path",
        PathKind::File,
        true,
    )?;
    let (activation, rollback) =
        tiny_live_activation_live_execute::load_validated_artifact_pair_for_live_target(
            config
                .activation_config_source_path
                .as_ref()
                .expect("checked activation path"),
            config
                .rollback_config_source_path
                .as_ref()
                .expect("checked rollback path"),
        )?;
    let target = load_from_path(&paths.target_config_path).with_context(|| {
        format!(
            "failed loading target config {}",
            paths.target_config_path.display()
        )
    })?;
    let fingerprint =
        tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &target,
        )?;
    let expected_target_config_path = paths.target_config_path.display().to_string();
    if activation.metadata.input_config_path != expected_target_config_path
        || rollback.metadata.input_config_path != expected_target_config_path
    {
        bail!(
            "rendered activation assets were generated for {}, not for deterministic target config {}",
            activation.metadata.input_config_path,
            paths.target_config_path.display()
        );
    }
    if activation.metadata.source_config_fingerprint_sha256 != fingerprint.sha256
        || rollback.metadata.source_config_fingerprint_sha256 != fingerprint.sha256
    {
        bail!(
            "rendered activation assets were generated from source fingerprint {}, but current target config {} is {}",
            activation.metadata.source_config_fingerprint_sha256,
            paths.target_config_path.display(),
            fingerprint.sha256
        );
    }
    Ok(SourceInstallContext {
        activation,
        rollback,
    })
}

fn validate_static_install_contract_inputs(config: &Config) -> Result<()> {
    validate_install_root(&config.install_root)?;
    tiny_live_activation_live_execute::validate_target_service_name_for_live_target(
        &config.target_service_name,
    )?;
    tiny_live_activation_live_execute::live_service_control_wrapper_contract::render_wrapper_script_contents(
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
    .map(|_| ())
}

fn inspect_install_target(
    config: &Config,
    paths: &InstallTargetPaths,
) -> Result<InstallTargetVerificationSummary> {
    let mut mismatches = Vec::new();
    let mut path_contract_ok = true;
    let mut wrapper_contract_ok = true;

    if let Err(error) = validate_path_under_root(
        &paths.install_root,
        &paths.wrapper_path,
        "wrapper path",
        PathKind::File,
        true,
    ) {
        path_contract_ok = false;
        mismatches.push(error.to_string());
    }
    if let Err(error) = validate_path_under_root(
        &paths.install_root,
        &paths.target_config_path,
        "target config path",
        PathKind::File,
        true,
    ) {
        path_contract_ok = false;
        mismatches.push(error.to_string());
    }
    for (label, path) in [
        ("runtime dir", &paths.runtime_dir),
        ("backup dir", &paths.backup_dir),
        ("session dir", &paths.session_dir),
    ] {
        if let Err(error) =
            validate_path_under_root(&paths.install_root, path, label, PathKind::Directory, true)
        {
            mismatches.push(error.to_string());
        }
    }

    let install_metadata_present = paths.install_metadata_path.exists();
    let mut install_metadata_matches_expected = None;
    if install_metadata_present {
        match load_install_metadata(&paths.install_metadata_path) {
            Ok(metadata) => {
                let mut metadata_mismatches = Vec::new();
                if metadata.metadata_version != INSTALL_TARGET_METADATA_VERSION {
                    metadata_mismatches.push(format!(
                        "install target metadata version must be {}, found {}",
                        INSTALL_TARGET_METADATA_VERSION, metadata.metadata_version
                    ));
                }
                for (label, actual, expected) in [
                    (
                        "install_root",
                        metadata.install_root.as_str(),
                        paths.install_root.display().to_string().as_str(),
                    ),
                    (
                        "wrapper_path",
                        metadata.wrapper_path.as_str(),
                        paths.wrapper_path.display().to_string().as_str(),
                    ),
                    (
                        "target_config_path",
                        metadata.target_config_path.as_str(),
                        paths.target_config_path.display().to_string().as_str(),
                    ),
                    (
                        "activation_config_path",
                        metadata.activation_config_path.as_str(),
                        paths.activation_config_path.display().to_string().as_str(),
                    ),
                    (
                        "activation_metadata_path",
                        metadata.activation_metadata_path.as_str(),
                        paths
                            .activation_metadata_path
                            .display()
                            .to_string()
                            .as_str(),
                    ),
                    (
                        "rollback_config_path",
                        metadata.rollback_config_path.as_str(),
                        paths.rollback_config_path.display().to_string().as_str(),
                    ),
                    (
                        "rollback_metadata_path",
                        metadata.rollback_metadata_path.as_str(),
                        paths.rollback_metadata_path.display().to_string().as_str(),
                    ),
                    (
                        "runtime_dir",
                        metadata.runtime_dir.as_str(),
                        paths.runtime_dir.display().to_string().as_str(),
                    ),
                    (
                        "backup_dir",
                        metadata.backup_dir.as_str(),
                        paths.backup_dir.display().to_string().as_str(),
                    ),
                    (
                        "session_dir",
                        metadata.session_dir.as_str(),
                        paths.session_dir.display().to_string().as_str(),
                    ),
                ] {
                    if actual != expected {
                        metadata_mismatches.push(format!(
                            "install target path mismatch for {label}: expected {expected}, found {actual}"
                        ));
                    }
                }
                if metadata.target_service_name != config.target_service_name {
                    metadata_mismatches.push(format!(
                        "install target metadata target_service_name must be {}, found {}",
                        config.target_service_name, metadata.target_service_name
                    ));
                }
                if metadata.backend_command != config.backend_command {
                    metadata_mismatches.push(format!(
                        "install target metadata backend_command must be {}, found {}",
                        config.backend_command, metadata.backend_command
                    ));
                }
                if metadata.wrapper_timeout_ms != config.wrapper_timeout_ms {
                    metadata_mismatches.push(format!(
                        "install target metadata wrapper_timeout_ms must be {}, found {}",
                        config.wrapper_timeout_ms, metadata.wrapper_timeout_ms
                    ));
                }
                install_metadata_matches_expected = Some(metadata_mismatches.is_empty());
                if !metadata_mismatches.is_empty() {
                    path_contract_ok = false;
                    mismatches.extend(metadata_mismatches);
                }
            }
            Err(error) => mismatches.push(error.to_string()),
        }
    } else {
        mismatches.push(format!(
            "install target metadata {} is missing",
            paths.install_metadata_path.display()
        ));
    }

    let target_config_present = paths.target_config_path.exists();
    let mut target_config_parseable = None;
    let mut current_target_fingerprint_sha256 = None;
    if target_config_present {
        match load_from_path(&paths.target_config_path) {
            Ok(target) => {
                target_config_parseable = Some(true);
                let fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
                    tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE,
                    &target,
                )?;
                current_target_fingerprint_sha256 = Some(fingerprint.sha256);
            }
            Err(error) => {
                target_config_parseable = Some(false);
                mismatches.push(format!(
                    "target config {} is not parseable: {error:#}",
                    paths.target_config_path.display()
                ));
            }
        }
    } else {
        mismatches.push(format!(
            "target config {} is missing",
            paths.target_config_path.display()
        ));
    }

    let wrapper_present = paths.wrapper_path.exists();
    let mut wrapper_verified = None;
    let mut wrapper_verification = None;
    if wrapper_present {
        match tiny_live_activation_live_execute::live_service_control_wrapper_contract::verify_wrapper(
            &paths.wrapper_path,
        ) {
            Ok(summary) => {
                let mut wrapper_mismatches = summary.mismatches.clone();
                if summary.backend_command.as_deref() != Some(config.backend_command.as_str()) {
                    wrapper_mismatches.push(format!(
                        "wrapper backend_command must be {}, found {}",
                        config.backend_command,
                        summary
                            .backend_command
                            .clone()
                            .unwrap_or_else(|| "<missing>".to_string())
                    ));
                }
                if summary.timeout_ms != Some(config.wrapper_timeout_ms) {
                    wrapper_mismatches.push(format!(
                        "wrapper timeout_ms must be {}, found {}",
                        config.wrapper_timeout_ms,
                        summary
                            .timeout_ms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "<missing>".to_string())
                    ));
                }
                wrapper_verified = Some(wrapper_mismatches.is_empty());
                if !wrapper_mismatches.is_empty() {
                    wrapper_contract_ok = false;
                    mismatches.extend(wrapper_mismatches);
                }
                wrapper_verification = Some(summary);
            }
            Err(error) => {
                wrapper_contract_ok = false;
                mismatches.push(format!(
                    "wrapper {} failed verification: {error:#}",
                    paths.wrapper_path.display()
                ));
            }
        }
    } else {
        mismatches.push(format!(
            "wrapper {} is missing",
            paths.wrapper_path.display()
        ));
    }

    let activation_artifact_present = paths.activation_config_path.exists();
    let rollback_artifact_present = paths.rollback_config_path.exists();
    let mut activation_artifact_verified = None;
    let mut rollback_artifact_verified = None;
    let mut rendered_source_fingerprint_sha256 = None;
    if activation_artifact_present && rollback_artifact_present {
        match tiny_live_activation_live_execute::load_validated_artifact_pair_for_live_target(
            &paths.activation_config_path,
            &paths.rollback_config_path,
        ) {
            Ok((activation, rollback)) => {
                activation_artifact_verified = Some(true);
                rollback_artifact_verified = Some(true);
                rendered_source_fingerprint_sha256 =
                    Some(activation.metadata.source_config_fingerprint_sha256.clone());
                let expected_target_config_path = paths.target_config_path.display().to_string();
                if activation.metadata.input_config_path != expected_target_config_path
                    || rollback.metadata.input_config_path != expected_target_config_path
                {
                    mismatches.push(format!(
                        "installed rendered artifacts must point back to deterministic target config {}",
                        paths.target_config_path.display()
                    ));
                }
                if activation.metadata.output_config_path
                    != paths.activation_config_path.display().to_string()
                {
                    path_contract_ok = false;
                    mismatches.push(format!(
                        "install target path mismatch for activation output: expected {}, found {}",
                        paths.activation_config_path.display(),
                        activation.metadata.output_config_path
                    ));
                }
                if rollback.metadata.output_config_path
                    != paths.rollback_config_path.display().to_string()
                {
                    path_contract_ok = false;
                    mismatches.push(format!(
                        "install target path mismatch for rollback output: expected {}, found {}",
                        paths.rollback_config_path.display(),
                        rollback.metadata.output_config_path
                    ));
                }
                if let Some(current_fingerprint) = current_target_fingerprint_sha256.as_deref() {
                    if activation.metadata.source_config_fingerprint_sha256 != current_fingerprint
                        || rollback.metadata.source_config_fingerprint_sha256 != current_fingerprint
                    {
                        mismatches.push(format!(
                            "installed rendered artifacts were generated from fingerprint {}, but current target config is {}",
                            activation.metadata.source_config_fingerprint_sha256,
                            current_fingerprint
                        ));
                    }
                }
            }
            Err(error) => {
                activation_artifact_verified = Some(false);
                rollback_artifact_verified = Some(false);
                mismatches.push(format!(
                    "installed rendered activation assets are invalid: {error:#}"
                ));
            }
        }
    } else {
        if !activation_artifact_present {
            mismatches.push(format!(
                "installed activation config {} is missing",
                paths.activation_config_path.display()
            ));
        }
        if !rollback_artifact_present {
            mismatches.push(format!(
                "installed rollback config {} is missing",
                paths.rollback_config_path.display()
            ));
        }
    }

    Ok(InstallTargetVerificationSummary {
        wrapper_present,
        wrapper_verified,
        install_metadata_present,
        install_metadata_matches_expected,
        target_config_present,
        target_config_parseable,
        current_target_fingerprint_sha256,
        rendered_source_fingerprint_sha256,
        activation_artifact_present,
        activation_artifact_verified,
        rollback_artifact_present,
        rollback_artifact_verified,
        runtime_dir_present: paths.runtime_dir.exists(),
        backup_dir_present: paths.backup_dir.exists(),
        session_dir_present: paths.session_dir.exists(),
        wrapper_contract_ok,
        path_contract_ok,
        wrapper_verification,
        mismatches,
    })
}

fn ensure_install_targets_are_new(paths: &InstallTargetPaths) -> Result<()> {
    for path in [
        &paths.wrapper_path,
        &paths.activation_config_path,
        &paths.activation_metadata_path,
        &paths.rollback_config_path,
        &paths.rollback_metadata_path,
        &paths.install_metadata_path,
    ] {
        if path.exists() {
            bail!(
                "refusing to overwrite existing install-target artifact {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn prepare_install_directories(paths: &InstallTargetPaths) -> Result<()> {
    for path in [
        &paths.runtime_dir,
        &paths.backup_dir,
        &paths.session_dir,
        paths
            .activation_config_path
            .parent()
            .ok_or_else(|| anyhow!("activation config path has no parent"))?,
        paths
            .wrapper_path
            .parent()
            .ok_or_else(|| anyhow!("wrapper path has no parent"))?,
    ] {
        fs::create_dir_all(path).with_context(|| {
            format!(
                "failed creating install-target directory {}",
                path.display()
            )
        })?;
    }
    Ok(())
}

fn install_wrapper(config: &Config, paths: &InstallTargetPaths) -> Result<()> {
    tiny_live_activation_live_execute::live_service_control_wrapper_contract::render_wrapper_script(
        &paths.wrapper_path,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
}

fn install_rendered_artifact(
    source: &LoadedRenderedConfigArtifact,
    destination_path: &Path,
) -> Result<()> {
    let mut metadata = source.metadata.clone();
    metadata.output_config_path = destination_path.display().to_string();
    write_bytes_file(
        destination_path,
        &fs::read(&source.rendered_config_path).with_context(|| {
            format!(
                "failed reading rendered config {}",
                source.rendered_config_path.display()
            )
        })?,
    )?;
    write_json_file(
        &tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(destination_path),
        &metadata,
    )?;
    tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(destination_path)?;
    Ok(())
}

fn write_install_metadata(config: &Config, paths: &InstallTargetPaths) -> Result<()> {
    let metadata = InstallTargetMetadata {
        metadata_version: INSTALL_TARGET_METADATA_VERSION.to_string(),
        installed_at: Utc::now(),
        install_root: paths.install_root.display().to_string(),
        wrapper_path: paths.wrapper_path.display().to_string(),
        target_config_path: paths.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        activation_config_path: paths.activation_config_path.display().to_string(),
        activation_metadata_path: paths.activation_metadata_path.display().to_string(),
        rollback_config_path: paths.rollback_config_path.display().to_string(),
        rollback_metadata_path: paths.rollback_metadata_path.display().to_string(),
        runtime_dir: paths.runtime_dir.display().to_string(),
        backup_dir: paths.backup_dir.display().to_string(),
        session_dir: paths.session_dir.display().to_string(),
        explicit_statement:
            "this metadata binds the repo-managed live-target install surface only; it does not authorize production activation by itself"
                .to_string(),
    };
    write_json_file(&paths.install_metadata_path, &metadata)
}

fn load_install_metadata(path: &Path) -> Result<InstallTargetMetadata> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading install-target metadata {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing install-target metadata {}", path.display()))
}

fn build_command_summaries(config: &Config, paths: &InstallTargetPaths) -> InstallCommandSummaries {
    InstallCommandSummaries {
        install_command_summary: format!(
            "copybot_tiny_live_activation_install_target --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --activation-config-source {} --rollback-config-source {} --install-target",
            shell_single_quote(&paths.install_root.display().to_string()),
            shell_single_quote(&config.target_service_name),
            shell_single_quote(&config.backend_command),
            config.wrapper_timeout_ms,
            shell_single_quote(
                &config
                    .activation_config_source_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing>".to_string())
            ),
            shell_single_quote(
                &config
                    .rollback_config_source_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing>".to_string())
            )
        ),
        live_execute_verify_command_summary: format!(
            "copybot_tiny_live_activation_live_execute --activation-config {} --rollback-config {} --target-config {} --target-service {} --service-control-command {} --runtime-dir {} --backup-dir {} --verify-live-target",
            shell_single_quote(&paths.activation_config_path.display().to_string()),
            shell_single_quote(&paths.rollback_config_path.display().to_string()),
            shell_single_quote(&paths.target_config_path.display().to_string()),
            shell_single_quote(&config.target_service_name),
            shell_single_quote(&paths.wrapper_path.display().to_string()),
            shell_single_quote(&paths.runtime_dir.display().to_string()),
            shell_single_quote(&paths.backup_dir.display().to_string())
        ),
        watch_live_command_summary: format!(
            "copybot_tiny_live_activation_watch --activation-config {} --rollback-config {} --runtime-dir {} --target-config {} --target-service {} --service-control-command {} --backup-dir {} --verify-watch-target",
            shell_single_quote(&paths.activation_config_path.display().to_string()),
            shell_single_quote(&paths.rollback_config_path.display().to_string()),
            shell_single_quote(&paths.runtime_dir.display().to_string()),
            shell_single_quote(&paths.target_config_path.display().to_string()),
            shell_single_quote(&config.target_service_name),
            shell_single_quote(&paths.wrapper_path.display().to_string()),
            shell_single_quote(&paths.backup_dir.display().to_string())
        ),
        cutover_plan_command_summary: format!(
            "copybot_tiny_live_activation_cutover --activation-config {} --rollback-config {} --runtime-dir {} --target-config {} --target-service {} --service-control-command {} --backup-dir {} --session-dir {} --plan-live-cutover",
            shell_single_quote(&paths.activation_config_path.display().to_string()),
            shell_single_quote(&paths.rollback_config_path.display().to_string()),
            shell_single_quote(&paths.runtime_dir.display().to_string()),
            shell_single_quote(&paths.target_config_path.display().to_string()),
            shell_single_quote(&config.target_service_name),
            shell_single_quote(&paths.wrapper_path.display().to_string()),
            shell_single_quote(&paths.backup_dir.display().to_string()),
            shell_single_quote(&paths.session_dir.display().to_string())
        ),
    }
}

#[allow(dead_code)]
pub(crate) fn build_install_target_package_summary(
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    activation_config_source_path: &Path,
    rollback_config_source_path: &Path,
) -> Result<InstallTargetPackageSummary> {
    let config = Config {
        mode: Mode::PlanInstallTarget,
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        activation_config_source_path: Some(activation_config_source_path.to_path_buf()),
        rollback_config_source_path: Some(rollback_config_source_path.to_path_buf()),
        output_path: None,
        json: false,
    };
    let report = plan_install_target_report(&config)?;
    Ok(install_target_package_summary_from_report(&report))
}

#[allow(dead_code)]
pub(crate) fn verify_install_target_package_summary(
    summary: &InstallTargetPackageSummary,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    let install_root = PathBuf::from(&summary.install_root);
    let paths = derive_paths(&install_root);
    for (label, actual, expected) in [
        (
            "wrapper_path",
            summary.wrapper_path.as_str(),
            paths.wrapper_path.display().to_string().as_str(),
        ),
        (
            "target_config_path",
            summary.target_config_path.as_str(),
            paths.target_config_path.display().to_string().as_str(),
        ),
        (
            "installed_activation_config_path",
            summary.installed_activation_config_path.as_str(),
            paths.activation_config_path.display().to_string().as_str(),
        ),
        (
            "installed_activation_metadata_path",
            summary.installed_activation_metadata_path.as_str(),
            paths
                .activation_metadata_path
                .display()
                .to_string()
                .as_str(),
        ),
        (
            "installed_rollback_config_path",
            summary.installed_rollback_config_path.as_str(),
            paths.rollback_config_path.display().to_string().as_str(),
        ),
        (
            "installed_rollback_metadata_path",
            summary.installed_rollback_metadata_path.as_str(),
            paths.rollback_metadata_path.display().to_string().as_str(),
        ),
        (
            "runtime_dir",
            summary.runtime_dir.as_str(),
            paths.runtime_dir.display().to_string().as_str(),
        ),
        (
            "backup_dir",
            summary.backup_dir.as_str(),
            paths.backup_dir.display().to_string().as_str(),
        ),
        (
            "session_dir",
            summary.session_dir.as_str(),
            paths.session_dir.display().to_string().as_str(),
        ),
        (
            "install_metadata_path",
            summary.install_metadata_path.as_str(),
            paths.install_metadata_path.display().to_string().as_str(),
        ),
    ] {
        if actual != expected {
            mismatches.push(format!(
                "install target package summary mismatch for {label}: expected {expected}, found {actual}"
            ));
        }
    }
    if summary.target_service_name.is_empty() {
        mismatches.push("install target package summary target_service_name is empty".to_string());
    }
    if summary.backend_command.is_empty() {
        mismatches.push("install target package summary backend_command is empty".to_string());
    }
    if summary.wrapper_timeout_ms == 0 {
        mismatches.push(
            "install target package summary wrapper_timeout_ms must be greater than zero"
                .to_string(),
        );
    }
    if summary.live_execute_verify_command_summary.is_empty() {
        mismatches.push(
            "install target package summary live_execute_verify_command_summary is empty"
                .to_string(),
        );
    }
    if summary.watch_live_command_summary.is_empty() {
        mismatches
            .push("install target package summary watch_live_command_summary is empty".to_string());
    }
    if summary.cutover_plan_command_summary.is_empty() {
        mismatches.push(
            "install target package summary cutover_plan_command_summary is empty".to_string(),
        );
    }
    if !summary
        .live_execute_verify_command_summary
        .contains(&summary.installed_activation_config_path)
        || !summary
            .live_execute_verify_command_summary
            .contains(&summary.installed_rollback_config_path)
        || !summary
            .live_execute_verify_command_summary
            .contains(&summary.wrapper_path)
    {
        mismatches.push(
            "install target package summary live_execute_verify_command_summary does not match the deterministic installed paths"
                .to_string(),
        );
    }
    if !summary
        .watch_live_command_summary
        .contains(&summary.runtime_dir)
        || !summary
            .watch_live_command_summary
            .contains(&summary.backup_dir)
        || !summary
            .watch_live_command_summary
            .contains(&summary.wrapper_path)
    {
        mismatches.push(
            "install target package summary watch_live_command_summary does not match the deterministic installed paths"
                .to_string(),
        );
    }
    if !summary
        .cutover_plan_command_summary
        .contains(&summary.session_dir)
        || !summary
            .cutover_plan_command_summary
            .contains(&summary.backup_dir)
        || !summary
            .cutover_plan_command_summary
            .contains(&summary.wrapper_path)
    {
        mismatches.push(
            "install target package summary cutover_plan_command_summary does not match the deterministic installed paths"
                .to_string(),
        );
    }
    mismatches
}

fn install_target_package_summary_from_report(
    report: &InstallTargetReport,
) -> InstallTargetPackageSummary {
    InstallTargetPackageSummary {
        install_root: report.install_root.clone(),
        wrapper_path: report.wrapper_path.clone(),
        target_config_path: report.target_config_path.clone(),
        target_service_name: report.target_service_name.clone(),
        backend_command: report.backend_command.clone(),
        wrapper_timeout_ms: report.wrapper_timeout_ms,
        installed_activation_config_path: report.installed_activation_config_path.clone(),
        installed_activation_metadata_path: report.installed_activation_metadata_path.clone(),
        installed_rollback_config_path: report.installed_rollback_config_path.clone(),
        installed_rollback_metadata_path: report.installed_rollback_metadata_path.clone(),
        runtime_dir: report.runtime_dir.clone(),
        backup_dir: report.backup_dir.clone(),
        session_dir: report.session_dir.clone(),
        install_metadata_path: report.install_metadata_path.clone(),
        live_execute_verify_command_summary: report.live_execute_verify_command_summary.clone(),
        watch_live_command_summary: report.watch_live_command_summary.clone(),
        cutover_plan_command_summary: report.cutover_plan_command_summary.clone(),
        explicit_statement: report.explicit_statement.clone(),
    }
}

fn derive_paths(install_root: &Path) -> InstallTargetPaths {
    let activation_config_path = install_root.join(ACTIVATION_CONFIG_RELATIVE_PATH);
    let rollback_config_path = install_root.join(ROLLBACK_CONFIG_RELATIVE_PATH);
    InstallTargetPaths {
        install_root: install_root.to_path_buf(),
        wrapper_path: install_root.join(WRAPPER_RELATIVE_PATH),
        target_config_path: install_root.join(TARGET_CONFIG_RELATIVE_PATH),
        activation_metadata_path: tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(
            &activation_config_path,
        ),
        rollback_metadata_path: tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(
            &rollback_config_path,
        ),
        activation_config_path,
        rollback_config_path,
        runtime_dir: install_root.join(RUNTIME_DIR_RELATIVE_PATH),
        backup_dir: install_root.join(BACKUP_DIR_RELATIVE_PATH),
        session_dir: install_root.join(SESSION_DIR_RELATIVE_PATH),
        install_metadata_path: install_root.join(INSTALL_METADATA_RELATIVE_PATH),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanInstallTarget => "plan_install_target",
        Mode::RenderInstallScript => "render_install_script",
        Mode::VerifyInstallTarget => "verify_install_target",
        Mode::InstallTarget => "install_target",
    }
}

fn render_install_script_contents(config: &Config) -> Result<String> {
    Ok(format!(
        "#!/usr/bin/env bash\nset -euo pipefail\ncopybot_tiny_live_activation_install_target --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --activation-config-source {} --rollback-config-source {} --install-target\n",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        shell_single_quote(
            &config
                .activation_config_source_path
                .as_ref()
                .ok_or_else(|| anyhow!("missing activation config source path"))?
                .display()
                .to_string()
        ),
        shell_single_quote(
            &config
                .rollback_config_source_path
                .as_ref()
                .ok_or_else(|| anyhow!("missing rollback config source path"))?
                .display()
                .to_string()
        ),
    ))
}

fn render_human_report(report: &InstallTargetReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("install_root={}", report.install_root),
        format!("wrapper_path={}", report.wrapper_path),
        format!("target_config_path={}", report.target_config_path),
        format!("target_service_name={}", report.target_service_name),
        format!("backend_command={}", report.backend_command),
        format!("wrapper_timeout_ms={}", report.wrapper_timeout_ms),
        format!(
            "installed_activation_config_path={}",
            report.installed_activation_config_path
        ),
        format!(
            "installed_rollback_config_path={}",
            report.installed_rollback_config_path
        ),
        format!("runtime_dir={}", report.runtime_dir),
        format!("backup_dir={}", report.backup_dir),
        format!("session_dir={}", report.session_dir),
        format!("install_metadata_path={}", report.install_metadata_path),
    ];
    if let Some(path) = &report.activation_config_source_path {
        lines.push(format!("activation_config_source_path={path}"));
    }
    if let Some(path) = &report.rollback_config_source_path {
        lines.push(format!("rollback_config_source_path={path}"));
    }
    if let Some(summary) = &report.install_command_summary {
        lines.push(format!("install_command_summary={summary}"));
    }
    lines.push(format!(
        "live_execute_verify_command_summary={}",
        report.live_execute_verify_command_summary
    ));
    lines.push(format!(
        "watch_live_command_summary={}",
        report.watch_live_command_summary
    ));
    lines.push(format!(
        "cutover_plan_command_summary={}",
        report.cutover_plan_command_summary
    ));
    if let Some(wrapper) = &report.wrapper_verification {
        lines.push(format!(
            "wrapper_contract_mismatches={}",
            wrapper.mismatches.join(" | ")
        ));
    }
    if let Some(verification) = &report.verification {
        lines.push(format!(
            "verification_mismatches={}",
            verification.mismatches.join(" | ")
        ));
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|raw| raw.as_str().map(str::to_string))
        .unwrap_or_else(|| "<unknown>".to_string())
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("refusing to overwrite existing output {}", path.display());
    }
    Ok(())
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent directory {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent directory {}", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(value)?;
    fs::write(path, raw).with_context(|| format!("failed writing {}", path.display()))
}

fn write_bytes_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent directory {}", parent.display()))?;
    }
    fs::write(path, bytes).with_context(|| format!("failed writing {}", path.display()))
}

fn validate_install_root(root: &Path) -> Result<()> {
    validate_existing_path(root, "install root", PathKind::Directory, false)
}

fn validate_path_under_root(
    root: &Path,
    path: &Path,
    label: &str,
    kind: PathKind,
    require_exists: bool,
) -> Result<()> {
    if !path.starts_with(root) {
        bail!(
            "{label} {} must stay under install root {}",
            path.display(),
            root.display()
        );
    }
    validate_existing_path(path, label, kind, require_exists)
}

fn validate_existing_path(
    path: &Path,
    label: &str,
    kind: PathKind,
    require_exists: bool,
) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} must be an absolute path");
    }
    let anchor = find_existing_anchor(path, label, kind)?;
    if require_exists && anchor != path {
        bail!("{label} {} must already exist", path.display());
    }
    reject_existing_descendant_symlinks(path, &anchor, label)?;
    Ok(())
}

fn find_existing_anchor(path: &Path, label: &str, kind: PathKind) -> Result<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!("{label} {} must not be a symlink", current.display());
                }
                if current == path {
                    match kind {
                        PathKind::File if !metadata.is_file() => {
                            bail!("{label} {} is not a regular file", path.display())
                        }
                        PathKind::Directory if !metadata.is_dir() => {
                            bail!("{label} {} is not a directory", path.display())
                        }
                        _ => {}
                    }
                }
                return Ok(current);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current = current.parent().map(Path::to_path_buf).ok_or_else(|| {
                    anyhow!("{label} {} has no existing parent anchor", path.display())
                })?;
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed inspecting {label} path component {}: {}",
                    current.display(),
                    error
                ))
            }
        }
    }
}

fn reject_existing_descendant_symlinks(path: &Path, anchor: &Path, label: &str) -> Result<()> {
    let relative = path.strip_prefix(anchor).map_err(|_| {
        anyhow!(
            "{label} {} must stay under its existing anchor {}",
            path.display(),
            anchor.display()
        )
    })?;
    let mut current = anchor.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!(
                        "{label} path component {} is a symlink; symlinked install-target paths are not allowed",
                        current.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(anyhow!(
                    "failed inspecting {label} path component {}: {}",
                    current.display(),
                    error
                ))
            }
        }
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn plan_install_target_is_green_for_fresh_fake_root() {
        let fixture = install_fixture("tiny_live_install_target_plan");
        let report = plan_install_target_report(&fixture.config).expect("plan");
        assert_eq!(
            report.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetPlanReady
        );
        assert!(report.reason.contains("install contract is ready"));
        assert_eq!(
            report.target_config_path,
            fixture.paths.target_config_path.display().to_string()
        );
    }

    #[test]
    fn render_install_script_is_deterministic_and_non_overwriting() {
        let fixture = install_fixture("tiny_live_install_target_script");
        let output = fixture.fixture_dir.join("install-target.sh");
        let report = render_install_script_report(&Config {
            mode: Mode::RenderInstallScript,
            output_path: Some(output.clone()),
            ..fixture.config.clone()
        })
        .expect("render script");
        assert_eq!(
            report.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetScriptRendered
        );
        let contents = fs::read_to_string(&output).unwrap();
        assert!(contents.contains("--install-target"));
        assert!(contents.contains("--install-root"));
        let refused_output = run(Config {
            mode: Mode::RenderInstallScript,
            output_path: Some(output),
            json: true,
            ..fixture.config.clone()
        })
        .expect("render refusal output");
        let refused: InstallTargetReport = serde_json::from_str(&refused_output).unwrap();
        assert_eq!(
            refused.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallRefused
        );
    }

    #[test]
    fn install_and_verify_target_is_green_for_fake_root() {
        let fixture = install_fixture("tiny_live_install_target_install");
        let install = install_target_report(&fixture.config).expect("install");
        assert_eq!(
            install.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetInstallCompleted
        );
        let verify = verify_install_target_report(&Config {
            mode: Mode::VerifyInstallTarget,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_path: None,
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetVerifyOk
        );
        assert!(fixture.paths.wrapper_path.exists());
        assert!(fixture.paths.runtime_dir.is_dir());
        assert!(fixture.paths.backup_dir.is_dir());
        assert!(fixture.paths.session_dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn tampered_wrapper_path_is_rejected() {
        let fixture = install_fixture("tiny_live_install_target_bad_wrapper");
        install_target_report(&fixture.config).expect("install");
        write_non_wrapper(&fixture.paths.wrapper_path);
        let verify = verify_install_target_report(&Config {
            mode: Mode::VerifyInstallTarget,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_path: None,
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetWrapperInvalid
        );
        assert!(verify.reason.contains("wrapper"));
    }

    #[test]
    fn missing_runtime_backup_and_session_dirs_are_rejected() {
        let fixture = install_fixture("tiny_live_install_target_missing_dirs");
        install_target_report(&fixture.config).expect("install");
        fs::remove_dir_all(&fixture.paths.runtime_dir).unwrap();
        fs::remove_dir_all(&fixture.paths.backup_dir).unwrap();
        fs::remove_dir_all(&fixture.paths.session_dir).unwrap();
        let verify = verify_install_target_report(&Config {
            mode: Mode::VerifyInstallTarget,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_path: None,
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetVerifyInvalid
        );
        assert!(verify
            .verification
            .as_ref()
            .map(|value| {
                value.mismatches.iter().any(|item| {
                    item.contains("runtime dir")
                        || item.contains("backup dir")
                        || item.contains("session dir")
                })
            })
            .unwrap_or(false));
    }

    #[test]
    fn path_mismatch_under_fake_root_is_rejected() {
        let fixture = install_fixture("tiny_live_install_target_path_mismatch");
        install_target_report(&fixture.config).expect("install");
        let mut metadata = load_install_metadata(&fixture.paths.install_metadata_path).unwrap();
        metadata.runtime_dir = fixture
            .fixture_dir
            .join("outside-runtime")
            .display()
            .to_string();
        write_json_file(&fixture.paths.install_metadata_path, &metadata).unwrap();
        let verify = verify_install_target_report(&Config {
            mode: Mode::VerifyInstallTarget,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_path: None,
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveInstallTargetVerdict::TinyLiveInstallTargetPathMismatch
        );
    }

    struct InstallFixture {
        fixture_dir: PathBuf,
        paths: InstallTargetPaths,
        config: Config,
    }

    fn install_fixture(label: &str) -> InstallFixture {
        let fixture_dir = temp_dir(label);
        let install_root = fixture_dir.join("root");
        let paths = derive_paths(&install_root);
        fs::create_dir_all(
            paths
                .target_config_path
                .parent()
                .expect("target config parent"),
        )
        .unwrap();
        fs::write(&paths.target_config_path, sample_safe_config_toml()).unwrap();
        let source_dir = fixture_dir.join("source-artifacts");
        fs::create_dir_all(&source_dir).unwrap();
        let activation_config_source_path = source_dir.join("rendered.activation.toml");
        let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
        write_rendered_artifact_pair(
            &paths.target_config_path,
            &activation_config_source_path,
            &rollback_config_source_path,
        );
        InstallFixture {
            fixture_dir,
            paths,
            config: Config {
                mode: Mode::PlanInstallTarget,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: "systemctl".to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                activation_config_source_path: Some(activation_config_source_path),
                rollback_config_source_path: Some(rollback_config_source_path),
                output_path: None,
                json: false,
            },
        }
    }

    fn write_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
    ) {
        let source = load_from_path(target_config_path).unwrap();
        let fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source,
        )
        .unwrap();
        write_rendered_artifact(
            activation_path,
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            target_config_path,
            &fingerprint.sha256,
            false,
            true,
        );
        write_rendered_artifact(
            rollback_path,
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Rollback,
            sample_safe_config_toml(),
            target_config_path,
            &fingerprint.sha256,
            false,
            false,
        );
    }

    fn write_rendered_artifact(
        path: &Path,
        render_kind: tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind,
        contents: String,
        source_config_path: &Path,
        source_fingerprint_sha256: &str,
        source_execution_enabled: bool,
        target_execution_enabled: bool,
    ) {
        fs::write(path, &contents).unwrap();
        let metadata = tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata {
            metadata_version: "1".to_string(),
            render_kind,
            generated_at: ts("2026-03-27T12:00:00Z"),
            input_config_path: source_config_path.display().to_string(),
            output_config_path: path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE
                .to_string(),
            source_config_fingerprint_sha256: source_fingerprint_sha256.to_string(),
            expected_source_fingerprint_sha256: Some(source_fingerprint_sha256.to_string()),
            rendered_config_sha256: sha256_hex(contents.as_bytes()),
            pre_activation_gate_verdict: "pre_activation_gates_green".to_string(),
            pre_activation_gate_reason: "green".to_string(),
            activation_plan_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
            activation_plan_reason: "ready".to_string(),
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            field_expectations: vec![tiny_live_activation_live_execute::tiny_live_activation_execute::FieldExpectation {
                field: "execution.enabled".to_string(),
                source_value: serde_json::json!(source_execution_enabled),
                target_value: serde_json::json!(target_execution_enabled),
                reason: "test".to_string(),
                source: "test".to_string(),
            }],
            execution_untouched_by_batch: true,
            activation_authorized: false,
            not_authorized_summary: "test".to_string(),
        };
        fs::write(
            tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(path),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
        tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(path).unwrap();
    }

    #[cfg(unix)]
    fn write_non_wrapper(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, "#!/usr/bin/env bash\necho fake\n").unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn sample_activation_config_toml() -> String {
        sample_safe_config_toml().replacen("enabled = false", "enabled = true", 1)
    }

    fn sample_safe_config_toml() -> String {
        r#"
[system]
env = "prod-live"

[sqlite]
path = "/tmp/copybot-live.db"

[execution]
enabled = false
mode = "adapter_submit_confirm"
batch_size = 1
default_route = "jito"
rpc_http_url = "https://rpc.example"
submit_adapter_http_url = "http://127.0.0.1:8080/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
submit_adapter_auth_token = "adapter-token"
submit_allowed_routes = ["jito"]
submit_route_order = ["jito"]
pretrade_max_fee_overhead_bps = 800
pretrade_max_priority_fee_lamports = 1500
slippage_bps = 40.0
execution_signer_pubkey = "11111111111111111111111111111111"

[execution.submit_route_max_slippage_bps]
jito = 40.0

[execution.submit_route_tip_lamports]
jito = 10000

[execution.submit_route_compute_unit_limit]
jito = 300000

[execution.submit_route_compute_unit_price_micro_lamports]
jito = 1500

[shadow]
copy_notional_sol = 0.05

[risk]
max_position_sol = 0.05
max_concurrent_positions = 1
daily_loss_limit_pct = 0.75

[tiny_live_policy]
enabled = true
max_trade_notional_sol = 0.05
max_batch_size = 1
max_concurrent_positions = 1
max_daily_loss_limit_pct = 1.0
allowed_routes = ["jito"]
require_policy_echo = true
max_pretrade_fee_overhead_bps = 1000
max_pretrade_priority_fee_lamports = 2000

[tiny_live_policy.max_route_slippage_bps]
jito = 50.0

[tiny_live_policy.max_route_tip_lamports]
jito = 10000

[tiny_live_policy.max_route_compute_unit_price_micro_lamports]
jito = 1500

[tiny_live_guardrails]
enabled = true
evaluation_window_seconds = 900
max_execution_error_rate_pct = 5.0
max_adapter_contract_failure_rate_pct = 1.0
max_policy_echo_mismatch_rate_pct = 1.0
max_fee_or_slippage_breach_rate_pct = 5.0
max_connectivity_degraded_window_seconds = 120
max_daily_realized_loss_sol = 0.05
max_consecutive_hard_failures = 3
"#
        .trim_start()
        .to_string()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}_{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }
}
