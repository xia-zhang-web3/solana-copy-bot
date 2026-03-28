use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_install_target.rs"]
mod tiny_live_activation_install_target;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
mod tiny_live_activation_live_execute;

type ServiceControlWrapperVerificationSummary =
    tiny_live_activation_live_execute::ServiceControlWrapperVerificationSummary;
type LoadedRenderedConfigArtifact =
    tiny_live_activation_live_execute::tiny_live_activation_execute::LoadedRenderedConfigArtifact;
type InstallTargetPackageSummary = tiny_live_activation_install_target::InstallTargetPackageSummary;

const USAGE: &str = "usage: copybot_tiny_live_activation_package --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--activation-config-source <path>] [--rollback-config-source <path>] [--output-dir <path>] [--package-dir <path>] [--json] (--plan-package | --export-package | --verify-package)";
const PACKAGE_VERSION: &str = "1";
const MANIFEST_FILE_NAME: &str = "tiny_live_activation.package.json";
const ACTIVATION_ARTIFACT_RELATIVE_PATH: &str = "artifacts/rendered.activation.toml";
const ROLLBACK_ARTIFACT_RELATIVE_PATH: &str = "artifacts/rendered.rollback.toml";
const WRAPPER_RELATIVE_PATH: &str = "wrapper/copybot-live-service-control";
const WRAPPER_VERIFICATION_RELATIVE_PATH: &str = "contracts/wrapper.verification.json";
const INSTALL_TARGET_SUMMARY_RELATIVE_PATH: &str = "contracts/install-target.contract.json";

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
    output_dir: Option<PathBuf>,
    package_dir: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanPackage,
    ExportPackage,
    VerifyPackage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveActivationPackageVerdict {
    TinyLiveActivationPackagePlanReady,
    TinyLiveActivationPackageExported,
    TinyLiveActivationPackageVerifyOk,
    TinyLiveActivationPackageVerifyInvalid,
    TinyLiveActivationPackageExportRefused,
    TinyLiveActivationPackageHashMismatch,
    TinyLiveActivationPackageWrapperInvalid,
    TinyLiveActivationPackageInstallContractInvalid,
}

#[derive(Debug, Clone)]
struct PackagePaths {
    package_dir: PathBuf,
    manifest_path: PathBuf,
    activation_artifact_path: PathBuf,
    activation_metadata_path: PathBuf,
    rollback_artifact_path: PathBuf,
    rollback_metadata_path: PathBuf,
    wrapper_path: PathBuf,
    wrapper_verification_path: PathBuf,
    install_target_summary_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagedFileHash {
    label: String,
    relative_path: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagedRenderedArtifactSummary {
    render_kind: String,
    config_relative_path: String,
    metadata_relative_path: String,
    source_config_path: String,
    source_config_fingerprint_sha256: String,
    pre_activation_gate_verdict: String,
    pre_activation_gate_reason: String,
    activation_plan_verdict: String,
    activation_plan_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WrapperContractSummary {
    wrapper_relative_path: String,
    wrapper_verification_relative_path: String,
    wrapper_version: String,
    backend_command: String,
    timeout_ms: u64,
    supported_actions: Vec<String>,
    status_schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageManifest {
    package_version: String,
    exported_at: DateTime<Utc>,
    activation_artifact: PackagedRenderedArtifactSummary,
    rollback_artifact: PackagedRenderedArtifactSummary,
    wrapper_contract: WrapperContractSummary,
    install_target_summary_relative_path: String,
    install_target_command_summary: String,
    live_execute_verify_command_summary: String,
    watch_live_command_summary: String,
    cutover_plan_command_summary: String,
    files: Vec<PackagedFileHash>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageVerificationSummary {
    manifest_present: bool,
    manifest_version_supported: Option<bool>,
    activation_artifact_verified: Option<bool>,
    rollback_artifact_verified: Option<bool>,
    wrapper_verified: Option<bool>,
    install_target_summary_verified: Option<bool>,
    hash_mismatches: Vec<String>,
    path_mismatches: Vec<String>,
    mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveActivationPackageVerdict,
    reason: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    activation_config_source_path: Option<String>,
    rollback_config_source_path: Option<String>,
    output_dir: Option<String>,
    package_dir: Option<String>,
    manifest_path: Option<String>,
    activation_artifact_path: Option<String>,
    activation_metadata_path: Option<String>,
    rollback_artifact_path: Option<String>,
    rollback_metadata_path: Option<String>,
    wrapper_path: Option<String>,
    wrapper_verification_path: Option<String>,
    install_target_summary_path: Option<String>,
    install_target_command_summary: Option<String>,
    live_execute_verify_command_summary: Option<String>,
    watch_live_command_summary: Option<String>,
    cutover_plan_command_summary: Option<String>,
    wrapper_verification: Option<ServiceControlWrapperVerificationSummary>,
    install_target_summary: Option<InstallTargetPackageSummary>,
    verification: Option<PackageVerificationSummary>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct VerifiedActivationPackageContract {
    pub(crate) package_dir: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) activation_artifact_path: PathBuf,
    pub(crate) activation_metadata_path: PathBuf,
    pub(crate) rollback_artifact_path: PathBuf,
    pub(crate) rollback_metadata_path: PathBuf,
    pub(crate) wrapper_path: PathBuf,
    pub(crate) wrapper_verification_path: PathBuf,
    pub(crate) install_target_summary_path: PathBuf,
    pub(crate) wrapper_verification: ServiceControlWrapperVerificationSummary,
    pub(crate) install_target_summary: InstallTargetPackageSummary,
    pub(crate) install_target_command_summary: String,
    pub(crate) live_execute_verify_command_summary: String,
    pub(crate) watch_live_command_summary: String,
    pub(crate) cutover_plan_command_summary: String,
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
    let mut output_dir: Option<PathBuf> = None;
    let mut package_dir: Option<PathBuf> = None;
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
            "--output-dir" => {
                output_dir = Some(PathBuf::from(parse_string_arg(
                    "--output-dir",
                    args.next(),
                )?))
            }
            "--package-dir" => {
                package_dir = Some(PathBuf::from(parse_string_arg(
                    "--package-dir",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--plan-package" => set_mode(&mut mode, Mode::PlanPackage, "--plan-package")?,
            "--export-package" => set_mode(&mut mode, Mode::ExportPackage, "--export-package")?,
            "--verify-package" => set_mode(&mut mode, Mode::VerifyPackage, "--verify-package")?,
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
        Mode::PlanPackage | Mode::ExportPackage => {
            if activation_config_source_path.is_none() {
                bail!("missing required --activation-config-source");
            }
            if rollback_config_source_path.is_none() {
                bail!("missing required --rollback-config-source");
            }
        }
        Mode::VerifyPackage => {}
    }
    if matches!(mode, Mode::ExportPackage) && output_dir.is_none() {
        bail!("missing required --output-dir for --export-package");
    }
    if matches!(mode, Mode::VerifyPackage) && package_dir.is_none() {
        bail!("missing required --package-dir for --verify-package");
    }

    Ok(Some(Config {
        mode,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
        activation_config_source_path,
        rollback_config_source_path,
        output_dir,
        package_dir,
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
        Mode::PlanPackage => match plan_package_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveActivationPackageVerdict::TinyLiveActivationPackageExportRefused,
                error.to_string(),
            ),
        },
        Mode::ExportPackage => match export_package_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveActivationPackageVerdict::TinyLiveActivationPackageExportRefused,
                error.to_string(),
            ),
        },
        Mode::VerifyPackage => match verify_package_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLiveActivationPackageVerdict::TinyLiveActivationPackageVerifyInvalid,
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

fn plan_package_report(config: &Config) -> Result<PackageReport> {
    let contract = validate_package_contract(config)?;
    Ok(base_report(
        config,
        None,
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackagePlanReady,
        format!(
            "tiny-live activation package is ready to export for install root {} without authorizing production activation",
            config.install_root.display()
        ),
        Some(contract.wrapper_verification),
        Some(contract.install_target_summary),
        None,
        Some(contract.install_target_command_summary),
    ))
}

fn export_package_report(config: &Config) -> Result<PackageReport> {
    let contract = validate_package_contract(config)?;
    let output_dir = config.output_dir.as_ref().expect("checked output dir");
    validate_new_directory_path(output_dir, "package output dir")?;
    let paths = package_paths(output_dir);
    create_package_directories(&paths)?;
    copy_artifact_pair_into_package(&contract.activation, &contract.rollback, &paths)?;
    render_wrapper_into_package(config, &paths)?;
    write_json_file(
        &paths.wrapper_verification_path,
        &contract.wrapper_verification,
    )?;
    write_json_file(
        &paths.install_target_summary_path,
        &contract.install_target_summary,
    )?;

    let manifest = build_manifest(config, &contract, &paths)?;
    write_json_file(&paths.manifest_path, &manifest)?;

    Ok(base_report(
        config,
        Some(&paths),
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageExported,
        format!(
            "tiny-live activation package exported deterministically to {}",
            output_dir.display()
        ),
        Some(contract.wrapper_verification),
        Some(contract.install_target_summary),
        None,
        Some(contract.install_target_command_summary),
    ))
}

fn verify_package_report(config: &Config) -> Result<PackageReport> {
    let package_dir = config.package_dir.as_ref().expect("checked package dir");
    validate_existing_path(package_dir, "package dir", PathKind::Directory, true)?;
    let paths = package_paths(package_dir);
    let verification = inspect_package(config, &paths)?;
    let verdict = determine_verify_verdict(&verification);
    let reason = if verification.mismatches.is_empty() {
        format!(
            "tiny-live activation package {} matches the deterministic artifact/hash/wrapper/install contract",
            package_dir.display()
        )
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package verification failed".to_string())
    };
    let manifest = load_manifest(&paths.manifest_path).ok();
    let wrapper_verification = manifest
        .as_ref()
        .and_then(|_| load_wrapper_verification_summary(&paths.wrapper_verification_path).ok());
    let install_target_summary = manifest
        .as_ref()
        .and_then(|_| load_install_target_summary(&paths.install_target_summary_path).ok());
    Ok(base_report(
        config,
        Some(&paths),
        verdict,
        reason,
        wrapper_verification,
        install_target_summary,
        Some(verification),
        manifest.map(|value| value.install_target_command_summary),
    ))
}

#[allow(dead_code)]
pub(crate) fn verify_package_contract_for_deploy(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
) -> Result<VerifiedActivationPackageContract> {
    let config = Config {
        mode: Mode::VerifyPackage,
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        activation_config_source_path: None,
        rollback_config_source_path: None,
        output_dir: None,
        package_dir: Some(package_dir.to_path_buf()),
        json: false,
    };
    validate_existing_path(package_dir, "package dir", PathKind::Directory, true)?;
    let paths = package_paths(package_dir);
    let verification = inspect_package(&config, &paths)?;
    if !verification.mismatches.is_empty() {
        bail!(
            "{}",
            verification
                .mismatches
                .first()
                .cloned()
                .unwrap_or_else(|| "package verification failed".to_string())
        );
    }
    let manifest = load_manifest(&paths.manifest_path)?;
    let wrapper_verification = load_wrapper_verification_summary(&paths.wrapper_verification_path)?;
    let install_target_summary = load_install_target_summary(&paths.install_target_summary_path)?;
    Ok(VerifiedActivationPackageContract {
        package_dir: package_dir.to_path_buf(),
        manifest_path: paths.manifest_path,
        activation_artifact_path: paths.activation_artifact_path,
        activation_metadata_path: paths.activation_metadata_path,
        rollback_artifact_path: paths.rollback_artifact_path,
        rollback_metadata_path: paths.rollback_metadata_path,
        wrapper_path: paths.wrapper_path,
        wrapper_verification_path: paths.wrapper_verification_path,
        install_target_summary_path: paths.install_target_summary_path,
        wrapper_verification,
        install_target_summary,
        install_target_command_summary: manifest.install_target_command_summary,
        live_execute_verify_command_summary: manifest.live_execute_verify_command_summary,
        watch_live_command_summary: manifest.watch_live_command_summary,
        cutover_plan_command_summary: manifest.cutover_plan_command_summary,
    })
}

#[allow(dead_code)]
pub(crate) fn export_activation_package_for_deploy(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    activation_config_source_path: &Path,
    rollback_config_source_path: &Path,
) -> Result<VerifiedActivationPackageContract> {
    let config = Config {
        mode: Mode::ExportPackage,
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        activation_config_source_path: Some(activation_config_source_path.to_path_buf()),
        rollback_config_source_path: Some(rollback_config_source_path.to_path_buf()),
        output_dir: Some(package_dir.to_path_buf()),
        package_dir: None,
        json: false,
    };
    export_package_report(&config)?;
    verify_package_contract_for_deploy(
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
    )
}

#[derive(Debug, Clone)]
struct PackageContractContext {
    activation: LoadedRenderedConfigArtifact,
    rollback: LoadedRenderedConfigArtifact,
    wrapper_verification: ServiceControlWrapperVerificationSummary,
    install_target_summary: InstallTargetPackageSummary,
    install_target_command_summary: String,
}

fn validate_package_contract(config: &Config) -> Result<PackageContractContext> {
    validate_static_package_inputs(config)?;
    let activation_path = config
        .activation_config_source_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing activation config source path"))?;
    let rollback_path = config
        .rollback_config_source_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing rollback config source path"))?;
    validate_existing_path(
        activation_path,
        "activation config source",
        PathKind::File,
        true,
    )?;
    validate_existing_path(
        rollback_path,
        "rollback config source",
        PathKind::File,
        true,
    )?;
    let install_target_summary =
        tiny_live_activation_install_target::build_install_target_package_summary(
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            activation_path,
            rollback_path,
        )?;
    let install_target_summary_mismatches =
        tiny_live_activation_install_target::verify_install_target_package_summary(
            &install_target_summary,
        );
    if !install_target_summary_mismatches.is_empty() {
        bail!("{}", install_target_summary_mismatches.join("; "));
    }
    let (activation, rollback) =
        tiny_live_activation_live_execute::load_validated_artifact_pair_for_live_target(
            activation_path,
            rollback_path,
        )?;
    let wrapper_contents = tiny_live_activation_live_execute::live_service_control_wrapper_contract::render_wrapper_script_contents(
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    let wrapper_temp_dir = temp_validation_dir("tiny_live_activation_package_wrapper");
    let wrapper_temp_path = wrapper_temp_dir.join("copybot-live-service-control");
    write_text_file(&wrapper_temp_path, &wrapper_contents)?;
    #[cfg(unix)]
    set_executable(&wrapper_temp_path)?;
    let wrapper_verification =
        tiny_live_activation_live_execute::inspect_service_control_wrapper_contract_for_live_target(
            &wrapper_temp_path,
        )?;
    fs::remove_dir_all(&wrapper_temp_dir).ok();
    let install_target_command_summary = format!(
        "copybot_tiny_live_activation_install_target --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --activation-config-source <package-dir>/{} --rollback-config-source <package-dir>/{} --install-target",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        ACTIVATION_ARTIFACT_RELATIVE_PATH,
        ROLLBACK_ARTIFACT_RELATIVE_PATH,
    );
    Ok(PackageContractContext {
        activation,
        rollback,
        wrapper_verification,
        install_target_summary,
        install_target_command_summary,
    })
}

fn validate_static_package_inputs(config: &Config) -> Result<()> {
    tiny_live_activation_live_execute::validate_target_service_name_for_live_target(
        &config.target_service_name,
    )?;
    validate_existing_path(
        &config.install_root,
        "install root",
        PathKind::Directory,
        false,
    )?;
    tiny_live_activation_live_execute::live_service_control_wrapper_contract::render_wrapper_script_contents(
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
    .map(|_| ())
}

fn base_report(
    config: &Config,
    paths: Option<&PackagePaths>,
    verdict: TinyLiveActivationPackageVerdict,
    reason: String,
    wrapper_verification: Option<ServiceControlWrapperVerificationSummary>,
    install_target_summary: Option<InstallTargetPackageSummary>,
    verification: Option<PackageVerificationSummary>,
    install_target_command_summary: Option<String>,
) -> PackageReport {
    PackageReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        install_root: config.install_root.display().to_string(),
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
        output_dir: config.output_dir.as_ref().map(|path| path.display().to_string()),
        package_dir: config
            .package_dir
            .as_ref()
            .or(config.output_dir.as_ref())
            .map(|path| path.display().to_string()),
        manifest_path: paths.map(|value| value.manifest_path.display().to_string()),
        activation_artifact_path: paths.map(|value| value.activation_artifact_path.display().to_string()),
        activation_metadata_path: paths.map(|value| value.activation_metadata_path.display().to_string()),
        rollback_artifact_path: paths.map(|value| value.rollback_artifact_path.display().to_string()),
        rollback_metadata_path: paths.map(|value| value.rollback_metadata_path.display().to_string()),
        wrapper_path: paths.map(|value| value.wrapper_path.display().to_string()),
        wrapper_verification_path: paths.map(|value| value.wrapper_verification_path.display().to_string()),
        install_target_summary_path: paths.map(|value| value.install_target_summary_path.display().to_string()),
        install_target_command_summary,
        live_execute_verify_command_summary: install_target_summary
            .as_ref()
            .map(|value| value.live_execute_verify_command_summary.clone()),
        watch_live_command_summary: install_target_summary
            .as_ref()
            .map(|value| value.watch_live_command_summary.clone()),
        cutover_plan_command_summary: install_target_summary
            .as_ref()
            .map(|value| value.cutover_plan_command_summary.clone()),
        wrapper_verification,
        install_target_summary,
        verification,
        activation_authorized: false,
        explicit_statement:
            "this activation package only captures bounded activation artifacts, wrapper truth, install-target contract, and live cutover summaries; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLiveActivationPackageVerdict,
    reason: String,
) -> PackageReport {
    let output_path = config.output_dir.as_ref().or(config.package_dir.as_ref());
    let paths = output_path.map(|path| package_paths(path));
    base_report(
        config,
        paths.as_ref(),
        verdict,
        reason,
        None,
        None,
        None,
        None,
    )
}

fn determine_verify_verdict(
    verification: &PackageVerificationSummary,
) -> TinyLiveActivationPackageVerdict {
    if verification.mismatches.is_empty() {
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageVerifyOk
    } else if verification
        .mismatches
        .iter()
        .any(|item| item.contains("install target") || item.contains("install_target"))
    {
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
    } else if verification
        .mismatches
        .iter()
        .any(|item| item.contains("wrapper"))
    {
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageWrapperInvalid
    } else if !verification.hash_mismatches.is_empty() {
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageHashMismatch
    } else {
        TinyLiveActivationPackageVerdict::TinyLiveActivationPackageVerifyInvalid
    }
}

fn build_manifest(
    config: &Config,
    contract: &PackageContractContext,
    paths: &PackagePaths,
) -> Result<PackageManifest> {
    let files = vec![
        file_hash(
            "activation_config",
            ACTIVATION_ARTIFACT_RELATIVE_PATH,
            &paths.activation_artifact_path,
        )?,
        file_hash(
            "activation_metadata",
            &relative_metadata_path(ACTIVATION_ARTIFACT_RELATIVE_PATH),
            &paths.activation_metadata_path,
        )?,
        file_hash(
            "rollback_config",
            ROLLBACK_ARTIFACT_RELATIVE_PATH,
            &paths.rollback_artifact_path,
        )?,
        file_hash(
            "rollback_metadata",
            &relative_metadata_path(ROLLBACK_ARTIFACT_RELATIVE_PATH),
            &paths.rollback_metadata_path,
        )?,
        file_hash("wrapper", WRAPPER_RELATIVE_PATH, &paths.wrapper_path)?,
        file_hash(
            "wrapper_verification",
            WRAPPER_VERIFICATION_RELATIVE_PATH,
            &paths.wrapper_verification_path,
        )?,
        file_hash(
            "install_target_summary",
            INSTALL_TARGET_SUMMARY_RELATIVE_PATH,
            &paths.install_target_summary_path,
        )?,
    ];
    Ok(PackageManifest {
        package_version: PACKAGE_VERSION.to_string(),
        exported_at: Utc::now(),
        activation_artifact: artifact_summary(
            &contract.activation,
            ACTIVATION_ARTIFACT_RELATIVE_PATH,
            &relative_metadata_path(ACTIVATION_ARTIFACT_RELATIVE_PATH),
        ),
        rollback_artifact: artifact_summary(
            &contract.rollback,
            ROLLBACK_ARTIFACT_RELATIVE_PATH,
            &relative_metadata_path(ROLLBACK_ARTIFACT_RELATIVE_PATH),
        ),
        wrapper_contract: WrapperContractSummary {
            wrapper_relative_path: WRAPPER_RELATIVE_PATH.to_string(),
            wrapper_verification_relative_path: WRAPPER_VERIFICATION_RELATIVE_PATH.to_string(),
            wrapper_version: contract
                .wrapper_verification
                .wrapper_version
                .clone()
                .unwrap_or_else(|| "<missing>".to_string()),
            backend_command: config.backend_command.clone(),
            timeout_ms: config.wrapper_timeout_ms,
            supported_actions: contract.wrapper_verification.supported_actions.clone(),
            status_schema_version: contract
                .wrapper_verification
                .status_schema_version
                .clone()
                .unwrap_or_else(|| "<missing>".to_string()),
        },
        install_target_summary_relative_path: INSTALL_TARGET_SUMMARY_RELATIVE_PATH.to_string(),
        install_target_command_summary: contract.install_target_command_summary.clone(),
        live_execute_verify_command_summary: contract
            .install_target_summary
            .live_execute_verify_command_summary
            .clone(),
        watch_live_command_summary: contract
            .install_target_summary
            .watch_live_command_summary
            .clone(),
        cutover_plan_command_summary: contract
            .install_target_summary
            .cutover_plan_command_summary
            .clone(),
        files,
        activation_authorized: false,
        explicit_statement:
            "this activation package only captures immutable activation/cutover inputs; it does not authorize production activation by itself"
                .to_string(),
    })
}

fn inspect_package(config: &Config, paths: &PackagePaths) -> Result<PackageVerificationSummary> {
    let mut mismatches = Vec::new();
    let mut hash_mismatches = Vec::new();
    let mut path_mismatches = Vec::new();

    let manifest_present = paths.manifest_path.exists();
    if !manifest_present {
        mismatches.push(format!(
            "package manifest {} is missing",
            paths.manifest_path.display()
        ));
        return Ok(PackageVerificationSummary {
            manifest_present,
            manifest_version_supported: None,
            activation_artifact_verified: None,
            rollback_artifact_verified: None,
            wrapper_verified: None,
            install_target_summary_verified: None,
            hash_mismatches,
            path_mismatches,
            mismatches,
        });
    }
    let manifest = load_manifest(&paths.manifest_path)?;
    let manifest_version_supported = Some(manifest.package_version == PACKAGE_VERSION);
    if manifest.package_version != PACKAGE_VERSION {
        mismatches.push(format!(
            "package manifest version must be {}, found {}",
            PACKAGE_VERSION, manifest.package_version
        ));
    }
    if manifest.activation_authorized {
        mismatches.push("package manifest activation_authorized must remain false".to_string());
    }

    let expected_relative_paths = BTreeMap::from([
        (
            "activation_config".to_string(),
            ACTIVATION_ARTIFACT_RELATIVE_PATH.to_string(),
        ),
        (
            "activation_metadata".to_string(),
            relative_metadata_path(ACTIVATION_ARTIFACT_RELATIVE_PATH),
        ),
        (
            "rollback_config".to_string(),
            ROLLBACK_ARTIFACT_RELATIVE_PATH.to_string(),
        ),
        (
            "rollback_metadata".to_string(),
            relative_metadata_path(ROLLBACK_ARTIFACT_RELATIVE_PATH),
        ),
        ("wrapper".to_string(), WRAPPER_RELATIVE_PATH.to_string()),
        (
            "wrapper_verification".to_string(),
            WRAPPER_VERIFICATION_RELATIVE_PATH.to_string(),
        ),
        (
            "install_target_summary".to_string(),
            INSTALL_TARGET_SUMMARY_RELATIVE_PATH.to_string(),
        ),
    ]);
    let file_entries = manifest
        .files
        .iter()
        .map(|entry| (entry.label.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for (label, expected_relative_path) in &expected_relative_paths {
        let Some(entry) = file_entries.get(label) else {
            mismatches.push(format!(
                "package manifest is missing file hash entry for {label}"
            ));
            continue;
        };
        if entry.relative_path != *expected_relative_path {
            path_mismatches.push(format!(
                "package manifest relative_path for {label} must be {expected_relative_path}, found {}",
                entry.relative_path
            ));
            continue;
        }
        let resolved = safe_join_package_path(&paths.package_dir, &entry.relative_path)?;
        if sha256_hex(
            &fs::read(&resolved)
                .with_context(|| format!("failed reading {}", resolved.display()))?,
        ) != entry.sha256
        {
            hash_mismatches.push(format!(
                "package hash mismatch for {label}: expected {}, found {}",
                entry.sha256,
                sha256_hex(&fs::read(&resolved).unwrap_or_default())
            ));
        }
    }
    if !path_mismatches.is_empty() {
        mismatches.extend(path_mismatches.clone());
    }
    if !hash_mismatches.is_empty() {
        mismatches.extend(hash_mismatches.clone());
    }

    let activation_artifact_verified = match tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(&paths.activation_artifact_path) {
        Ok(artifact) => {
            let expected = &manifest.activation_artifact;
            if expected.render_kind != "activation"
                || expected.config_relative_path != ACTIVATION_ARTIFACT_RELATIVE_PATH
                || expected.metadata_relative_path
                    != relative_metadata_path(ACTIVATION_ARTIFACT_RELATIVE_PATH)
                || expected.source_config_path != artifact.metadata.input_config_path
                || expected.source_config_fingerprint_sha256
                    != artifact.metadata.source_config_fingerprint_sha256
                || expected.pre_activation_gate_verdict != artifact.metadata.pre_activation_gate_verdict
                || expected.activation_plan_verdict != artifact.metadata.activation_plan_verdict
            {
                mismatches.push(
                    "package activation artifact summary does not match the packaged activation artifact"
                        .to_string(),
                );
                Some(false)
            } else {
                Some(true)
            }
        }
        Err(error) => {
            mismatches.push(format!(
                "packaged activation artifact is invalid: {error:#}"
            ));
            Some(false)
        }
    };

    let rollback_artifact_verified = match tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(&paths.rollback_artifact_path) {
        Ok(artifact) => {
            let expected = &manifest.rollback_artifact;
            if expected.render_kind != "rollback"
                || expected.config_relative_path != ROLLBACK_ARTIFACT_RELATIVE_PATH
                || expected.metadata_relative_path
                    != relative_metadata_path(ROLLBACK_ARTIFACT_RELATIVE_PATH)
                || expected.source_config_path != artifact.metadata.input_config_path
                || expected.source_config_fingerprint_sha256
                    != artifact.metadata.source_config_fingerprint_sha256
                || expected.pre_activation_gate_verdict != artifact.metadata.pre_activation_gate_verdict
                || expected.activation_plan_verdict != artifact.metadata.activation_plan_verdict
            {
                mismatches.push(
                    "package rollback artifact summary does not match the packaged rollback artifact"
                        .to_string(),
                );
                Some(false)
            } else {
                Some(true)
            }
        }
        Err(error) => {
            mismatches.push(format!("packaged rollback artifact is invalid: {error:#}"));
            Some(false)
        }
    };

    let wrapper_summary = load_wrapper_verification_summary(&paths.wrapper_verification_path)?;
    let wrapper_verified = match tiny_live_activation_live_execute::inspect_service_control_wrapper_contract_for_live_target(&paths.wrapper_path) {
        Ok(actual) => {
            if !wrapper_verification_matches(&actual, &wrapper_summary) {
                mismatches.push(
                    "packaged wrapper verification summary does not match the packaged wrapper artifact"
                        .to_string(),
                );
                Some(false)
            } else if manifest.wrapper_contract.wrapper_relative_path != WRAPPER_RELATIVE_PATH
                || manifest.wrapper_contract.wrapper_verification_relative_path
                    != WRAPPER_VERIFICATION_RELATIVE_PATH
                || manifest.wrapper_contract.backend_command
                    != wrapper_summary.backend_command.clone().unwrap_or_default()
                || manifest.wrapper_contract.timeout_ms
                    != wrapper_summary.timeout_ms.unwrap_or_default()
                || manifest.wrapper_contract.wrapper_version
                    != wrapper_summary.wrapper_version.clone().unwrap_or_default()
                || manifest.wrapper_contract.supported_actions
                    != wrapper_summary.supported_actions
                || manifest.wrapper_contract.status_schema_version
                    != wrapper_summary.status_schema_version.clone().unwrap_or_default()
            {
                mismatches.push(
                    "package wrapper contract summary does not match the packaged wrapper verification truth"
                        .to_string(),
                );
                Some(false)
            } else {
                Some(true)
            }
        }
        Err(error) => {
            mismatches.push(format!("packaged wrapper is invalid: {error:#}"));
            Some(false)
        }
    };

    let install_target_summary = load_install_target_summary(&paths.install_target_summary_path)?;
    let install_target_summary_mismatches =
        tiny_live_activation_install_target::verify_install_target_package_summary(
            &install_target_summary,
        );
    let requested_target_mismatches =
        verify_requested_target_contract(config, &install_target_summary);
    let install_target_summary_verified = if install_target_summary_mismatches.is_empty()
        && requested_target_mismatches.is_empty()
        && manifest.install_target_summary_relative_path == INSTALL_TARGET_SUMMARY_RELATIVE_PATH
        && manifest.live_execute_verify_command_summary
            == install_target_summary.live_execute_verify_command_summary
        && manifest.watch_live_command_summary == install_target_summary.watch_live_command_summary
        && manifest.cutover_plan_command_summary
            == install_target_summary.cutover_plan_command_summary
    {
        Some(true)
    } else {
        for mismatch in install_target_summary_mismatches {
            mismatches.push(format!("install target contract mismatch: {mismatch}"));
        }
        for mismatch in requested_target_mismatches {
            mismatches.push(format!("install target contract mismatch: {mismatch}"));
        }
        if manifest.install_target_summary_relative_path != INSTALL_TARGET_SUMMARY_RELATIVE_PATH {
            mismatches.push(format!(
                "package install_target_summary_relative_path must be {}, found {}",
                INSTALL_TARGET_SUMMARY_RELATIVE_PATH, manifest.install_target_summary_relative_path
            ));
        }
        if manifest.live_execute_verify_command_summary
            != install_target_summary.live_execute_verify_command_summary
        {
            mismatches.push(
                "package live_execute_verify_command_summary does not match install target summary"
                    .to_string(),
            );
        }
        if manifest.watch_live_command_summary != install_target_summary.watch_live_command_summary
        {
            mismatches.push(
                "package watch_live_command_summary does not match install target summary"
                    .to_string(),
            );
        }
        if manifest.cutover_plan_command_summary
            != install_target_summary.cutover_plan_command_summary
        {
            mismatches.push(
                "package cutover_plan_command_summary does not match install target summary"
                    .to_string(),
            );
        }
        Some(false)
    };

    if !manifest
        .install_target_command_summary
        .contains("<package-dir>/artifacts/rendered.activation.toml")
        || !manifest
            .install_target_command_summary
            .contains("<package-dir>/artifacts/rendered.rollback.toml")
    {
        mismatches.push(
            "package install_target_command_summary must reference the packaged activation assets via <package-dir>/..."
                .to_string(),
        );
    }

    Ok(PackageVerificationSummary {
        manifest_present,
        manifest_version_supported,
        activation_artifact_verified,
        rollback_artifact_verified,
        wrapper_verified,
        install_target_summary_verified,
        hash_mismatches,
        path_mismatches,
        mismatches,
    })
}

fn verify_requested_target_contract(
    config: &Config,
    summary: &InstallTargetPackageSummary,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    let requested_install_root = config.install_root.display().to_string();
    if summary.install_root != requested_install_root {
        mismatches.push(format!(
            "packaged install_root {} does not match requested --install-root {}",
            summary.install_root, requested_install_root
        ));
    }
    if summary.target_service_name != config.target_service_name {
        mismatches.push(format!(
            "packaged target_service_name {} does not match requested --target-service {}",
            summary.target_service_name, config.target_service_name
        ));
    }
    if summary.backend_command != config.backend_command {
        mismatches.push(format!(
            "packaged backend_command {} does not match requested --backend-command {}",
            summary.backend_command, config.backend_command
        ));
    }
    if summary.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "packaged wrapper_timeout_ms {} does not match requested --wrapper-timeout-ms {}",
            summary.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    mismatches
}

fn load_manifest(path: &Path) -> Result<PackageManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading package manifest {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing package manifest {}", path.display()))
}

fn load_wrapper_verification_summary(
    path: &Path,
) -> Result<ServiceControlWrapperVerificationSummary> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading wrapper verification {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing wrapper verification {}", path.display()))
}

fn load_install_target_summary(path: &Path) -> Result<InstallTargetPackageSummary> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading install target summary {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing install target summary {}", path.display()))
}

fn wrapper_verification_matches(
    actual: &ServiceControlWrapperVerificationSummary,
    expected: &ServiceControlWrapperVerificationSummary,
) -> bool {
    actual.wrapper_version == expected.wrapper_version
        && actual.backend_command == expected.backend_command
        && actual.timeout_ms == expected.timeout_ms
        && actual.supported_actions == expected.supported_actions
        && actual.status_schema_version == expected.status_schema_version
        && actual.executable == expected.executable
        && actual.exact_content_matches_expected == expected.exact_content_matches_expected
        && actual.mismatches == expected.mismatches
}

fn artifact_summary(
    artifact: &LoadedRenderedConfigArtifact,
    config_relative_path: &str,
    metadata_relative_path: &str,
) -> PackagedRenderedArtifactSummary {
    PackagedRenderedArtifactSummary {
        render_kind: match artifact.metadata.render_kind {
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Activation => "activation".to_string(),
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Rollback => "rollback".to_string(),
        },
        config_relative_path: config_relative_path.to_string(),
        metadata_relative_path: metadata_relative_path.to_string(),
        source_config_path: artifact.metadata.input_config_path.clone(),
        source_config_fingerprint_sha256: artifact.metadata.source_config_fingerprint_sha256.clone(),
        pre_activation_gate_verdict: artifact.metadata.pre_activation_gate_verdict.clone(),
        pre_activation_gate_reason: artifact.metadata.pre_activation_gate_reason.clone(),
        activation_plan_verdict: artifact.metadata.activation_plan_verdict.clone(),
        activation_plan_reason: artifact.metadata.activation_plan_reason.clone(),
    }
}

fn package_paths(package_dir: &Path) -> PackagePaths {
    PackagePaths {
        package_dir: package_dir.to_path_buf(),
        manifest_path: package_dir.join(MANIFEST_FILE_NAME),
        activation_artifact_path: package_dir.join(ACTIVATION_ARTIFACT_RELATIVE_PATH),
        activation_metadata_path: package_dir
            .join(relative_metadata_path(ACTIVATION_ARTIFACT_RELATIVE_PATH)),
        rollback_artifact_path: package_dir.join(ROLLBACK_ARTIFACT_RELATIVE_PATH),
        rollback_metadata_path: package_dir
            .join(relative_metadata_path(ROLLBACK_ARTIFACT_RELATIVE_PATH)),
        wrapper_path: package_dir.join(WRAPPER_RELATIVE_PATH),
        wrapper_verification_path: package_dir.join(WRAPPER_VERIFICATION_RELATIVE_PATH),
        install_target_summary_path: package_dir.join(INSTALL_TARGET_SUMMARY_RELATIVE_PATH),
    }
}

fn relative_metadata_path(config_relative_path: &str) -> String {
    tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(
        Path::new(config_relative_path),
    )
        .display()
        .to_string()
}

fn create_package_directories(paths: &PackagePaths) -> Result<()> {
    fs::create_dir_all(&paths.package_dir).with_context(|| {
        format!(
            "failed creating package dir {}",
            paths.package_dir.display()
        )
    })?;
    for path in [
        paths.activation_artifact_path.parent(),
        paths.wrapper_path.parent(),
        paths.wrapper_verification_path.parent(),
        paths.install_target_summary_path.parent(),
    ]
    .into_iter()
    .flatten()
    {
        fs::create_dir_all(path)
            .with_context(|| format!("failed creating package subdir {}", path.display()))?;
    }
    Ok(())
}

fn copy_artifact_pair_into_package(
    activation: &LoadedRenderedConfigArtifact,
    rollback: &LoadedRenderedConfigArtifact,
    paths: &PackagePaths,
) -> Result<()> {
    copy_file(
        &activation.rendered_config_path,
        &paths.activation_artifact_path,
    )?;
    copy_file(&activation.metadata_path, &paths.activation_metadata_path)?;
    copy_file(
        &rollback.rendered_config_path,
        &paths.rollback_artifact_path,
    )?;
    copy_file(&rollback.metadata_path, &paths.rollback_metadata_path)?;
    tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(
        &paths.activation_artifact_path,
    )?;
    tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(
        &paths.rollback_artifact_path,
    )?;
    Ok(())
}

fn render_wrapper_into_package(config: &Config, paths: &PackagePaths) -> Result<()> {
    tiny_live_activation_live_execute::live_service_control_wrapper_contract::render_wrapper_script(
        &paths.wrapper_path,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
}

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    let bytes = fs::read(source)
        .with_context(|| format!("failed reading source file {}", source.display()))?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(destination, bytes)
        .with_context(|| format!("failed writing destination file {}", destination.display()))
}

fn file_hash(label: &str, relative_path: &str, absolute_path: &Path) -> Result<PackagedFileHash> {
    Ok(PackagedFileHash {
        label: label.to_string(),
        relative_path: relative_path.to_string(),
        sha256: sha256_hex(
            &fs::read(absolute_path)
                .with_context(|| format!("failed reading {}", absolute_path.display()))?,
        ),
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanPackage => "plan_package",
        Mode::ExportPackage => "export_package",
        Mode::VerifyPackage => "verify_package",
    }
}

fn render_human_report(report: &PackageReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("install_root={}", report.install_root),
        format!("target_service_name={}", report.target_service_name),
        format!("backend_command={}", report.backend_command),
        format!("wrapper_timeout_ms={}", report.wrapper_timeout_ms),
    ];
    if let Some(path) = &report.package_dir {
        lines.push(format!("package_dir={path}"));
    }
    if let Some(path) = &report.manifest_path {
        lines.push(format!("manifest_path={path}"));
    }
    if let Some(path) = &report.activation_artifact_path {
        lines.push(format!("activation_artifact_path={path}"));
    }
    if let Some(path) = &report.rollback_artifact_path {
        lines.push(format!("rollback_artifact_path={path}"));
    }
    if let Some(path) = &report.wrapper_path {
        lines.push(format!("wrapper_path={path}"));
    }
    if let Some(summary) = &report.install_target_command_summary {
        lines.push(format!("install_target_command_summary={summary}"));
    }
    if let Some(summary) = &report.live_execute_verify_command_summary {
        lines.push(format!("live_execute_verify_command_summary={summary}"));
    }
    if let Some(summary) = &report.watch_live_command_summary {
        lines.push(format!("watch_live_command_summary={summary}"));
    }
    if let Some(summary) = &report.cutover_plan_command_summary {
        lines.push(format!("cutover_plan_command_summary={summary}"));
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
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "<unknown>".to_string())
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn validate_new_directory_path(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} must be an absolute path");
    }
    if path.exists() {
        bail!("refusing to overwrite existing {label} {}", path.display());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{label} {} has no parent", path.display()))?;
    validate_existing_path(
        parent,
        &format!("{label} parent"),
        PathKind::Directory,
        true,
    )
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
                        "{label} path component {} is a symlink; symlinked package paths are not allowed",
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

fn safe_join_package_path(package_dir: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = Path::new(relative_path);
    if relative.is_absolute() {
        bail!(
            "package relative path {} must not be absolute",
            relative_path
        );
    }
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        bail!(
            "package relative path {} must not escape the package dir",
            relative_path
        );
    }
    let joined = package_dir.join(relative);
    if !joined.starts_with(package_dir) {
        bail!(
            "package relative path {} must stay under package dir {}",
            relative_path,
            package_dir.display()
        );
    }
    Ok(joined)
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn temp_validation_dir(label: &str) -> PathBuf {
    for attempt in 0..128u32 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "{label}_{}_{}_{}_{}",
            std::process::id(),
            nanos,
            attempt,
            std::thread::current().name().unwrap_or("unnamed")
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("failed creating temp validation dir {}: {error}", path.display()),
        }
    }
    panic!("failed to allocate unique temp validation dir for {label}");
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed reading metadata for {}", path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed setting executable bit for {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::load_from_path;
    #[test]
    fn export_package_succeeds_for_valid_contract() {
        let fixture = package_fixture("tiny_live_activation_package_export");
        let report = export_package_report(&fixture.config).expect("export");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageExported
        );
        assert!(fixture.package_dir.join(MANIFEST_FILE_NAME).exists());
    }

    #[test]
    fn verify_package_is_green_for_coherent_export() {
        let fixture = package_fixture("tiny_live_activation_package_verify_green");
        export_package_report(&fixture.config).expect("export");
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageVerifyOk
        );
    }

    #[test]
    fn verify_package_rejects_wrong_install_root() {
        let fixture = package_fixture("tiny_live_activation_package_wrong_install_root");
        export_package_report(&fixture.config).expect("export");
        let wrong_root = fixture.fixture_dir.join("other-root");
        fs::create_dir_all(&wrong_root).unwrap();
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            install_root: wrong_root,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
        );
        assert!(report.reason.contains("--install-root"));
    }

    #[test]
    fn verify_package_rejects_wrong_target_service() {
        let fixture = package_fixture("tiny_live_activation_package_wrong_target_service");
        export_package_report(&fixture.config).expect("export");
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            target_service_name: "other.service".to_string(),
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
        );
        assert!(report.reason.contains("--target-service"));
    }

    #[test]
    fn verify_package_rejects_wrong_backend_or_wrapper_timeout() {
        let fixture = package_fixture("tiny_live_activation_package_wrong_backend_timeout");
        export_package_report(&fixture.config).expect("export");
        let backend_report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            backend_command: "launchctl".to_string(),
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            backend_report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
        );
        assert!(backend_report.reason.contains("--backend-command"));

        let timeout_report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            wrapper_timeout_ms: fixture.config.wrapper_timeout_ms + 1,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            timeout_report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
        );
        assert!(timeout_report.reason.contains("--wrapper-timeout-ms"));
    }

    #[test]
    fn tampered_activation_artifact_hash_is_rejected() {
        let fixture = package_fixture("tiny_live_activation_package_bad_hash");
        export_package_report(&fixture.config).expect("export");
        fs::write(
            fixture.package_dir.join(ACTIVATION_ARTIFACT_RELATIVE_PATH),
            sample_safe_config_toml(),
        )
        .unwrap();
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageHashMismatch
        );
    }

    #[cfg(unix)]
    #[test]
    fn tampered_wrapper_artifact_or_summary_is_rejected() {
        let fixture = package_fixture("tiny_live_activation_package_bad_wrapper");
        export_package_report(&fixture.config).expect("export");
        write_non_wrapper(&fixture.package_dir.join(WRAPPER_RELATIVE_PATH));
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageWrapperInvalid
        );
        let summary_fixture = package_fixture("tiny_live_activation_package_bad_wrapper_summary");
        export_package_report(&summary_fixture.config).expect("export");
        let summary_path = summary_fixture
            .package_dir
            .join(WRAPPER_VERIFICATION_RELATIVE_PATH);
        let mut summary: ServiceControlWrapperVerificationSummary =
            serde_json::from_str(&fs::read_to_string(&summary_path).unwrap()).unwrap();
        summary.backend_command = Some("tampered-backend".to_string());
        write_json_file(&summary_path, &summary).unwrap();
        let summary_report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(summary_fixture.package_dir.clone()),
            ..summary_fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            summary_report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageWrapperInvalid
        );
    }

    #[test]
    fn broken_install_target_summary_is_rejected() {
        let fixture = package_fixture("tiny_live_activation_package_bad_install");
        export_package_report(&fixture.config).expect("export");
        let summary_path = fixture
            .package_dir
            .join(INSTALL_TARGET_SUMMARY_RELATIVE_PATH);
        let mut summary: InstallTargetPackageSummary =
            serde_json::from_str(&fs::read_to_string(&summary_path).unwrap()).unwrap();
        summary.runtime_dir = "/outside/runtime".to_string();
        write_json_file(&summary_path, &summary).unwrap();
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageInstallContractInvalid
        );
    }

    #[test]
    fn path_traversal_references_are_rejected() {
        let fixture = package_fixture("tiny_live_activation_package_escape");
        export_package_report(&fixture.config).expect("export");
        let manifest_path = fixture.package_dir.join(MANIFEST_FILE_NAME);
        let mut manifest: PackageManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest.files[0].relative_path = "../escape.toml".to_string();
        write_json_file(&manifest_path, &manifest).unwrap();
        let report = verify_package_report(&Config {
            mode: Mode::VerifyPackage,
            activation_config_source_path: None,
            rollback_config_source_path: None,
            output_dir: None,
            package_dir: Some(fixture.package_dir.clone()),
            ..fixture.config.clone()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveActivationPackageVerdict::TinyLiveActivationPackageVerifyInvalid
        );
        assert!(report.reason.contains("escape") || report.reason.contains("relative path"));
    }

    struct PackageFixture {
        fixture_dir: PathBuf,
        package_dir: PathBuf,
        config: Config,
    }

    fn package_fixture(label: &str) -> PackageFixture {
        let fixture_dir = temp_dir(label);
        let install_root = fixture_dir.join("root");
        let target_config_path = install_root.join("etc/solana-copy-bot/live.server.toml");
        fs::create_dir_all(target_config_path.parent().unwrap()).unwrap();
        fs::write(&target_config_path, sample_safe_config_toml()).unwrap();

        let source_dir = fixture_dir.join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let activation_config_source_path = source_dir.join("rendered.activation.toml");
        let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
        write_rendered_artifact_pair(
            &target_config_path,
            &activation_config_source_path,
            &rollback_config_source_path,
        );

        let package_dir = fixture_dir.join("package");
        PackageFixture {
            fixture_dir,
            package_dir: package_dir.clone(),
            config: Config {
                mode: Mode::PlanPackage,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: "systemctl".to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                activation_config_source_path: Some(activation_config_source_path),
                rollback_config_source_path: Some(rollback_config_source_path),
                output_dir: Some(package_dir),
                package_dir: None,
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

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        loop {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "{label}_{}_{}_{}",
                std::process::id(),
                unique,
                counter
            ));
            match fs::create_dir(&path) {
                Ok(()) => return path,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("failed creating temp dir {}: {error}", path.display()),
            }
        }
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }
}
