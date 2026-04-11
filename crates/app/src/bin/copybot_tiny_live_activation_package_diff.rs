use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_diff [--root <path> --session-dir <path> | --left-session-dir <path> --right-session-dir <path>] [--json] (--latest-vs-session | --session-vs-session | --verify-no-drift)";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PACKAGE_FILE_PREFIX: &str = "tiny_live_activation_package_";
const SESSION_FILE_SUFFIX: &str = ".session.json";
const STATUS_FILE_SUFFIX: &str = ".status.json";
const REPORT_FILE_SUFFIX: &str = ".report.json";
const NOT_AUTHORIZED_SUMMARY: &str =
    "This tiny-live package diff surface only audits persisted package session/status/report artifacts. It stays planning-safe, does not authorize activation, and does not override the Stage 3 production gate.";

const PACKAGE_CHAIN_STEPS: &[&str] = &[
    "turn_green",
    "execute_frozen",
    "decision_packet",
    "handoff_bundle",
    "review_receipt",
    "activation_ticket",
    "release_capsule",
    "attestation_seal",
    "provenance_certificate",
    "notarization_receipt",
    "registry_entry",
    "filing_certificate",
    "archive_receipt",
    "closure_certificate",
    "finality_receipt",
    "consummation_record",
    "completion_certificate",
    "culmination_receipt",
    "summit_certificate",
    "pinnacle_receipt",
    "capstone_certificate",
    "keystone_receipt",
    "cornerstone_certificate",
    "foundation_receipt",
    "bedrock_certificate",
    "basal_receipt",
    "substructure_certificate",
    "plinth_receipt",
    "pedestal_certificate",
    "dais_receipt",
    "rostrum_certificate",
    "podium_receipt",
    "lectern_certificate",
    "pulpit_receipt",
    "chancel_certificate",
    "apse_receipt",
    "sanctuary_certificate",
    "nave_receipt",
    "transept_certificate",
    "choir_receipt",
    "clerestory_certificate",
];

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
    root: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    left_session_dir: Option<PathBuf>,
    right_session_dir: Option<PathBuf>,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    LatestVsSession,
    SessionVsSession,
    VerifyNoDrift,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageDiffVerdict {
    TinyLivePackageDiffAvailable,
    TinyLivePackageDiffEmpty,
    TinyLivePackageDiffInvalid,
    TinyLivePackageDiffVerifyOk,
    TinyLivePackageDiffVerifyInvalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactKind {
    Session,
    Status,
    Report,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Status => "status",
            Self::Report => "report",
        }
    }
}

#[derive(Debug, Clone)]
struct PackageSessionArtifact {
    package_step_name: String,
    step_rank: usize,
    session_dir: PathBuf,
    generated_at: Option<DateTime<Utc>>,
    file_modified_at: Option<DateTime<Utc>>,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    session_json: Option<Value>,
    status_json: Option<Value>,
    invalid_reasons: Vec<String>,
}

impl PackageSessionArtifact {
    fn ordering_time(&self) -> Option<DateTime<Utc>> {
        self.generated_at.or(self.file_modified_at)
    }
}

#[derive(Debug, Clone)]
struct LineageStepSurface {
    artifact: PackageSessionArtifact,
    next_lower_step_name: Option<String>,
    next_lower_session_dir: Option<PathBuf>,
    next_lower_report_path: Option<PathBuf>,
    next_lower_report_exists: bool,
    next_lower_report_parse_ok: bool,
    next_lower_identity_summary_link: Option<String>,
    next_lower_identity_sha256_link: Option<String>,
    next_lower_identity_algorithm_link: Option<String>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct LineageResolution {
    top_step_name: String,
    top_session_dir: PathBuf,
    lineage: Vec<LineageStepSurface>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct ManifestArtifactEntry {
    artifact_key: String,
    exists: bool,
    parseable: bool,
    size_bytes: Option<u64>,
    sha256_digest: Option<String>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct ManifestSurface {
    manifest_digest_sha256: String,
    required_artifact_count: usize,
    present_artifact_count: usize,
    parseable_artifact_count: usize,
    invalid_reasons: Vec<String>,
    artifacts: Vec<ManifestArtifactEntry>,
}

#[derive(Debug, Clone)]
struct ComparisonSide {
    source: String,
    top_step_name: String,
    top_session_dir: PathBuf,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    ordered_lineage_summary: Option<String>,
    lineage_entries: Vec<ComparableLineageEntry>,
    manifest_digest_sha256: String,
    manifest_entries: Vec<ManifestArtifactEntry>,
    required_artifact_count: usize,
    present_artifact_count: usize,
    parseable_artifact_count: usize,
    continuity_valid: bool,
    manifest_valid: bool,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct ComparableLineageEntry {
    step_name: String,
    next_lower_step_name: Option<String>,
    next_lower_identity_summary: Option<String>,
    next_lower_identity_sha256: Option<String>,
    next_lower_identity_algorithm: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DiffChainSummary {
    source: String,
    top_step_name: String,
    top_session_path: String,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    ordered_lineage_summary: Option<String>,
    manifest_digest_sha256: String,
    required_artifact_count: usize,
    present_artifact_count: usize,
    parseable_artifact_count: usize,
    continuity_valid: bool,
    manifest_valid: bool,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DiffEntry {
    category: String,
    key: String,
    left_value: Option<String>,
    right_value: Option<String>,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct PackageDiffReport {
    mode: String,
    verdict: TinyLivePackageDiffVerdict,
    reason: String,
    left_chain: Option<DiffChainSummary>,
    right_chain: Option<DiffChainSummary>,
    comparable: bool,
    top_identity_matches: bool,
    lineage_matches: bool,
    manifest_matches: bool,
    byte_identical_at_bounded_artifact_level: bool,
    diff_entries: Vec<DiffEntry>,
    planning_safe_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut root: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut left_session_dir: Option<PathBuf> = None;
    let mut right_session_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = Some(PathBuf::from(parse_required_value("--root", args.next())?));
            }
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_required_value(
                    "--session-dir",
                    args.next(),
                )?));
            }
            "--left-session-dir" => {
                left_session_dir = Some(PathBuf::from(parse_required_value(
                    "--left-session-dir",
                    args.next(),
                )?));
            }
            "--right-session-dir" => {
                right_session_dir = Some(PathBuf::from(parse_required_value(
                    "--right-session-dir",
                    args.next(),
                )?));
            }
            "--latest-vs-session" => {
                set_mode(&mut mode, Mode::LatestVsSession, "--latest-vs-session")?
            }
            "--session-vs-session" => {
                set_mode(&mut mode, Mode::SessionVsSession, "--session-vs-session")?
            }
            "--verify-no-drift" => set_mode(&mut mode, Mode::VerifyNoDrift, "--verify-no-drift")?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        bail!(
            "exactly one of --latest-vs-session, --session-vs-session, or --verify-no-drift is required"
        );
    };

    match mode {
        Mode::LatestVsSession | Mode::VerifyNoDrift => {
            if root.is_none() {
                bail!("missing required --root");
            }
            if session_dir.is_none() {
                bail!("missing required --session-dir");
            }
            if left_session_dir.is_some() || right_session_dir.is_some() {
                bail!("--left-session-dir/--right-session-dir are only valid with --session-vs-session");
            }
        }
        Mode::SessionVsSession => {
            if left_session_dir.is_none() {
                bail!("missing required --left-session-dir");
            }
            if right_session_dir.is_none() {
                bail!("missing required --right-session-dir");
            }
            if root.is_some() || session_dir.is_some() {
                bail!("--root/--session-dir are only valid with --latest-vs-session or --verify-no-drift");
            }
        }
    }

    Ok(Some(Config {
        root,
        session_dir,
        left_session_dir,
        right_session_dir,
        json,
        mode,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::LatestVsSession => build_latest_vs_session_report(&config)?,
        Mode::SessionVsSession => build_session_vs_session_report(&config)?,
        Mode::VerifyNoDrift => build_verify_no_drift_report(&config)?,
    };
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing tiny-live package diff report json")
    } else {
        Ok(render_diff_human(&report))
    }
}

fn build_latest_vs_session_report(config: &Config) -> Result<PackageDiffReport> {
    let root = config.root.as_ref().expect("validated root");
    let latest = resolve_latest_side(root, "latest_root")?;
    let Some(left_side) = latest else {
        return Ok(empty_diff_report(
            "latest_vs_session",
            format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
        ));
    };
    let right_session_dir = config.session_dir.as_ref().expect("validated session dir");
    let right_side = resolve_session_side(
        right_session_dir,
        format!("session {}", right_session_dir.display()),
    )?;
    Ok(compare_sides(
        "latest_vs_session",
        left_side,
        right_side,
        false,
    ))
}

fn build_session_vs_session_report(config: &Config) -> Result<PackageDiffReport> {
    let left_session_dir = config
        .left_session_dir
        .as_ref()
        .expect("validated left session dir");
    let right_session_dir = config
        .right_session_dir
        .as_ref()
        .expect("validated right session dir");
    let left_side = resolve_session_side(
        left_session_dir,
        format!("session {}", left_session_dir.display()),
    )?;
    let right_side = resolve_session_side(
        right_session_dir,
        format!("session {}", right_session_dir.display()),
    )?;
    Ok(compare_sides(
        "session_vs_session",
        left_side,
        right_side,
        false,
    ))
}

fn build_verify_no_drift_report(config: &Config) -> Result<PackageDiffReport> {
    let root = config.root.as_ref().expect("validated root");
    let latest = resolve_latest_side(root, "latest_root")?;
    let Some(left_side) = latest else {
        return Ok(empty_diff_report(
            "verify_no_drift",
            format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
        ));
    };
    let right_session_dir = config.session_dir.as_ref().expect("validated session dir");
    let right_side = resolve_session_side(
        right_session_dir,
        format!("session {}", right_session_dir.display()),
    )?;
    Ok(compare_sides(
        "verify_no_drift",
        left_side,
        right_side,
        true,
    ))
}

fn empty_diff_report(mode: &str, reason: String) -> PackageDiffReport {
    PackageDiffReport {
        mode: mode.to_string(),
        verdict: TinyLivePackageDiffVerdict::TinyLivePackageDiffEmpty,
        reason,
        left_chain: None,
        right_chain: None,
        comparable: false,
        top_identity_matches: false,
        lineage_matches: false,
        manifest_matches: false,
        byte_identical_at_bounded_artifact_level: false,
        diff_entries: Vec::new(),
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    }
}

fn resolve_latest_side(root: &Path, source_label: &str) -> Result<Option<ComparisonSide>> {
    let sessions = scan_package_sessions(root)?;
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Ok(None);
    };
    let resolution =
        resolve_lineage_from_top(&candidate.session_dir, &candidate.package_step_name)?;
    Ok(Some(build_comparison_side(
        source_label.to_string(),
        resolution,
    )))
}

fn resolve_session_side(session_dir: &Path, source_label: String) -> Result<ComparisonSide> {
    let resolution = resolve_lineage_from_session_dir(session_dir)?;
    Ok(build_comparison_side(source_label, resolution))
}

fn build_comparison_side(source: String, resolution: LineageResolution) -> ComparisonSide {
    let manifest = build_manifest_surface(&resolution);
    let top = resolution.lineage.last().expect("non-empty lineage");
    let continuity_invalid_reasons = resolution.invalid_reasons.clone();
    let manifest_invalid_reasons = manifest.invalid_reasons.clone();
    let mut invalid_reasons = continuity_invalid_reasons.clone();
    invalid_reasons.extend(manifest_invalid_reasons.iter().cloned());
    invalid_reasons.dedup();

    ComparisonSide {
        source,
        top_step_name: resolution.top_step_name.clone(),
        top_session_dir: resolution.top_session_dir.clone(),
        top_identity_summary: top.artifact.top_identity_summary.clone(),
        top_identity_sha256: top.artifact.top_identity_sha256.clone(),
        top_identity_algorithm: top.artifact.top_identity_algorithm.clone(),
        ordered_lineage_summary: summarize_lineage_chain(&resolution.lineage),
        lineage_entries: resolution
            .lineage
            .iter()
            .map(|node| ComparableLineageEntry {
                step_name: node.artifact.package_step_name.clone(),
                next_lower_step_name: node.next_lower_step_name.clone(),
                next_lower_identity_summary: node.next_lower_identity_summary_link.clone(),
                next_lower_identity_sha256: node.next_lower_identity_sha256_link.clone(),
                next_lower_identity_algorithm: node.next_lower_identity_algorithm_link.clone(),
            })
            .collect(),
        manifest_digest_sha256: manifest.manifest_digest_sha256,
        manifest_entries: manifest.artifacts,
        required_artifact_count: manifest.required_artifact_count,
        present_artifact_count: manifest.present_artifact_count,
        parseable_artifact_count: manifest.parseable_artifact_count,
        continuity_valid: continuity_invalid_reasons.is_empty(),
        manifest_valid: manifest_invalid_reasons.is_empty(),
        invalid_reasons,
    }
}

fn compare_sides(
    mode: &str,
    left_side: ComparisonSide,
    right_side: ComparisonSide,
    verify_mode: bool,
) -> PackageDiffReport {
    let left_summary = summarize_side(&left_side);
    let right_summary = summarize_side(&right_side);

    let mut diff_entries = Vec::new();
    let comparable = left_side.invalid_reasons.is_empty() && right_side.invalid_reasons.is_empty();

    if !left_side.invalid_reasons.is_empty() {
        diff_entries.extend(left_side.invalid_reasons.iter().map(|reason| DiffEntry {
            category: "left_invalid".to_string(),
            key: "left_chain".to_string(),
            left_value: Some(reason.clone()),
            right_value: None,
            reason: format!("left chain is not comparable: {reason}"),
        }));
    }
    if !right_side.invalid_reasons.is_empty() {
        diff_entries.extend(right_side.invalid_reasons.iter().map(|reason| DiffEntry {
            category: "right_invalid".to_string(),
            key: "right_chain".to_string(),
            left_value: None,
            right_value: Some(reason.clone()),
            reason: format!("right chain is not comparable: {reason}"),
        }));
    }

    let mut top_identity_matches = false;
    let mut lineage_matches = false;
    let mut manifest_matches = false;

    if comparable {
        diff_entries.extend(compare_top_identity(&left_side, &right_side));
        let top_identity_entry_count = diff_entries.len();
        diff_entries.extend(compare_lineage(&left_side, &right_side));
        let lineage_entry_count = diff_entries.len();
        diff_entries.extend(compare_manifest(&left_side, &right_side));
        top_identity_matches = top_identity_entry_count == 0;
        lineage_matches = lineage_entry_count == top_identity_entry_count;
        manifest_matches = diff_entries.len() == lineage_entry_count;
    }

    let byte_identical_at_bounded_artifact_level = comparable && manifest_matches;

    let mut invalid_reasons = Vec::new();
    if verify_mode {
        if left_side.top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
            invalid_reasons.push(format!(
                "left chain stops at {} below the current top accepted layer {}",
                left_side.top_step_name, TOP_ACCEPTED_PACKAGE_STEP
            ));
        }
        if right_side.top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
            invalid_reasons.push(format!(
                "right chain stops at {} below the current top accepted layer {}",
                right_side.top_step_name, TOP_ACCEPTED_PACKAGE_STEP
            ));
        }
    }
    if !comparable {
        invalid_reasons.extend(left_side.invalid_reasons.iter().cloned());
        invalid_reasons.extend(right_side.invalid_reasons.iter().cloned());
    }
    if verify_mode && comparable && !top_identity_matches {
        invalid_reasons
            .push("top identity drift detected between compared persisted chains".to_string());
    }
    if verify_mode && comparable && !lineage_matches {
        invalid_reasons
            .push("lineage drift detected between compared persisted chains".to_string());
    }
    if verify_mode && comparable && !manifest_matches {
        invalid_reasons
            .push("artifact manifest drift detected between compared persisted chains".to_string());
    }
    invalid_reasons.dedup();

    let (verdict, reason) = if invalid_reasons.is_empty() {
        if verify_mode {
            (
                TinyLivePackageDiffVerdict::TinyLivePackageDiffVerifyOk,
                "both persisted immutable tiny-live package chains are comparable and show no top-identity, lineage, or manifest drift".to_string(),
            )
        } else if top_identity_matches && lineage_matches && manifest_matches {
            (
                TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable,
                "both persisted immutable tiny-live package chains are comparable and identical at the bounded artifact level".to_string(),
            )
        } else {
            (
                TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable,
                "both persisted immutable tiny-live package chains are comparable but drift exists in top identity, lineage, or artifact manifest state".to_string(),
            )
        }
    } else if verify_mode {
        (
            TinyLivePackageDiffVerdict::TinyLivePackageDiffVerifyInvalid,
            invalid_reasons.join("; "),
        )
    } else {
        (
            TinyLivePackageDiffVerdict::TinyLivePackageDiffInvalid,
            invalid_reasons.join("; "),
        )
    };

    PackageDiffReport {
        mode: mode.to_string(),
        verdict,
        reason,
        left_chain: Some(left_summary),
        right_chain: Some(right_summary),
        comparable,
        top_identity_matches,
        lineage_matches,
        manifest_matches,
        byte_identical_at_bounded_artifact_level,
        diff_entries,
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    }
}

fn compare_top_identity(left: &ComparisonSide, right: &ComparisonSide) -> Vec<DiffEntry> {
    let mut entries = Vec::new();
    push_diff_if_mismatch(
        &mut entries,
        "top_identity",
        "top_step_name",
        Some(left.top_step_name.as_str()),
        Some(right.top_step_name.as_str()),
    );
    push_diff_if_mismatch(
        &mut entries,
        "top_identity",
        "top_identity_summary",
        left.top_identity_summary.as_deref(),
        right.top_identity_summary.as_deref(),
    );
    push_diff_if_mismatch(
        &mut entries,
        "top_identity",
        "top_identity_sha256",
        left.top_identity_sha256.as_deref(),
        right.top_identity_sha256.as_deref(),
    );
    push_diff_if_mismatch(
        &mut entries,
        "top_identity",
        "top_identity_algorithm",
        left.top_identity_algorithm.as_deref(),
        right.top_identity_algorithm.as_deref(),
    );
    entries
}

fn compare_lineage(left: &ComparisonSide, right: &ComparisonSide) -> Vec<DiffEntry> {
    let mut entries = Vec::new();
    push_diff_if_mismatch(
        &mut entries,
        "lineage",
        "ordered_lineage_summary",
        left.ordered_lineage_summary.as_deref(),
        right.ordered_lineage_summary.as_deref(),
    );

    let max_len = left.lineage_entries.len().max(right.lineage_entries.len());
    for index in 0..max_len {
        let left_entry = left.lineage_entries.get(index);
        let right_entry = right.lineage_entries.get(index);
        if left_entry.is_none() || right_entry.is_none() {
            entries.push(DiffEntry {
                category: "lineage".to_string(),
                key: format!("lineage[{index}]"),
                left_value: left_entry.map(render_lineage_entry),
                right_value: right_entry.map(render_lineage_entry),
                reason: format!("lineage entry count drift at position {index}"),
            });
            continue;
        }
        let left_entry = left_entry.expect("checked left entry");
        let right_entry = right_entry.expect("checked right entry");
        push_diff_if_mismatch(
            &mut entries,
            "lineage",
            &format!("lineage[{index}].step_name"),
            Some(left_entry.step_name.as_str()),
            Some(right_entry.step_name.as_str()),
        );
        push_diff_if_mismatch(
            &mut entries,
            "lineage",
            &format!("lineage[{index}].next_lower_step_name"),
            left_entry.next_lower_step_name.as_deref(),
            right_entry.next_lower_step_name.as_deref(),
        );
        push_diff_if_mismatch(
            &mut entries,
            "lineage",
            &format!("lineage[{index}].next_lower_identity_summary"),
            left_entry.next_lower_identity_summary.as_deref(),
            right_entry.next_lower_identity_summary.as_deref(),
        );
        push_diff_if_mismatch(
            &mut entries,
            "lineage",
            &format!("lineage[{index}].next_lower_identity_sha256"),
            left_entry.next_lower_identity_sha256.as_deref(),
            right_entry.next_lower_identity_sha256.as_deref(),
        );
        push_diff_if_mismatch(
            &mut entries,
            "lineage",
            &format!("lineage[{index}].next_lower_identity_algorithm"),
            left_entry.next_lower_identity_algorithm.as_deref(),
            right_entry.next_lower_identity_algorithm.as_deref(),
        );
    }
    entries
}

fn compare_manifest(left: &ComparisonSide, right: &ComparisonSide) -> Vec<DiffEntry> {
    let mut entries = Vec::new();
    push_diff_if_mismatch(
        &mut entries,
        "manifest",
        "manifest_digest_sha256",
        Some(left.manifest_digest_sha256.as_str()),
        Some(right.manifest_digest_sha256.as_str()),
    );

    let left_map = left
        .manifest_entries
        .iter()
        .map(|entry| (entry.artifact_key.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let right_map = right
        .manifest_entries
        .iter()
        .map(|entry| (entry.artifact_key.clone(), entry))
        .collect::<BTreeMap<_, _>>();

    let keys = left_map
        .keys()
        .chain(right_map.keys())
        .cloned()
        .collect::<HashSet<_>>();
    let mut keys = keys.into_iter().collect::<Vec<_>>();
    keys.sort();

    for key in keys {
        let left_entry = left_map.get(&key);
        let right_entry = right_map.get(&key);
        if left_entry.is_none() || right_entry.is_none() {
            entries.push(DiffEntry {
                category: "manifest".to_string(),
                key,
                left_value: left_entry.map(|entry| render_manifest_entry(entry)),
                right_value: right_entry.map(|entry| render_manifest_entry(entry)),
                reason: "artifact membership drift detected".to_string(),
            });
            continue;
        }
        let left_entry = left_entry.expect("checked left entry");
        let right_entry = right_entry.expect("checked right entry");
        push_diff_if_mismatch(
            &mut entries,
            "manifest",
            &format!("{}.exists", key),
            Some(if left_entry.exists { "true" } else { "false" }),
            Some(if right_entry.exists { "true" } else { "false" }),
        );
        push_diff_if_mismatch(
            &mut entries,
            "manifest",
            &format!("{}.parseable", key),
            Some(if left_entry.parseable {
                "true"
            } else {
                "false"
            }),
            Some(if right_entry.parseable {
                "true"
            } else {
                "false"
            }),
        );
        push_diff_if_mismatch(
            &mut entries,
            "manifest",
            &format!("{}.size_bytes", key),
            left_entry
                .size_bytes
                .map(|value| value.to_string())
                .as_deref(),
            right_entry
                .size_bytes
                .map(|value| value.to_string())
                .as_deref(),
        );
        push_diff_if_mismatch(
            &mut entries,
            "manifest",
            &format!("{}.sha256_digest", key),
            left_entry.sha256_digest.as_deref(),
            right_entry.sha256_digest.as_deref(),
        );
    }

    entries
}

fn push_diff_if_mismatch(
    entries: &mut Vec<DiffEntry>,
    category: &str,
    key: &str,
    left_value: Option<&str>,
    right_value: Option<&str>,
) {
    if left_value == right_value {
        return;
    }
    entries.push(DiffEntry {
        category: category.to_string(),
        key: key.to_string(),
        left_value: left_value.map(ToOwned::to_owned),
        right_value: right_value.map(ToOwned::to_owned),
        reason: format!("{category} drift detected for {key}"),
    });
}

fn render_lineage_entry(entry: &ComparableLineageEntry) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        entry.step_name,
        entry.next_lower_step_name.as_deref().unwrap_or("<none>"),
        entry
            .next_lower_identity_summary
            .as_deref()
            .unwrap_or("<none>"),
        entry
            .next_lower_identity_sha256
            .as_deref()
            .unwrap_or("<none>"),
        entry
            .next_lower_identity_algorithm
            .as_deref()
            .unwrap_or("<none>")
    )
}

fn render_manifest_entry(entry: &ManifestArtifactEntry) -> String {
    format!(
        "{}|{}|{}|{}",
        if entry.exists { "true" } else { "false" },
        if entry.parseable { "true" } else { "false" },
        entry
            .size_bytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<none>".to_string()),
        entry.sha256_digest.as_deref().unwrap_or("<none>")
    )
}

fn summarize_side(side: &ComparisonSide) -> DiffChainSummary {
    DiffChainSummary {
        source: side.source.clone(),
        top_step_name: side.top_step_name.clone(),
        top_session_path: side.top_session_dir.display().to_string(),
        top_identity_summary: side.top_identity_summary.clone(),
        top_identity_sha256: side.top_identity_sha256.clone(),
        top_identity_algorithm: side.top_identity_algorithm.clone(),
        ordered_lineage_summary: side.ordered_lineage_summary.clone(),
        manifest_digest_sha256: side.manifest_digest_sha256.clone(),
        required_artifact_count: side.required_artifact_count,
        present_artifact_count: side.present_artifact_count,
        parseable_artifact_count: side.parseable_artifact_count,
        continuity_valid: side.continuity_valid,
        manifest_valid: side.manifest_valid,
        invalid_reasons: side.invalid_reasons.clone(),
    }
}

fn build_manifest_surface(resolution: &LineageResolution) -> ManifestSurface {
    let mut artifacts = Vec::new();
    for node in &resolution.lineage {
        let step_name = &node.artifact.package_step_name;
        let session_path = node.artifact.session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
        ));
        artifacts.push(inspect_manifest_artifact(
            artifact_key(step_name, ArtifactKind::Session, None),
            &session_path,
        ));

        let status_path = node.artifact.session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
        ));
        artifacts.push(inspect_manifest_artifact(
            artifact_key(step_name, ArtifactKind::Status, None),
            &status_path,
        ));

        if let Some(lower_step) = node.next_lower_step_name.as_ref() {
            let report_path = node.next_lower_report_path.clone().unwrap_or_else(|| {
                node.artifact.session_dir.join(format!(
                    "{PACKAGE_FILE_PREFIX}{step_name}.{lower_step}{REPORT_FILE_SUFFIX}"
                ))
            });
            artifacts.push(inspect_manifest_artifact(
                artifact_key(step_name, ArtifactKind::Report, Some(lower_step)),
                &report_path,
            ));
        }
    }

    let required_artifact_count = artifacts.len();
    let present_artifact_count = artifacts.iter().filter(|artifact| artifact.exists).count();
    let parseable_artifact_count = artifacts
        .iter()
        .filter(|artifact| artifact.parseable)
        .count();
    let mut invalid_reasons = resolution.invalid_reasons.clone();
    invalid_reasons.extend(
        artifacts
            .iter()
            .flat_map(|artifact| artifact.invalid_reasons.iter().cloned()),
    );
    if present_artifact_count != required_artifact_count {
        invalid_reasons.push(format!(
            "manifest is incomplete: present {present_artifact_count} of {required_artifact_count} required persisted artifacts"
        ));
    }
    if parseable_artifact_count != required_artifact_count {
        invalid_reasons.push(format!(
            "manifest is not fully parseable: parseable {parseable_artifact_count} of {required_artifact_count} required persisted artifacts"
        ));
    }
    invalid_reasons.dedup();

    ManifestSurface {
        manifest_digest_sha256: compute_manifest_digest(&artifacts),
        required_artifact_count,
        present_artifact_count,
        parseable_artifact_count,
        invalid_reasons,
        artifacts,
    }
}

fn artifact_key(step_name: &str, kind: ArtifactKind, nested_step: Option<&str>) -> String {
    format!(
        "{}|{}|{}",
        step_name,
        kind.as_str(),
        nested_step.unwrap_or("<none>")
    )
}

fn inspect_manifest_artifact(artifact_key: String, path: &Path) -> ManifestArtifactEntry {
    let exists = path.exists();
    let size_bytes = fs::metadata(path).ok().map(|metadata| metadata.len());
    let mut parseable = false;
    let mut sha256_digest = None;
    let mut invalid_reasons = Vec::new();

    if !exists {
        invalid_reasons.push(format!(
            "missing required persisted artifact {}",
            path.display()
        ));
    } else {
        match fs::read(path) {
            Ok(bytes) => {
                sha256_digest = Some(sha256_hex(&bytes));
                parseable = serde_json::from_slice::<Value>(&bytes).is_ok();
                if !parseable {
                    invalid_reasons.push(format!(
                        "failed parsing persisted artifact {}",
                        path.display()
                    ));
                }
            }
            Err(error) => {
                invalid_reasons.push(format!(
                    "failed reading persisted artifact {}: {}",
                    path.display(),
                    error
                ));
            }
        }
    }

    ManifestArtifactEntry {
        artifact_key,
        exists,
        parseable,
        size_bytes,
        sha256_digest,
        invalid_reasons,
    }
}

fn compute_manifest_digest(artifacts: &[ManifestArtifactEntry]) -> String {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        hasher.update(artifact.artifact_key.as_bytes());
        hasher.update(b"\n");
        hasher.update(if artifact.exists { "true" } else { "false" }.as_bytes());
        hasher.update(b"\n");
        hasher.update(if artifact.parseable { "true" } else { "false" }.as_bytes());
        hasher.update(b"\n");
        hasher.update(
            artifact
                .size_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            artifact
                .sha256_digest
                .as_deref()
                .unwrap_or("<none>")
                .as_bytes(),
        );
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn scan_package_sessions(root: &Path) -> Result<Vec<PackageSessionArtifact>> {
    if !root.exists() {
        bail!("root {} does not exist", root.display());
    }
    if !root.is_dir() {
        bail!("root {} is not a directory", root.display());
    }

    let mut session_paths = Vec::new();
    collect_session_paths(root, &mut session_paths)?;
    let mut sessions = Vec::new();
    for session_path in session_paths {
        if let Some(surface) = inspect_session_path(&session_path)? {
            sessions.push(surface);
        }
    }
    Ok(sessions)
}

fn collect_session_paths(dir: &Path, session_paths: &mut Vec<PathBuf>) -> Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("failed reading directory {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed reading file type for {}", path.display()))?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_session_paths(&path, session_paths)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if name.starts_with(PACKAGE_FILE_PREFIX) && name.ends_with(SESSION_FILE_SUFFIX) {
            session_paths.push(path);
        }
    }
    Ok(())
}

fn inspect_session_path(session_path: &Path) -> Result<Option<PackageSessionArtifact>> {
    let Some(file_name) = session_path.file_name().and_then(OsStr::to_str) else {
        return Ok(None);
    };
    let Some(step_name) = session_step_name_from_session_file(file_name) else {
        return Ok(None);
    };
    if step_rank(&step_name) == 0 {
        return Ok(None);
    }
    let session_dir = session_path.parent().ok_or_else(|| {
        anyhow!(
            "session path {} has no parent directory",
            session_path.display()
        )
    })?;
    Ok(Some(inspect_package_artifact(session_dir, &step_name)?))
}

fn select_latest_top_chain_candidate(
    sessions: &[PackageSessionArtifact],
) -> Option<&PackageSessionArtifact> {
    let max_rank = sessions.iter().map(|session| session.step_rank).max()?;
    sessions
        .iter()
        .filter(|session| session.step_rank == max_rank)
        .max_by(|left, right| {
            left.ordering_time()
                .cmp(&right.ordering_time())
                .then_with(|| left.session_dir.cmp(&right.session_dir))
        })
}

fn resolve_lineage_from_session_dir(session_dir: &Path) -> Result<LineageResolution> {
    if !session_dir.exists() {
        bail!("session dir {} does not exist", session_dir.display());
    }
    if !session_dir.is_dir() {
        bail!("session dir {} is not a directory", session_dir.display());
    }
    let step_name = detect_session_step_name(session_dir)?;
    resolve_lineage_from_top(session_dir, &step_name)
}

fn resolve_lineage_from_top(session_dir: &Path, step_name: &str) -> Result<LineageResolution> {
    let mut lineage_top_down = Vec::new();
    let mut visited = HashSet::new();
    let mut current_step = step_name.to_string();
    let mut current_session_dir = session_dir.to_path_buf();

    loop {
        let visit_key = format!("{current_step}::{}", current_session_dir.display());
        if !visited.insert(visit_key) {
            let mut node = inspect_lineage_step(&current_session_dir, &current_step)?;
            node.invalid_reasons.push(format!(
                "cycle detected while following persisted lineage at {}",
                current_session_dir.display()
            ));
            lineage_top_down.push(node);
            break;
        }

        let node = inspect_lineage_step(&current_session_dir, &current_step)?;
        let next_step_name = node.next_lower_step_name.clone();
        let next_session_dir = node.next_lower_session_dir.clone();
        lineage_top_down.push(node);

        match (next_step_name, next_session_dir) {
            (Some(next_step_name), Some(next_session_dir)) => {
                current_step = next_step_name;
                current_session_dir = next_session_dir;
            }
            _ => break,
        }
    }

    lineage_top_down.reverse();
    let invalid_reasons = validate_lineage_nodes(&mut lineage_top_down);
    let top = lineage_top_down.last().ok_or_else(|| {
        anyhow!(
            "failed resolving persisted lineage from {}",
            session_dir.display()
        )
    })?;

    Ok(LineageResolution {
        top_step_name: top.artifact.package_step_name.clone(),
        top_session_dir: top.artifact.session_dir.clone(),
        lineage: lineage_top_down,
        invalid_reasons,
    })
}

fn detect_session_step_name(session_dir: &Path) -> Result<String> {
    let mut step_names = Vec::new();
    let entries = fs::read_dir(session_dir)
        .with_context(|| format!("failed reading session dir {}", session_dir.display()))?;
    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed reading entry in {}", session_dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed reading file type for {}", path.display()))?;
        if !file_type.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let Some(step_name) = session_step_name_from_session_file(file_name) else {
            continue;
        };
        if step_rank(&step_name) == 0 {
            continue;
        }
        step_names.push(step_name);
    }

    match step_names.len() {
        0 => bail!(
            "session dir {} does not contain a persisted tiny-live package session artifact",
            session_dir.display()
        ),
        1 => Ok(step_names.remove(0)),
        _ => bail!(
            "session dir {} contains multiple persisted tiny-live package session artifacts",
            session_dir.display()
        ),
    }
}

fn inspect_lineage_step(session_dir: &Path, step_name: &str) -> Result<LineageStepSurface> {
    let artifact = inspect_package_artifact(session_dir, step_name)?;
    let next_lower_step_name = previous_step(step_name).map(ToOwned::to_owned);
    let mut invalid_reasons = artifact.invalid_reasons.clone();

    let next_lower_session_dir = next_lower_step_name.as_ref().and_then(|lower_step| {
        let key = format!("{lower_step}_session_dir");
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &key,
        )
        .map(PathBuf::from)
    });
    let next_lower_report_path = next_lower_step_name.as_ref().map(|lower_step| {
        session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}.{lower_step}{REPORT_FILE_SUFFIX}"
        ))
    });
    let next_lower_report_exists = next_lower_report_path
        .as_ref()
        .map(|path| path.exists())
        .unwrap_or(false);
    let next_lower_report_parse_ok = next_lower_report_path
        .as_ref()
        .map(|path| load_json_value(path).is_ok())
        .unwrap_or(false);
    let next_lower_identity_summary_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_summary"),
        )
    });
    let next_lower_identity_sha256_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_sha256"),
        )
    });
    let next_lower_identity_algorithm_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_algorithm"),
        )
    });

    if let Some(lower_step) = next_lower_step_name.as_ref() {
        let lower_session_dir_key = format!("{lower_step}_session_dir");
        if extract_non_empty_string(artifact.status_json.as_ref(), &lower_session_dir_key).is_none()
            || extract_non_empty_string(artifact.session_json.as_ref(), &lower_session_dir_key)
                .is_none()
        {
            invalid_reasons.push(format!(
                "{} is missing required duplicated {} linkage in session/status artifacts",
                step_name, lower_session_dir_key
            ));
        }
        if next_lower_session_dir.is_none() {
            invalid_reasons.push(format!(
                "{} is missing {}_session_dir",
                step_name, lower_step
            ));
        }
        if let Some(report_path) = next_lower_report_path.as_ref() {
            if !next_lower_report_exists {
                invalid_reasons.push(format!(
                    "{} is missing nested lineage report {}",
                    step_name,
                    report_path.display()
                ));
            } else if !next_lower_report_parse_ok {
                invalid_reasons.push(format!(
                    "{} has unparsable nested lineage report {}",
                    step_name,
                    report_path.display()
                ));
            }
        }
        if next_lower_identity_summary_link.is_none()
            && next_lower_identity_sha256_link.is_none()
            && next_lower_identity_algorithm_link.is_none()
        {
            invalid_reasons.push(format!(
                "{} does not store nested {} identity linkage beyond path/report references",
                step_name, lower_step
            ));
        }

        for key in [
            format!("{lower_step}_session_dir"),
            format!("{lower_step}_summary"),
            format!("{lower_step}_sha256"),
            format!("{lower_step}_algorithm"),
        ] {
            if let Some(reason) = mismatched_string_field(
                artifact.status_json.as_ref(),
                artifact.session_json.as_ref(),
                &key,
                step_name,
            ) {
                invalid_reasons.push(reason);
            }
        }
    }

    Ok(LineageStepSurface {
        artifact,
        next_lower_step_name,
        next_lower_session_dir,
        next_lower_report_path,
        next_lower_report_exists,
        next_lower_report_parse_ok,
        next_lower_identity_summary_link,
        next_lower_identity_sha256_link,
        next_lower_identity_algorithm_link,
        invalid_reasons,
    })
}

fn inspect_package_artifact(session_dir: &Path, step_name: &str) -> Result<PackageSessionArtifact> {
    let session_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
    ));
    let status_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
    ));
    let session_exists = session_path.exists();
    let status_exists = status_path.exists();
    let session_json = if session_exists {
        load_json_value(&session_path).ok()
    } else {
        None
    };
    let status_json = if status_exists {
        load_json_value(&status_path).ok()
    } else {
        None
    };

    let mut invalid_reasons = Vec::new();
    if !session_exists {
        invalid_reasons.push(format!(
            "missing session artifact {}",
            session_path.display()
        ));
    } else if session_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing session artifact {}",
            session_path.display()
        ));
    }
    if !status_exists {
        invalid_reasons.push(format!("missing status artifact {}", status_path.display()));
    } else if status_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing status artifact {}",
            status_path.display()
        ));
    }

    let generated_at = extract_datetime(status_json.as_ref(), "updated_at")
        .or_else(|| extract_datetime(session_json.as_ref(), "planned_at"));
    if generated_at.is_none() {
        invalid_reasons.push(format!(
            "{} is missing generated_at/updated_at persisted timing",
            session_dir.display()
        ));
    }

    let top_identity_summary = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_summary"),
    );
    let top_identity_sha256 = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_sha256"),
    );
    let top_identity_algorithm = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_algorithm"),
    );

    for key in [
        "session_dir".to_string(),
        "result".to_string(),
        format!("{step_name}_summary"),
        format!("{step_name}_sha256"),
        format!("{step_name}_algorithm"),
    ] {
        if let Some(reason) =
            mismatched_string_field(status_json.as_ref(), session_json.as_ref(), &key, step_name)
        {
            invalid_reasons.push(reason);
        }
    }

    validate_expected_path_field(
        session_json.as_ref(),
        "session_dir",
        session_dir,
        step_name,
        "session",
        &mut invalid_reasons,
    );
    validate_expected_path_field(
        status_json.as_ref(),
        "session_dir",
        session_dir,
        step_name,
        "status",
        &mut invalid_reasons,
    );

    let file_modified_at = file_modified_at(&session_path);

    Ok(PackageSessionArtifact {
        package_step_name: step_name.to_string(),
        step_rank: step_rank(step_name),
        session_dir: session_dir.to_path_buf(),
        generated_at,
        file_modified_at,
        top_identity_summary,
        top_identity_sha256,
        top_identity_algorithm,
        session_json,
        status_json,
        invalid_reasons,
    })
}

fn validate_lineage_nodes(nodes: &mut [LineageStepSurface]) -> Vec<String> {
    let mut invalid_reasons = Vec::new();

    if nodes.is_empty() {
        invalid_reasons.push("resolved lineage is empty".to_string());
        return invalid_reasons;
    }

    if nodes[0].artifact.package_step_name != PACKAGE_CHAIN_STEPS[0] {
        let reason = format!(
            "resolved lineage does not begin at {}; root step is {}",
            PACKAGE_CHAIN_STEPS[0], nodes[0].artifact.package_step_name
        );
        nodes[0].invalid_reasons.push(reason.clone());
        invalid_reasons.push(reason);
    }

    let top_rank = nodes
        .last()
        .map(|node| node.artifact.step_rank)
        .unwrap_or(0);
    if nodes.len() != top_rank {
        let reason = format!(
            "resolved lineage length {} does not match top step rank {} for {}",
            nodes.len(),
            top_rank,
            nodes
                .last()
                .map(|node| node.artifact.package_step_name.as_str())
                .unwrap_or("<unknown>")
        );
        if let Some(top) = nodes.last_mut() {
            top.invalid_reasons.push(reason.clone());
        }
        invalid_reasons.push(reason);
    }

    for (index, node) in nodes.iter_mut().enumerate() {
        let expected_step = PACKAGE_CHAIN_STEPS
            .get(index)
            .copied()
            .unwrap_or("<out-of-range>");
        if node.artifact.package_step_name != expected_step {
            let reason = format!(
                "resolved lineage step {} is {}, expected {}",
                index, node.artifact.package_step_name, expected_step
            );
            node.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
    }

    for index in 1..nodes.len() {
        let lower = nodes[index - 1].clone();
        let current = &mut nodes[index];

        if current.next_lower_step_name.as_deref()
            != Some(lower.artifact.package_step_name.as_str())
        {
            let reason = format!(
                "{} does not link to expected lower step {}; stored lower step is {}",
                current.artifact.package_step_name,
                lower.artifact.package_step_name,
                current
                    .next_lower_step_name
                    .as_deref()
                    .unwrap_or("<missing>")
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        if current.next_lower_session_dir.as_ref() != Some(&lower.artifact.session_dir) {
            let reason = format!(
                "{} lower-step session dir drifted: stored {} expected {}",
                current.artifact.package_step_name,
                current
                    .next_lower_session_dir
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing>".to_string()),
                lower.artifact.session_dir.display()
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        if !current.next_lower_report_exists {
            let reason = format!(
                "{} is missing required nested report for {}",
                current.artifact.package_step_name, lower.artifact.package_step_name
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        } else if !current.next_lower_report_parse_ok {
            let reason = format!(
                "{} has unparsable nested report for {}",
                current.artifact.package_step_name, lower.artifact.package_step_name
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        let stored_summary = current.next_lower_identity_summary_link.clone();
        let actual_summary = lower.artifact.top_identity_summary.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "summary",
            stored_summary.as_deref(),
            actual_summary.as_deref(),
        );
        let stored_sha256 = current.next_lower_identity_sha256_link.clone();
        let actual_sha256 = lower.artifact.top_identity_sha256.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "sha256",
            stored_sha256.as_deref(),
            actual_sha256.as_deref(),
        );
        let stored_algorithm = current.next_lower_identity_algorithm_link.clone();
        let actual_algorithm = lower.artifact.top_identity_algorithm.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "algorithm",
            stored_algorithm.as_deref(),
            actual_algorithm.as_deref(),
        );
    }

    for node in nodes.iter() {
        invalid_reasons.extend(node.invalid_reasons.iter().cloned());
    }
    invalid_reasons.dedup();
    invalid_reasons
}

fn compare_nested_identity_link(
    invalid_reasons: &mut Vec<String>,
    current: &mut LineageStepSurface,
    lower: &PackageSessionArtifact,
    field_name: &str,
    stored: Option<&str>,
    actual: Option<&str>,
) {
    match (stored, actual) {
        (Some(stored), Some(actual)) if stored != actual => {
            let reason = format!(
                "{} nested {} {} drifted: stored {} expected {}",
                current.artifact.package_step_name,
                lower.package_step_name,
                field_name,
                stored,
                actual
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
        (Some(stored), None) => {
            let reason = format!(
                "{} stores nested {} {} {} but the lower persisted step does not expose that identity field",
                current.artifact.package_step_name, lower.package_step_name, field_name, stored
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
        _ => {}
    }
}

fn summarize_lineage_chain(lineage: &[LineageStepSurface]) -> Option<String> {
    if lineage.is_empty() {
        return None;
    }
    Some(
        lineage
            .iter()
            .map(|node| node.artifact.package_step_name.as_str())
            .collect::<Vec<_>>()
            .join(" -> "),
    )
}

fn previous_step(step_name: &str) -> Option<&'static str> {
    let rank = step_rank(step_name);
    if rank <= 1 {
        None
    } else {
        PACKAGE_CHAIN_STEPS.get(rank - 2).copied()
    }
}

fn session_step_name_from_session_file(file_name: &str) -> Option<String> {
    let trimmed = file_name.strip_prefix(PACKAGE_FILE_PREFIX)?;
    let step = trimmed.strip_suffix(SESSION_FILE_SUFFIX)?;
    if step.is_empty() {
        None
    } else {
        Some(step.to_string())
    }
}

fn step_rank(step_name: &str) -> usize {
    PACKAGE_CHAIN_STEPS
        .iter()
        .position(|candidate| *candidate == step_name)
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn load_json_value(path: &Path) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn extract_datetime(value: Option<&Value>, key: &str) -> Option<DateTime<Utc>> {
    value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_string(status: Option<&Value>, session: Option<&Value>, key: &str) -> Option<String> {
    extract_non_empty_string(status, key).or_else(|| extract_non_empty_string(session, key))
}

fn extract_non_empty_string(value: Option<&Value>, key: &str) -> Option<String> {
    let candidate = value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|raw| !raw.is_empty())?;
    Some(candidate.to_string())
}

fn mismatched_string_field(
    status: Option<&Value>,
    session: Option<&Value>,
    key: &str,
    step_name: &str,
) -> Option<String> {
    let status_value = extract_non_empty_string(status, key);
    let session_value = extract_non_empty_string(session, key);
    match (status_value, session_value) {
        (Some(status_value), Some(session_value)) if status_value != session_value => {
            Some(format!(
                "{} persisted {} drifted between status ({}) and session ({})",
                step_name, key, status_value, session_value
            ))
        }
        _ => None,
    }
}

fn validate_expected_path_field(
    value: Option<&Value>,
    key: &str,
    expected: &Path,
    step_name: &str,
    source_name: &str,
    invalid_reasons: &mut Vec<String>,
) {
    if let Some(stored) = extract_non_empty_string(value, key) {
        if PathBuf::from(&stored) != expected {
            invalid_reasons.push(format!(
                "{} {} {} drifted: stored {} expected {}",
                step_name,
                source_name,
                key,
                stored,
                expected.display()
            ));
        }
    }
}

fn parse_required_value(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("duplicate mode flag provided: {flag}");
    }
    Ok(())
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<serialization-error>".to_string())
}

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .map(DateTime::<Utc>::from)
}

fn render_diff_human(report: &PackageDiffReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_diff".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("comparable={}", report.comparable),
        format!("top_identity_matches={}", report.top_identity_matches),
        format!("lineage_matches={}", report.lineage_matches),
        format!("manifest_matches={}", report.manifest_matches),
        format!(
            "byte_identical_at_bounded_artifact_level={}",
            report.byte_identical_at_bounded_artifact_level
        ),
        format!("diff_entry_count={}", report.diff_entries.len()),
    ];
    if let Some(left) = &report.left_chain {
        lines.push(format!("left.source={}", left.source));
        lines.push(format!("left.top_step_name={}", left.top_step_name));
        lines.push(format!("left.top_session_path={}", left.top_session_path));
    }
    if let Some(right) = &report.right_chain {
        lines.push(format!("right.source={}", right.source));
        lines.push(format!("right.top_step_name={}", right.top_step_name));
        lines.push(format!("right.top_session_path={}", right.top_session_path));
    }
    lines.push("planning_safe_only=true".to_string());
    lines.push("activation_authorized=false".to_string());
    lines.push(format!(
        "not_authorized_summary={}",
        report.not_authorized_summary
    ));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn latest_vs_session_reports_identical_chains_when_persisted_chains_match_exactly() {
        let fixture = DiffFixture::new(
            "tiny_live_package_diff_identical",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let report = build_latest_vs_session_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            left_session_dir: None,
            right_session_dir: None,
            json: true,
            mode: Mode::LatestVsSession,
        })
        .expect("latest-vs-session report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable
        );
        assert!(report.comparable);
        assert!(report.top_identity_matches);
        assert!(report.lineage_matches);
        assert!(report.manifest_matches);
        assert!(report.byte_identical_at_bounded_artifact_level);
        assert!(report.diff_entries.is_empty());
    }

    #[test]
    fn session_vs_session_reports_top_identity_drift_when_top_summary_or_sha_differs() {
        let left = DiffFixture::new(
            "tiny_live_package_diff_top_identity_left",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        let right = DiffFixture::new(
            "tiny_live_package_diff_top_identity_right",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        set_json_string(
            right
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.session.json"),
            "clerestory_certificate_summary",
            "drifted clerestory summary",
        );
        set_json_string(
            right
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
            "clerestory_certificate_summary",
            "drifted clerestory summary",
        );
        set_json_string(
            right
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.session.json"),
            "clerestory_certificate_sha256",
            "drifted-clerestory-sha256",
        );
        set_json_string(
            right
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
            "clerestory_certificate_sha256",
            "drifted-clerestory-sha256",
        );

        let report = build_session_vs_session_report(&Config {
            root: None,
            session_dir: None,
            left_session_dir: Some(left.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            right_session_dir: Some(right.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionVsSession,
        })
        .expect("session-vs-session report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable
        );
        assert!(report.comparable);
        assert!(!report.top_identity_matches);
        assert!(report
            .diff_entries
            .iter()
            .any(|entry| entry.category == "top_identity"));
    }

    #[test]
    fn session_vs_session_reports_lineage_drift_when_one_nested_session_link_differs() {
        let left = DiffFixture::new(
            "tiny_live_package_diff_lineage_left",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        let right = DiffFixture::new(
            "tiny_live_package_diff_lineage_right",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        set_json_string(
            right
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.session.json"),
            "execute_frozen_summary",
            "alternate execute_frozen summary",
        );
        set_json_string(
            right
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.status.json"),
            "execute_frozen_summary",
            "alternate execute_frozen summary",
        );
        set_json_string(
            right
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.session.json"),
            "execute_frozen_sha256",
            "alternate-execute-frozen-sha256",
        );
        set_json_string(
            right
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.status.json"),
            "execute_frozen_sha256",
            "alternate-execute-frozen-sha256",
        );
        set_json_string(
            right
                .step_dir("execute_frozen")
                .join("tiny_live_activation_package_execute_frozen.session.json"),
            "execute_frozen_summary",
            "alternate execute_frozen summary",
        );
        set_json_string(
            right
                .step_dir("execute_frozen")
                .join("tiny_live_activation_package_execute_frozen.status.json"),
            "execute_frozen_summary",
            "alternate execute_frozen summary",
        );
        set_json_string(
            right
                .step_dir("execute_frozen")
                .join("tiny_live_activation_package_execute_frozen.session.json"),
            "execute_frozen_sha256",
            "alternate-execute-frozen-sha256",
        );
        set_json_string(
            right
                .step_dir("execute_frozen")
                .join("tiny_live_activation_package_execute_frozen.status.json"),
            "execute_frozen_sha256",
            "alternate-execute-frozen-sha256",
        );

        let report = build_session_vs_session_report(&Config {
            root: None,
            session_dir: None,
            left_session_dir: Some(left.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            right_session_dir: Some(right.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionVsSession,
        })
        .expect("session-vs-session report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable
        );
        assert!(report.comparable);
        assert!(!report.lineage_matches);
        assert!(report
            .diff_entries
            .iter()
            .any(|entry| entry.category == "lineage"));
    }

    #[test]
    fn session_vs_session_reports_manifest_drift_when_one_required_artifact_digest_differs() {
        let left = DiffFixture::new(
            "tiny_live_package_diff_manifest_left",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        let right = DiffFixture::new(
            "tiny_live_package_diff_manifest_right",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        set_json_string(
            right
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
            "reason",
            "mutated manifest report bytes",
        );

        let report = build_session_vs_session_report(&Config {
            root: None,
            session_dir: None,
            left_session_dir: Some(left.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            right_session_dir: Some(right.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionVsSession,
        })
        .expect("session-vs-session report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffAvailable
        );
        assert!(report.comparable);
        assert!(!report.manifest_matches);
        assert!(!report.byte_identical_at_bounded_artifact_level);
        assert!(report
            .diff_entries
            .iter()
            .any(|entry| entry.category == "manifest"));
    }

    #[test]
    fn verify_no_drift_fails_when_either_side_is_below_clerestory_certificate() {
        let left = DiffFixture::new(
            "tiny_live_package_diff_verify_left",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        let right = DiffFixture::new("tiny_live_package_diff_verify_right", "choir_receipt");

        let report = build_verify_no_drift_report(&Config {
            root: Some(left.root.clone()),
            session_dir: Some(right.step_dir("choir_receipt").to_path_buf()),
            left_session_dir: None,
            right_session_dir: None,
            json: true,
            mode: Mode::VerifyNoDrift,
        })
        .expect("verify-no-drift report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffVerifyInvalid
        );
        assert!(report
            .reason
            .contains("below the current top accepted layer clerestory_certificate"));
    }

    #[test]
    fn all_modes_stay_planning_safe_and_do_not_imply_activation_authorization() {
        let fixture = DiffFixture::new(
            "tiny_live_package_diff_planning_safe",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let latest = build_latest_vs_session_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            left_session_dir: None,
            right_session_dir: None,
            json: true,
            mode: Mode::LatestVsSession,
        })
        .expect("latest-vs-session report");
        let session = build_session_vs_session_report(&Config {
            root: None,
            session_dir: None,
            left_session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            right_session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionVsSession,
        })
        .expect("session-vs-session report");
        let verify = build_verify_no_drift_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            left_session_dir: None,
            right_session_dir: None,
            json: true,
            mode: Mode::VerifyNoDrift,
        })
        .expect("verify-no-drift report");

        for report in [&latest, &session, &verify] {
            assert!(report.planning_safe_only);
            assert!(report.execution_untouched);
            assert!(!report.activation_authorized);
            assert!(report
                .not_authorized_summary
                .contains("does not authorize activation"));
        }
        assert_eq!(
            verify.verdict,
            TinyLivePackageDiffVerdict::TinyLivePackageDiffVerifyOk
        );
    }

    #[derive(Debug)]
    struct DiffFixture {
        root: PathBuf,
        step_dirs: BTreeMap<String, PathBuf>,
    }

    impl DiffFixture {
        fn new(label: &str, top_step: &str) -> Self {
            let root = temp_dir(label);
            let mut step_dirs = BTreeMap::new();
            let top_rank = step_rank(top_step);
            let mut lower: Option<StepIdentity> = None;

            for index in 0..top_rank {
                let step_name = PACKAGE_CHAIN_STEPS[index];
                let step_dir = root.join(format!("{:02}-{step_name}", index + 1));
                let identity = StepIdentity {
                    step_name: step_name.to_string(),
                    session_dir: step_dir.clone(),
                    summary: format!("{step_name} summary"),
                    sha256: format!("{step_name}-sha256"),
                    algorithm: "sha256".to_string(),
                };
                write_step_session(
                    &step_dir,
                    step_name,
                    &format!("2026-04-11T00:{:02}:00Z", index),
                    &format!("2026-04-11T00:{:02}:30Z", index),
                    &identity,
                    lower.as_ref(),
                );
                lower = Some(identity);
                step_dirs.insert(step_name.to_string(), step_dir);
            }

            Self { root, step_dirs }
        }

        fn step_dir(&self, step_name: &str) -> &Path {
            self.step_dirs
                .get(step_name)
                .map(PathBuf::as_path)
                .unwrap_or_else(|| panic!("missing fixture step dir for {step_name}"))
        }
    }

    #[derive(Debug, Clone)]
    struct StepIdentity {
        step_name: String,
        session_dir: PathBuf,
        summary: String,
        sha256: String,
        algorithm: String,
    }

    fn write_step_session(
        step_dir: &Path,
        step_name: &str,
        planned_at: &str,
        updated_at: &str,
        identity: &StepIdentity,
        lower: Option<&StepIdentity>,
    ) {
        fs::create_dir_all(step_dir).unwrap();

        let mut session = serde_json::Map::new();
        session.insert("planned_at".to_string(), json!(planned_at));
        session.insert(
            "session_dir".to_string(),
            json!(step_dir.display().to_string()),
        );
        session.insert(
            "result".to_string(),
            json!("ready_for_manual_execution_when_gate_turns_green"),
        );
        session.insert(
            format!("{step_name}_summary"),
            json!(identity.summary.clone()),
        );
        session.insert(
            format!("{step_name}_sha256"),
            json!(identity.sha256.clone()),
        );
        session.insert(
            format!("{step_name}_algorithm"),
            json!(identity.algorithm.clone()),
        );

        let mut status = serde_json::Map::new();
        status.insert("updated_at".to_string(), json!(updated_at));
        status.insert(
            "session_dir".to_string(),
            json!(step_dir.display().to_string()),
        );
        status.insert(
            "result".to_string(),
            json!("ready_for_manual_execution_when_gate_turns_green"),
        );
        status.insert(
            format!("{step_name}_summary"),
            json!(identity.summary.clone()),
        );
        status.insert(
            format!("{step_name}_sha256"),
            json!(identity.sha256.clone()),
        );
        status.insert(
            format!("{step_name}_algorithm"),
            json!(identity.algorithm.clone()),
        );

        if let Some(lower) = lower {
            session.insert(
                format!("{}_session_dir", lower.step_name),
                json!(lower.session_dir.display().to_string()),
            );
            session.insert(
                format!("{}_summary", lower.step_name),
                json!(lower.summary.clone()),
            );
            session.insert(
                format!("{}_sha256", lower.step_name),
                json!(lower.sha256.clone()),
            );
            session.insert(
                format!("{}_algorithm", lower.step_name),
                json!(lower.algorithm.clone()),
            );

            status.insert(
                format!("{}_session_dir", lower.step_name),
                json!(lower.session_dir.display().to_string()),
            );
            status.insert(
                format!("{}_summary", lower.step_name),
                json!(lower.summary.clone()),
            );
            status.insert(
                format!("{}_sha256", lower.step_name),
                json!(lower.sha256.clone()),
            );
            status.insert(
                format!("{}_algorithm", lower.step_name),
                json!(lower.algorithm.clone()),
            );

            let report_path = step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}.{}{REPORT_FILE_SUFFIX}",
                lower.step_name
            ));
            fs::write(
                report_path,
                serde_json::to_vec_pretty(&json!({
                    "verdict": "tiny_live_package_nested_verify_ok",
                    "reason": "nested diff continuity ok"
                }))
                .unwrap(),
            )
            .unwrap();
        }

        fs::write(
            step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
            )),
            serde_json::to_vec_pretty(&Value::Object(session)).unwrap(),
        )
        .unwrap();
        fs::write(
            step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
            )),
            serde_json::to_vec_pretty(&Value::Object(status)).unwrap(),
        )
        .unwrap();
    }

    fn set_json_string(path: PathBuf, key: &str, new_value: &str) {
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value
            .as_object_mut()
            .expect("json object")
            .insert(key.to_string(), json!(new_value));
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
