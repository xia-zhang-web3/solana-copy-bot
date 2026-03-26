#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_channel.rs"]
mod activation_artifact_channel;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_history.rs"]
mod activation_artifact_release_history;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_publish_report.rs"]
mod activation_artifact_release_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_linkage_report (--release-archive-dir <path> | --release-artifact <path>...) --review-archive-dir <path> [--latest-pointer-dir <path>] [--latest-pointer-name <name>] [--review-channel-dir <path>] [--review-channel-name <name>] [--json]";

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
    release_archive_dir: Option<PathBuf>,
    release_artifact_paths: Vec<PathBuf>,
    review_archive_dir: PathBuf,
    latest_pointer_dir: Option<PathBuf>,
    latest_pointer_name: String,
    review_channel_dir: Option<PathBuf>,
    review_channel_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactLinkageVerdict {
    ArtifactLinkageComplete,
    ArtifactLinkageIncomplete,
    ArtifactLinkageInvalidArtifactsPresent,
    ArtifactLinkageInconsistent,
    ArtifactLinkageAmbiguousLegacyReference,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReleaseArtifactLinkageVerdict {
    Complete,
    Incomplete,
    Inconsistent,
    AmbiguousLegacyReference,
}

#[derive(Debug, Clone, Serialize)]
struct LinkageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
struct ReleaseArtifactRecord {
    source_surface: String,
    canonical_path: Option<PathBuf>,
    loaded: activation_artifact_release_history::LoadedRelease,
}

#[derive(Debug, Clone)]
struct ReviewGenerationRecord {
    canonical_packet_paths: BTreeSet<PathBuf>,
    canonical_runbook_json_paths: BTreeSet<PathBuf>,
    canonical_runbook_markdown_paths: BTreeSet<PathBuf>,
    canonical_generation_dirs: BTreeSet<PathBuf>,
}

#[derive(Debug, Clone)]
struct ReviewArchiveIndex {
    generation_by_id: BTreeMap<String, ReviewGenerationRecord>,
    packet_to_generation: BTreeMap<PathBuf, String>,
    runbook_json_to_generation: BTreeMap<PathBuf, String>,
    runbook_markdown_to_generation: BTreeMap<PathBuf, String>,
    generation_dir_to_generations: BTreeMap<PathBuf, BTreeSet<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseArtifactLinkageSummary {
    release_artifact_path: String,
    #[serde(skip_serializing)]
    canonical_release_artifact_path: Option<String>,
    source_surface: String,
    released_at: Option<DateTime<Utc>>,
    released_at_source: String,
    compat_loaded_without_released_at: bool,
    deterministic_timestamp_available: bool,
    linkage_verdict: ReleaseArtifactLinkageVerdict,
    generation_id: Option<String>,
    matched_generation_id: Option<String>,
    matched_generation_directory: Option<String>,
    matched_by: Option<String>,
    generation_directory: Option<String>,
    generation_directory_exists: bool,
    generation_directory_inside_review_archive: bool,
    generation_directory_matches_generation: bool,
    linked_generation_present: bool,
    packet_path: Option<String>,
    packet_path_exists: bool,
    packet_path_matches_generation: bool,
    runbook_json_path: Option<String>,
    runbook_json_path_exists: bool,
    runbook_json_path_matches_generation: bool,
    runbook_markdown_path: Option<String>,
    runbook_markdown_path_exists: bool,
    runbook_markdown_path_matches_generation: bool,
    incomplete_reasons: Vec<String>,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
    ambiguous_references: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LatestReleasePointerLinkageSummary {
    requested: bool,
    pointer_name: String,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_release_artifact_path: Option<String>,
    selected_generation_id: Option<String>,
    selected_release_linkage_verdict: Option<ReleaseArtifactLinkageVerdict>,
    selected_release_linkage_reason: Option<String>,
    target_exists: bool,
    target_matches_identity: bool,
    linked_generation_present: bool,
    release_artifact_examined: bool,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ReviewChannelLinkageSummary {
    requested: bool,
    channel_name: String,
    channel_verdict: Option<String>,
    channel_reason: String,
    channel_metadata_path: Option<String>,
    selected_generation_id: Option<String>,
    matches_latest_release_selection: bool,
    divergence_summary: Option<String>,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactLinkageReport {
    mode: String,
    verdict: ArtifactLinkageVerdict,
    reason: String,
    release_archive_dir: Option<String>,
    explicit_release_paths: Vec<String>,
    review_archive_dir: String,
    latest_pointer_dir: Option<String>,
    latest_pointer_name: String,
    review_channel_dir: Option<String>,
    review_channel_name: String,
    release_artifact_count_examined: usize,
    linked_generation_count: usize,
    missing_generation_ref_count: usize,
    missing_packet_ref_count: usize,
    missing_runbook_ref_count: usize,
    invalid_artifact_count: usize,
    ambiguous_legacy_reference_count: usize,
    invalid_artifacts: Vec<LinkageIssue>,
    release_artifacts: Vec<ReleaseArtifactLinkageSummary>,
    latest_release_pointer: LatestReleasePointerLinkageSummary,
    review_generation_channel: ReviewChannelLinkageSummary,
    artifact_linkage_only: bool,
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
    let mut release_archive_dir: Option<PathBuf> = None;
    let mut release_artifact_paths = Vec::new();
    let mut review_archive_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut latest_pointer_name =
        activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut review_channel_dir: Option<PathBuf> = None;
    let mut review_channel_name = activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string();
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--release-archive-dir" => {
                release_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--release-archive-dir",
                    args.next(),
                )?))
            }
            "--release-artifact" => release_artifact_paths.push(PathBuf::from(parse_string_arg(
                "--release-artifact",
                args.next(),
            )?)),
            "--review-archive-dir" => {
                review_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-archive-dir",
                    args.next(),
                )?))
            }
            "--latest-pointer-dir" => {
                latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--latest-pointer-name" => {
                latest_pointer_name =
                    parse_pointer_name(parse_string_arg("--latest-pointer-name", args.next())?)?
            }
            "--review-channel-dir" => {
                review_channel_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-channel-dir",
                    args.next(),
                )?))
            }
            "--review-channel-name" => {
                review_channel_name =
                    parse_channel_name(parse_string_arg("--review-channel-name", args.next())?)?
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    if release_archive_dir.is_none() && release_artifact_paths.is_empty() {
        bail!("either --release-archive-dir or at least one --release-artifact is required");
    }
    if latest_pointer_dir.is_some() && release_archive_dir.is_none() {
        bail!("--latest-pointer-dir requires --release-archive-dir");
    }
    if review_channel_dir.is_none()
        && review_channel_name != activation_artifact_channel::DEFAULT_CHANNEL_NAME
    {
        bail!("--review-channel-name requires --review-channel-dir");
    }

    Ok(Some(Config {
        release_archive_dir,
        release_artifact_paths,
        review_archive_dir: review_archive_dir
            .ok_or_else(|| anyhow!("--review-archive-dir is required"))?,
        latest_pointer_dir,
        latest_pointer_name,
        review_channel_dir,
        review_channel_name,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_pointer_name(value: String) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        bail!("latest pointer name may only contain ascii alphanumeric characters, '-' or '_'");
    }
}

fn parse_channel_name(value: String) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        bail!("review channel name may only contain ascii alphanumeric characters, '-' or '_'");
    }
}

fn run(config: Config) -> Result<String> {
    let report = build_report(&config)?;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

fn build_report(config: &Config) -> Result<ArtifactLinkageReport> {
    let release_paths = collect_release_inputs(config)?;
    let (release_records, mut invalid_artifacts) = load_release_artifacts(release_paths);

    let review_inventory =
        activation_artifact_archive::archive_inventory(&config.review_archive_dir)?;
    invalid_artifacts.extend(
        review_inventory
            .invalid_artifacts
            .iter()
            .cloned()
            .map(|issue| LinkageIssue {
                surface: "review_archive".to_string(),
                path: issue.path,
                error: issue.error,
            }),
    );
    let review_index = build_review_index(&config.review_archive_dir, &review_inventory)?;

    let mut release_summaries = release_records
        .iter()
        .map(|record| link_release_artifact(record, &review_index, &config.review_archive_dir))
        .collect::<Vec<_>>();
    release_summaries
        .sort_by(|left, right| left.release_artifact_path.cmp(&right.release_artifact_path));

    let latest_release_pointer = inspect_latest_release_pointer(config, &release_summaries);
    let review_generation_channel = inspect_review_generation_channel(
        config,
        latest_release_selection(&release_summaries, &latest_release_pointer),
    )?;

    let invalid_artifact_count = invalid_artifacts.len();
    let ambiguous_legacy_reference_count = release_summaries
        .iter()
        .filter(|summary| {
            summary.linkage_verdict == ReleaseArtifactLinkageVerdict::AmbiguousLegacyReference
        })
        .count();
    let inconsistent_release_count = release_summaries
        .iter()
        .filter(|summary| summary.linkage_verdict == ReleaseArtifactLinkageVerdict::Inconsistent)
        .count();
    let incomplete_release_count = release_summaries
        .iter()
        .filter(|summary| summary.linkage_verdict == ReleaseArtifactLinkageVerdict::Incomplete)
        .count();
    let linked_generation_count = release_summaries
        .iter()
        .filter(|summary| summary.linked_generation_present)
        .count();
    let missing_generation_ref_count = release_summaries
        .iter()
        .filter(|summary| !summary.linked_generation_present)
        .count();
    let missing_packet_ref_count = release_summaries
        .iter()
        .filter(|summary| {
            !summary.packet_path_exists
                || !summary.packet_path_matches_generation
                || summary.packet_path.is_none()
        })
        .count();
    let missing_runbook_ref_count = release_summaries
        .iter()
        .filter(|summary| {
            !summary.runbook_json_path_exists
                || !summary.runbook_json_path_matches_generation
                || summary.runbook_json_path.is_none()
                || !summary.runbook_markdown_path_exists
                || !summary.runbook_markdown_path_matches_generation
                || summary.runbook_markdown_path.is_none()
        })
        .count();

    let latest_pointer_inconsistent = latest_release_pointer.requested
        && (!latest_release_pointer.inconsistencies.is_empty()
            || (latest_release_pointer.release_artifact_examined
                && latest_release_pointer.selected_release_linkage_verdict
                    != Some(ReleaseArtifactLinkageVerdict::Complete))
            || (!latest_release_pointer.target_exists
                && latest_release_pointer.pointer_verdict.is_some()));
    let latest_pointer_incomplete = latest_release_pointer.requested
        && latest_release_pointer.pointer_verdict.is_some()
        && latest_release_pointer.pointer_path.is_some()
        && !latest_release_pointer.release_artifact_examined
        && latest_release_pointer.target_exists;
    let channel_invalid = review_generation_channel.requested
        && review_generation_channel.channel_verdict.as_deref()
            == Some("artifact_channel_invalid_metadata");
    let channel_inconsistent = review_generation_channel.requested
        && (!review_generation_channel.inconsistencies.is_empty()
            || review_generation_channel.divergence_summary.is_some()
            || matches!(
                review_generation_channel.channel_verdict.as_deref(),
                Some("artifact_channel_missing_target") | Some("artifact_channel_inconsistent")
            ));
    let channel_incomplete = review_generation_channel.requested
        && review_generation_channel.channel_verdict.as_deref()
            == Some("artifact_channel_missing_target")
        && review_generation_channel.divergence_summary.is_none();

    let (verdict, reason) = if invalid_artifact_count > 0 || channel_invalid {
        (
            ArtifactLinkageVerdict::ArtifactLinkageInvalidArtifactsPresent,
            format!(
                "artifact linkage report found {} invalid artifact or metadata issue(s); fix them before trusting cross-surface linkage",
                invalid_artifact_count
                    + usize::from(channel_invalid)
            ),
        )
    } else if inconsistent_release_count > 0 || latest_pointer_inconsistent || channel_inconsistent
    {
        (
            ArtifactLinkageVerdict::ArtifactLinkageInconsistent,
            "release artifacts, latest pointer, or review channel disagree with persisted review-generation state".to_string(),
        )
    } else if ambiguous_legacy_reference_count > 0 {
        (
            ArtifactLinkageVerdict::ArtifactLinkageAmbiguousLegacyReference,
            format!(
                "{} release artifact(s) use legacy linkage that cannot be established deterministically",
                ambiguous_legacy_reference_count
            ),
        )
    } else if release_summaries.is_empty() {
        (
            ArtifactLinkageVerdict::ArtifactLinkageIncomplete,
            "no persisted release artifacts were found to link back to the review-generation archive".to_string(),
        )
    } else if incomplete_release_count > 0 || latest_pointer_incomplete || channel_incomplete {
        (
            ArtifactLinkageVerdict::ArtifactLinkageIncomplete,
            "release-to-review linkage is only partially covered; review missing generation or packet/runbook references".to_string(),
        )
    } else {
        (
            ArtifactLinkageVerdict::ArtifactLinkageComplete,
            "release artifacts, latest release pointer, and review-generation archive references are mutually consistent".to_string(),
        )
    };

    Ok(ArtifactLinkageReport {
        mode: "linkage_report".to_string(),
        verdict,
        reason,
        release_archive_dir: config
            .release_archive_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        explicit_release_paths: config
            .release_artifact_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        review_archive_dir: config.review_archive_dir.display().to_string(),
        latest_pointer_dir: config
            .latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        latest_pointer_name: config.latest_pointer_name.clone(),
        review_channel_dir: config
            .review_channel_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        review_channel_name: config.review_channel_name.clone(),
        release_artifact_count_examined: release_summaries.len(),
        linked_generation_count,
        missing_generation_ref_count,
        missing_packet_ref_count,
        missing_runbook_ref_count,
        invalid_artifact_count,
        ambiguous_legacy_reference_count,
        invalid_artifacts,
        release_artifacts: release_summaries,
        latest_release_pointer,
        review_generation_channel,
        artifact_linkage_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact linkage analysis only correlates persisted release artifacts with persisted review generations. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_release_inputs(config: &Config) -> Result<Vec<(String, PathBuf)>> {
    let mut inputs = Vec::new();
    let mut seen = BTreeSet::new();

    if let Some(root) = &config.release_archive_dir {
        for path in collect_release_files(root)? {
            let key = path.display().to_string();
            if seen.insert(key) {
                inputs.push(("release_archive".to_string(), path));
            }
        }
    }

    for path in &config.release_artifact_paths {
        let key = path.display().to_string();
        if seen.insert(key.clone()) {
            inputs.push(("explicit_release_path".to_string(), path.clone()));
        }
    }

    Ok(inputs)
}

fn collect_release_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .with_context(|| format!("failed reading release archive dir {}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file()
                && path.extension().and_then(|ext| ext.to_str()) == Some("json")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn load_release_artifacts(
    paths: Vec<(String, PathBuf)>,
) -> (Vec<ReleaseArtifactRecord>, Vec<LinkageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for (surface, path) in paths {
        match activation_artifact_release_history::inspect_release_artifact(&path) {
            Ok(loaded) => {
                let canonical_path = fs::canonicalize(&path).ok();
                valid.push(ReleaseArtifactRecord {
                    source_surface: surface,
                    canonical_path,
                    loaded,
                });
            }
            Err(error) => invalid.push(LinkageIssue {
                surface,
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn build_review_index(
    review_archive_dir: &Path,
    inventory: &activation_artifact_archive::ArchiveInventory,
) -> Result<ReviewArchiveIndex> {
    let canonical_review_root = fs::canonicalize(review_archive_dir).with_context(|| {
        format!(
            "failed canonicalizing review archive dir {}",
            review_archive_dir.display()
        )
    })?;
    let mut generation_by_id = BTreeMap::new();
    let mut packet_to_generation = BTreeMap::new();
    let mut runbook_json_to_generation = BTreeMap::new();
    let mut runbook_markdown_to_generation = BTreeMap::new();
    let mut generation_dir_to_generations = BTreeMap::<PathBuf, BTreeSet<String>>::new();

    for summary in &inventory.generation_summaries {
        let generation_id = activation_artifact_archive::generation_id_from_summary(summary);
        let canonical_packet_paths =
            canonical_existing_paths(&summary.decision_packet_paths, &canonical_review_root)?;
        let canonical_runbook_json_paths =
            canonical_existing_paths(&summary.runbook_json_paths, &canonical_review_root)?;
        let canonical_runbook_markdown_paths =
            canonical_existing_paths(&summary.runbook_markdown_paths, &canonical_review_root)?;
        let canonical_generation_dirs = canonical_packet_paths
            .iter()
            .chain(canonical_runbook_json_paths.iter())
            .chain(canonical_runbook_markdown_paths.iter())
            .filter_map(|path| path.parent().map(|parent| parent.to_path_buf()))
            .collect::<BTreeSet<_>>();

        for path in &canonical_packet_paths {
            packet_to_generation.insert(path.clone(), generation_id.clone());
        }
        for path in &canonical_runbook_json_paths {
            runbook_json_to_generation.insert(path.clone(), generation_id.clone());
        }
        for path in &canonical_runbook_markdown_paths {
            runbook_markdown_to_generation.insert(path.clone(), generation_id.clone());
        }
        for dir in &canonical_generation_dirs {
            generation_dir_to_generations
                .entry(dir.clone())
                .or_default()
                .insert(generation_id.clone());
        }

        generation_by_id.insert(
            generation_id,
            ReviewGenerationRecord {
                canonical_packet_paths,
                canonical_runbook_json_paths,
                canonical_runbook_markdown_paths,
                canonical_generation_dirs,
            },
        );
    }

    Ok(ReviewArchiveIndex {
        generation_by_id,
        packet_to_generation,
        runbook_json_to_generation,
        runbook_markdown_to_generation,
        generation_dir_to_generations,
    })
}

fn canonical_existing_paths(paths: &[String], canonical_root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut canonicalized = BTreeSet::new();
    for raw in paths {
        let canonical = fs::canonicalize(raw)
            .with_context(|| format!("failed canonicalizing persisted review artifact {raw}"))?;
        if !canonical.starts_with(canonical_root) {
            bail!(
                "persisted review artifact {} resolves outside review archive {}",
                canonical.display(),
                canonical_root.display()
            );
        }
        canonicalized.insert(canonical);
    }
    Ok(canonicalized)
}

fn link_release_artifact(
    record: &ReleaseArtifactRecord,
    review_index: &ReviewArchiveIndex,
    review_archive_dir: &Path,
) -> ReleaseArtifactLinkageSummary {
    let canonical_review_root = fs::canonicalize(review_archive_dir).ok();
    let release = &record.loaded.artifact;
    let compat_loaded_without_released_at = record.loaded.released_at_source
        != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt;
    let deterministic_timestamp_available = record.loaded.effective_released_at.is_some();
    let mut incomplete_reasons = Vec::new();
    let mut missing_paths = Vec::new();
    let mut inconsistencies = Vec::new();
    let mut ambiguous_references = Vec::new();

    let mut matched_generation_id = None;
    let mut matched_by = None;

    if let Some(generation_id) = release.generation_id.as_ref() {
        if review_index.generation_by_id.contains_key(generation_id) {
            matched_generation_id = Some(generation_id.clone());
            matched_by = Some("generation_id".to_string());
        } else {
            inconsistencies.push(format!(
                "release artifact generation_id `{generation_id}` is absent from the review-generation archive"
            ));
        }
    } else {
        let candidates = infer_generation_candidates(
            release,
            review_index,
            canonical_review_root.as_deref(),
            &mut missing_paths,
            &mut inconsistencies,
        );
        if candidates.len() == 1 {
            matched_generation_id = candidates.into_iter().next();
            matched_by = Some("reference_membership".to_string());
        } else if candidates.is_empty() {
            ambiguous_references.push(
                "release artifact lacks generation_id and does not resolve deterministically to a persisted review generation".to_string(),
            );
        } else {
            ambiguous_references.push(format!(
                "release artifact lacks generation_id and its references match multiple persisted review generations: {}",
                candidates.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }

    let matched_generation = matched_generation_id
        .as_ref()
        .and_then(|id| review_index.generation_by_id.get(id));
    if matched_generation.is_none()
        && release.generation_id.is_none()
        && ambiguous_references.is_empty()
    {
        incomplete_reasons.push(
            "release artifact does not establish a persisted review generation linkage".to_string(),
        );
    }

    let (
        generation_directory_exists,
        generation_directory_inside_review_archive,
        generation_directory_matches_generation,
    ) = validate_generation_directory(
        release.generation_directory.as_deref(),
        canonical_review_root.as_deref(),
        matched_generation,
        &mut missing_paths,
        &mut inconsistencies,
        &mut incomplete_reasons,
    );

    let (packet_path_exists, packet_path_matches_generation) = validate_reference_path(
        release.packet_path.as_deref(),
        "packet_path",
        "decision packet",
        canonical_review_root.as_deref(),
        matched_generation.map(|value| &value.canonical_packet_paths),
        &mut missing_paths,
        &mut inconsistencies,
        &mut incomplete_reasons,
    );
    let (runbook_json_path_exists, runbook_json_path_matches_generation) = validate_reference_path(
        release.runbook_json_path.as_deref(),
        "runbook_json_path",
        "runbook json",
        canonical_review_root.as_deref(),
        matched_generation.map(|value| &value.canonical_runbook_json_paths),
        &mut missing_paths,
        &mut inconsistencies,
        &mut incomplete_reasons,
    );
    let (runbook_markdown_path_exists, runbook_markdown_path_matches_generation) =
        validate_reference_path(
            release.runbook_markdown_path.as_deref(),
            "runbook_markdown_path",
            "runbook markdown",
            canonical_review_root.as_deref(),
            matched_generation.map(|value| &value.canonical_runbook_markdown_paths),
            &mut missing_paths,
            &mut inconsistencies,
            &mut incomplete_reasons,
        );

    let linkage_verdict = if !inconsistencies.is_empty() {
        ReleaseArtifactLinkageVerdict::Inconsistent
    } else if !ambiguous_references.is_empty() {
        ReleaseArtifactLinkageVerdict::AmbiguousLegacyReference
    } else if !incomplete_reasons.is_empty() || matched_generation.is_none() {
        ReleaseArtifactLinkageVerdict::Incomplete
    } else {
        ReleaseArtifactLinkageVerdict::Complete
    };

    ReleaseArtifactLinkageSummary {
        release_artifact_path: record.loaded.path.display().to_string(),
        canonical_release_artifact_path: record
            .canonical_path
            .as_ref()
            .map(|path| path.display().to_string()),
        source_surface: record.source_surface.clone(),
        released_at: record.loaded.effective_released_at,
        released_at_source: serialize_enum(&record.loaded.released_at_source),
        compat_loaded_without_released_at,
        deterministic_timestamp_available,
        linkage_verdict,
        generation_id: release.generation_id.clone(),
        matched_generation_id: matched_generation_id.clone(),
        matched_generation_directory: matched_generation
            .and_then(|summary| summary.canonical_generation_dirs.iter().next())
            .map(|path| path.display().to_string()),
        matched_by,
        generation_directory: release.generation_directory.clone(),
        generation_directory_exists,
        generation_directory_inside_review_archive,
        generation_directory_matches_generation,
        linked_generation_present: matched_generation.is_some(),
        packet_path: release.packet_path.clone(),
        packet_path_exists,
        packet_path_matches_generation,
        runbook_json_path: release.runbook_json_path.clone(),
        runbook_json_path_exists,
        runbook_json_path_matches_generation,
        runbook_markdown_path: release.runbook_markdown_path.clone(),
        runbook_markdown_path_exists,
        runbook_markdown_path_matches_generation,
        incomplete_reasons,
        missing_paths,
        inconsistencies,
        ambiguous_references,
    }
}

fn infer_generation_candidates(
    release: &activation_artifact_release_history::ArtifactReleaseArtifact,
    review_index: &ReviewArchiveIndex,
    canonical_review_root: Option<&Path>,
    missing_paths: &mut Vec<String>,
    inconsistencies: &mut Vec<String>,
) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();

    if let Some(path) = release.packet_path.as_deref() {
        if let Some(canonical) = resolve_existing_reference(
            path,
            "packet_path",
            canonical_review_root,
            missing_paths,
            inconsistencies,
        ) {
            if let Some(generation_id) = review_index.packet_to_generation.get(&canonical) {
                candidates.insert(generation_id.clone());
            }
        }
    }

    if let Some(path) = release.runbook_json_path.as_deref() {
        if let Some(canonical) = resolve_existing_reference(
            path,
            "runbook_json_path",
            canonical_review_root,
            missing_paths,
            inconsistencies,
        ) {
            if let Some(generation_id) = review_index.runbook_json_to_generation.get(&canonical) {
                candidates.insert(generation_id.clone());
            }
        }
    }

    if let Some(path) = release.runbook_markdown_path.as_deref() {
        if let Some(canonical) = resolve_existing_reference(
            path,
            "runbook_markdown_path",
            canonical_review_root,
            missing_paths,
            inconsistencies,
        ) {
            if let Some(generation_id) = review_index.runbook_markdown_to_generation.get(&canonical)
            {
                candidates.insert(generation_id.clone());
            }
        }
    }

    if let Some(path) = release.generation_directory.as_deref() {
        if let Some(canonical) = resolve_existing_reference(
            path,
            "generation_directory",
            canonical_review_root,
            missing_paths,
            inconsistencies,
        ) {
            if let Some(generation_ids) = review_index.generation_dir_to_generations.get(&canonical)
            {
                candidates.extend(generation_ids.iter().cloned());
            }
        }
    }

    candidates
}

fn validate_generation_directory(
    raw_path: Option<&str>,
    canonical_review_root: Option<&Path>,
    matched_generation: Option<&ReviewGenerationRecord>,
    missing_paths: &mut Vec<String>,
    inconsistencies: &mut Vec<String>,
    incomplete_reasons: &mut Vec<String>,
) -> (bool, bool, bool) {
    let Some(raw_path) = raw_path else {
        incomplete_reasons
            .push("release artifact does not record a generation_directory reference".to_string());
        return (false, false, false);
    };
    let Some(canonical) = resolve_existing_reference(
        raw_path,
        "generation_directory",
        canonical_review_root,
        missing_paths,
        inconsistencies,
    ) else {
        return (false, false, false);
    };
    let inside_review_archive =
        canonical_review_root.is_some_and(|root| canonical.starts_with(root));
    let matches_generation = matched_generation
        .is_some_and(|summary| summary.canonical_generation_dirs.contains(&canonical));
    if let Some(summary) = matched_generation {
        if !summary.canonical_generation_dirs.contains(&canonical) {
            inconsistencies.push(format!(
                "generation_directory {} does not belong to matched persisted review generation",
                canonical.display()
            ));
        }
    }
    (true, inside_review_archive, matches_generation)
}

fn validate_reference_path(
    raw_path: Option<&str>,
    field_name: &str,
    label: &str,
    canonical_review_root: Option<&Path>,
    expected_paths: Option<&BTreeSet<PathBuf>>,
    missing_paths: &mut Vec<String>,
    inconsistencies: &mut Vec<String>,
    incomplete_reasons: &mut Vec<String>,
) -> (bool, bool) {
    let Some(raw_path) = raw_path else {
        incomplete_reasons.push(format!(
            "release artifact does not record a {field_name} reference"
        ));
        return (false, false);
    };
    let missing_paths_before = missing_paths.len();
    let Some(canonical) = resolve_existing_reference(
        raw_path,
        field_name,
        canonical_review_root,
        missing_paths,
        inconsistencies,
    ) else {
        if missing_paths.len() > missing_paths_before {
            incomplete_reasons.push(format!(
                "release artifact {field_name} reference {raw_path} is missing on disk"
            ));
        }
        return (false, false);
    };
    if let Some(expected_paths) = expected_paths {
        if !expected_paths.contains(&canonical) {
            inconsistencies.push(format!(
                "{label} reference {} is not part of the matched persisted review generation",
                canonical.display()
            ));
            return (true, false);
        }
        (true, true)
    } else {
        incomplete_reasons.push(format!(
            "release artifact records {field_name}, but no persisted review generation was matched"
        ));
        (true, false)
    }
}

fn resolve_existing_reference(
    raw_path: &str,
    field_name: &str,
    canonical_review_root: Option<&Path>,
    missing_paths: &mut Vec<String>,
    inconsistencies: &mut Vec<String>,
) -> Option<PathBuf> {
    let path = PathBuf::from(raw_path);
    if !path.exists() {
        missing_paths.push(path.display().to_string());
        return None;
    }
    let canonical = match fs::canonicalize(&path) {
        Ok(path) => path,
        Err(error) => {
            inconsistencies.push(format!(
                "failed canonicalizing {field_name} {}: {error:#}",
                path.display()
            ));
            return None;
        }
    };
    if let Some(root) = canonical_review_root {
        if !canonical.starts_with(root) {
            inconsistencies.push(format!(
                "{field_name} {} resolves outside review archive {}",
                canonical.display(),
                root.display()
            ));
        }
    }
    Some(canonical)
}

fn inspect_latest_release_pointer(
    config: &Config,
    release_summaries: &[ReleaseArtifactLinkageSummary],
) -> LatestReleasePointerLinkageSummary {
    let Some(latest_pointer_dir) = &config.latest_pointer_dir else {
        return LatestReleasePointerLinkageSummary {
            requested: false,
            pointer_name: config.latest_pointer_name.clone(),
            pointer_verdict: None,
            pointer_reason: "latest release pointer was not requested".to_string(),
            pointer_path: None,
            selected_release_artifact_path: None,
            selected_generation_id: None,
            selected_release_linkage_verdict: None,
            selected_release_linkage_reason: None,
            target_exists: false,
            target_matches_identity: false,
            linked_generation_present: false,
            release_artifact_examined: false,
            inconsistencies: Vec::new(),
        };
    };

    let inspected = match activation_artifact_release_publish_report::inspect_latest_pointer_report(
        config
            .release_archive_dir
            .as_ref()
            .expect("latest pointer requires release archive dir"),
        latest_pointer_dir,
        &config.latest_pointer_name,
        true,
    ) {
        Ok(report) => report,
        Err(error) => {
            return LatestReleasePointerLinkageSummary {
                requested: true,
                pointer_name: config.latest_pointer_name.clone(),
                pointer_verdict: Some("artifact_release_report_failed".to_string()),
                pointer_reason: format!("failed verifying latest release pointer: {error:#}"),
                pointer_path: None,
                selected_release_artifact_path: None,
                selected_generation_id: None,
                selected_release_linkage_verdict: None,
                selected_release_linkage_reason: None,
                target_exists: false,
                target_matches_identity: false,
                linked_generation_present: false,
                release_artifact_examined: false,
                inconsistencies: vec![format!("{error:#}")],
            };
        }
    };

    let selected_release_summary =
        inspected
            .persisted_release_artifact_path
            .as_ref()
            .and_then(|path| {
                release_summaries.iter().find(|summary| {
                    summary.release_artifact_path == *path
                        || summary.canonical_release_artifact_path.as_deref() == Some(path.as_str())
                })
            });

    let selected_release_linkage_reason = selected_release_summary.map(|summary| match summary.linkage_verdict {
        ReleaseArtifactLinkageVerdict::Complete => {
            "selected release artifact still links cleanly to the persisted review generation".to_string()
        }
        ReleaseArtifactLinkageVerdict::Incomplete => summary
            .incomplete_reasons
            .first()
            .cloned()
            .unwrap_or_else(|| "selected release artifact has incomplete review-generation linkage".to_string()),
        ReleaseArtifactLinkageVerdict::Inconsistent => summary
            .inconsistencies
            .first()
            .cloned()
            .unwrap_or_else(|| "selected release artifact is inconsistent with the persisted review-generation archive".to_string()),
        ReleaseArtifactLinkageVerdict::AmbiguousLegacyReference => summary
            .ambiguous_references
            .first()
            .cloned()
            .unwrap_or_else(|| "selected release artifact uses ambiguous legacy linkage".to_string()),
    });

    let mut inconsistencies = inspected.inconsistencies.clone();
    if inspected.persisted_release_artifact_path.is_some() && selected_release_summary.is_none() {
        inconsistencies.push(
            "latest release pointer resolves to a release artifact that was not part of the examined release artifact set".to_string(),
        );
    }

    LatestReleasePointerLinkageSummary {
        requested: true,
        pointer_name: config.latest_pointer_name.clone(),
        pointer_verdict: Some(serialize_enum(&inspected.verdict)),
        pointer_reason: inspected.reason.clone(),
        pointer_path: inspected.latest_pointer_path.clone(),
        selected_release_artifact_path: inspected.persisted_release_artifact_path.clone(),
        selected_generation_id: inspected.generation_id.clone(),
        selected_release_linkage_verdict: selected_release_summary
            .map(|summary| summary.linkage_verdict),
        selected_release_linkage_reason,
        target_exists: inspected.latest_pointer_target_exists,
        target_matches_identity: inspected.latest_pointer_target_matches_identity,
        linked_generation_present: selected_release_summary
            .is_some_and(|summary| summary.linked_generation_present),
        release_artifact_examined: selected_release_summary.is_some(),
        inconsistencies,
    }
}

fn latest_release_selection<'a>(
    release_summaries: &'a [ReleaseArtifactLinkageSummary],
    latest_release_pointer: &LatestReleasePointerLinkageSummary,
) -> Option<&'a ReleaseArtifactLinkageSummary> {
    if let Some(selected_path) = latest_release_pointer
        .selected_release_artifact_path
        .as_ref()
    {
        if let Some(summary) = release_summaries.iter().find(|summary| {
            &summary.release_artifact_path == selected_path
                || summary.canonical_release_artifact_path.as_deref()
                    == Some(selected_path.as_str())
        }) {
            return Some(summary);
        }
    }
    release_summaries
        .iter()
        .filter(|summary| summary.released_at.is_some())
        .max_by(|left, right| {
            left.released_at
                .cmp(&right.released_at)
                .then_with(|| left.release_artifact_path.cmp(&right.release_artifact_path))
        })
}

fn inspect_review_generation_channel(
    config: &Config,
    latest_release_selection: Option<&ReleaseArtifactLinkageSummary>,
) -> Result<ReviewChannelLinkageSummary> {
    let Some(channel_dir) = &config.review_channel_dir else {
        return Ok(ReviewChannelLinkageSummary {
            requested: false,
            channel_name: config.review_channel_name.clone(),
            channel_verdict: None,
            channel_reason: "review generation channel was not requested".to_string(),
            channel_metadata_path: None,
            selected_generation_id: None,
            matches_latest_release_selection: false,
            divergence_summary: None,
            missing_paths: Vec::new(),
            inconsistencies: Vec::new(),
        });
    };

    let channel_config = activation_artifact_channel::Config {
        archive_dir: config.review_archive_dir.clone(),
        channel_dir: channel_dir.clone(),
        channel_name: config.review_channel_name.clone(),
        json: false,
        mode: activation_artifact_channel::Mode::Verify,
    };
    let report = activation_artifact_channel::inspect_channel(&channel_config, true)?;
    let mut inconsistencies = report.inconsistencies.clone();
    let mut divergence_summary = None;
    let matches_latest_release_selection = if let Some(latest_release) = latest_release_selection {
        match (
            latest_release.matched_generation_id.as_ref(),
            report.selected_generation_id.as_ref(),
        ) {
            (Some(latest_generation_id), Some(channel_generation_id)) => {
                if latest_generation_id == channel_generation_id {
                    true
                } else {
                    divergence_summary = Some(format!(
                        "latest release selection points to generation `{latest_generation_id}`, but review channel `{}` points to `{channel_generation_id}`",
                        config.review_channel_name
                    ));
                    false
                }
            }
            (Some(latest_generation_id), None) => {
                divergence_summary = Some(format!(
                    "latest release selection points to generation `{latest_generation_id}`, but review channel `{}` has no selected generation",
                    config.review_channel_name
                ));
                false
            }
            _ => false,
        }
    } else {
        false
    };

    if report.verdict
        == activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelInvalidMetadata
    {
        inconsistencies.push("review generation channel metadata is invalid".to_string());
    }

    Ok(ReviewChannelLinkageSummary {
        requested: true,
        channel_name: config.review_channel_name.clone(),
        channel_verdict: Some(serialize_enum(&report.verdict)),
        channel_reason: report.reason,
        channel_metadata_path: Some(report.channel_metadata_path),
        selected_generation_id: report.selected_generation_id,
        matches_latest_release_selection,
        divergence_summary,
        missing_paths: report.missing_paths,
        inconsistencies,
    })
}

fn render_human(report: &ArtifactLinkageReport) -> String {
    let latest_pointer_verdict = report
        .latest_release_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    let review_channel_verdict = report
        .review_generation_channel
        .channel_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());

    [
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "release_archive_dir={}",
            report.release_archive_dir.as_deref().unwrap_or("<none>")
        ),
        format!("review_archive_dir={}", report.review_archive_dir),
        format!(
            "release_artifact_count_examined={}",
            report.release_artifact_count_examined
        ),
        format!("linked_generation_count={}", report.linked_generation_count),
        format!(
            "missing_generation_ref_count={}",
            report.missing_generation_ref_count
        ),
        format!(
            "missing_packet_ref_count={}",
            report.missing_packet_ref_count
        ),
        format!(
            "missing_runbook_ref_count={}",
            report.missing_runbook_ref_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "ambiguous_legacy_reference_count={}",
            report.ambiguous_legacy_reference_count
        ),
        format!("latest_pointer_verdict={latest_pointer_verdict}"),
        format!("review_channel_verdict={review_channel_verdict}"),
        format!("artifact_linkage_only={}", report.artifact_linkage_only),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn complete_release_to_generation_linkage_yields_green() {
        let review_archive_dir = temp_dir("artifact_linkage_complete_review");
        let release_archive_dir = temp_dir("artifact_linkage_complete_release");
        let latest_pointer_dir = temp_dir("artifact_linkage_complete_pointer");
        let review_channel_dir = temp_dir("artifact_linkage_complete_channel");

        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published_and_promoted.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published_and_promoted",
        );
        write_latest_pointer(
            &latest_pointer_dir,
            &release_archive_dir,
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some(&generation.generation_id),
            "artifact_release_published_and_promoted",
        );
        write_review_channel(
            &review_channel_dir,
            &review_archive_dir,
            &generation,
            activation_artifact_channel::DEFAULT_CHANNEL_NAME,
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: Some(latest_pointer_dir),
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: Some(review_channel_dir),
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageComplete
        );
        assert_eq!(report.linked_generation_count, 1);
        assert_eq!(
            report
                .latest_release_pointer
                .selected_release_linkage_verdict,
            Some(ReleaseArtifactLinkageVerdict::Complete)
        );
        assert!(
            report
                .review_generation_channel
                .matches_latest_release_selection
        );
    }

    #[test]
    fn missing_generation_reference_yields_inconsistent_linkage() {
        let review_archive_dir = temp_dir("artifact_linkage_missing_generation_review");
        let release_archive_dir = temp_dir("artifact_linkage_missing_generation_release");
        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some("2026-03-26T12:00:00Z|missing_prod|missing_non_prod"),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: None,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageInconsistent
        );
        assert_eq!(
            report.release_artifacts[0].linkage_verdict,
            ReleaseArtifactLinkageVerdict::Inconsistent
        );
    }

    #[test]
    fn missing_packet_or_runbook_file_is_surfaced_explicitly() {
        let review_archive_dir = temp_dir("artifact_linkage_missing_packet_review");
        let release_archive_dir = temp_dir("artifact_linkage_missing_packet_release");
        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        fs::remove_file(&generation.packet_path).expect("remove packet");

        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: None,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(report.missing_packet_ref_count, 1);
        assert!(report.release_artifacts[0]
            .missing_paths
            .iter()
            .any(|path| path.ends_with("decision_packet.json")));
    }

    #[test]
    fn missing_runbook_markdown_is_non_green_and_increments_runbook_counter() {
        let review_archive_dir = temp_dir("artifact_linkage_missing_markdown_review");
        let release_archive_dir = temp_dir("artifact_linkage_missing_markdown_release");
        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        fs::remove_file(&generation.runbook_markdown_path).expect("remove runbook markdown");

        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: None,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageIncomplete
        );
        assert_eq!(report.missing_runbook_ref_count, 1);
        assert_eq!(
            report.release_artifacts[0].linkage_verdict,
            ReleaseArtifactLinkageVerdict::Incomplete
        );
        assert!(report.release_artifacts[0]
            .missing_paths
            .iter()
            .any(|path| path.ends_with("activation_runbook.md")));
    }

    #[test]
    fn wrong_runbook_markdown_path_outside_generation_is_non_green() {
        let review_archive_dir = temp_dir("artifact_linkage_wrong_markdown_review");
        let release_archive_dir = temp_dir("artifact_linkage_wrong_markdown_release");
        let stray_dir = temp_dir("artifact_linkage_wrong_markdown_stray");
        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        let stray_markdown_path = stray_dir.join("activation_runbook.md");
        fs::write(&stray_markdown_path, "# stray\n").expect("write stray markdown");

        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(stray_markdown_path.to_string_lossy().as_ref()),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: None,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageInconsistent
        );
        assert_eq!(report.missing_runbook_ref_count, 1);
        assert_eq!(
            report.release_artifacts[0].linkage_verdict,
            ReleaseArtifactLinkageVerdict::Inconsistent
        );
        assert!(report.release_artifacts[0]
            .inconsistencies
            .iter()
            .any(|entry| entry.contains("runbook_markdown_path")));
    }

    #[test]
    fn latest_release_pointer_with_broken_generation_linkage_is_surfaced() {
        let review_archive_dir = temp_dir("artifact_linkage_pointer_broken_review");
        let release_archive_dir = temp_dir("artifact_linkage_pointer_broken_release");
        let latest_pointer_dir = temp_dir("artifact_linkage_pointer_broken_pointer");
        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some("2026-03-26T12:00:00Z|missing_prod|missing_non_prod"),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published",
        );
        write_latest_pointer(
            &latest_pointer_dir,
            &release_archive_dir,
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some("2026-03-26T12:00:00Z|missing_prod|missing_non_prod"),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: Some(latest_pointer_dir),
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageInconsistent
        );
        assert_eq!(
            report
                .latest_release_pointer
                .selected_release_linkage_verdict,
            Some(ReleaseArtifactLinkageVerdict::Inconsistent)
        );
    }

    #[test]
    fn ambiguous_legacy_linkage_is_not_treated_as_clean_green() {
        let review_archive_dir = temp_dir("artifact_linkage_legacy_review");
        let release_archive_dir = temp_dir("artifact_linkage_legacy_release");
        write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        let release_artifact_path = release_archive_dir.join("legacy_release.json");
        write_release_artifact(
            &release_artifact_path,
            None,
            None,
            None,
            None,
            None,
            None,
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: Some(release_archive_dir),
            release_artifact_paths: Vec::new(),
            review_archive_dir,
            latest_pointer_dir: None,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            review_channel_dir: None,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactLinkageVerdict::ArtifactLinkageAmbiguousLegacyReference
        );
        assert_eq!(report.ambiguous_legacy_reference_count, 1);
    }

    #[derive(Debug, Clone)]
    struct ReviewGenerationFixture {
        generation_id: String,
        generation_dir: String,
        packet_path: String,
        runbook_json_path: String,
        runbook_markdown_path: String,
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_review_generation(
        review_archive_dir: &Path,
        generated_at: &str,
        prod_fp: &str,
        non_prod_fp: &str,
    ) -> ReviewGenerationFixture {
        let stamp = generated_at.replace(':', "-");
        let generation_dir =
            review_archive_dir.join(format!("{}__{}__{}", stamp, prod_fp, non_prod_fp));
        fs::create_dir_all(&generation_dir).expect("create generation dir");
        let packet_path = generation_dir.join("decision_packet.json");
        let runbook_json_path = generation_dir.join("activation_runbook.json");
        let runbook_markdown_path = generation_dir.join("activation_runbook.md");
        fs::write(
            &packet_path,
            serde_json::to_string_pretty(&sample_packet_json(generated_at, prod_fp, non_prod_fp))
                .expect("packet json"),
        )
        .expect("write packet");
        fs::write(
            &runbook_json_path,
            serde_json::to_string_pretty(&sample_runbook_json(
                generated_at,
                generated_at,
                prod_fp,
                non_prod_fp,
            ))
            .expect("runbook json"),
        )
        .expect("write runbook json");
        fs::write(
            &runbook_markdown_path,
            "# Tiny-Live Activation Runbook\n\nsample\n",
        )
        .expect("write runbook markdown");

        ReviewGenerationFixture {
            generation_id: format!(
                "{}|{}|{}",
                DateTime::parse_from_rfc3339(generated_at)
                    .expect("generated_at ts")
                    .with_timezone(&Utc)
                    .to_rfc3339(),
                prod_fp,
                non_prod_fp
            ),
            generation_dir: generation_dir.display().to_string(),
            packet_path: packet_path.display().to_string(),
            runbook_json_path: runbook_json_path.display().to_string(),
            runbook_markdown_path: runbook_markdown_path.display().to_string(),
        }
    }

    fn write_release_artifact(
        path: &Path,
        released_at: Option<&str>,
        generation_id: Option<&str>,
        generation_directory: Option<&str>,
        packet_path: Option<&str>,
        runbook_json_path: Option<&str>,
        runbook_markdown_path: Option<&str>,
        verdict: &str,
    ) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        let payload = json!({
            "mode": "artifact_release",
            "released_at": released_at,
            "verdict": verdict,
            "reason": format!("release {verdict}"),
            "config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "archive_dir": "/var/www/solana-copy-bot/state/activation_artifacts/archive",
            "generation_id": generation_id,
            "generation_directory": generation_directory,
            "packet_path": packet_path,
            "runbook_json_path": runbook_json_path,
            "runbook_markdown_path": runbook_markdown_path,
            "manifest_path": null,
            "bundle_path": null,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "checklist_verdict": "activation_checklist_blocked_by_prod_stage3",
            "publish_verdict": "artifact_publish_succeeded",
            "publish_reason": "publish ok",
            "generation_published": true,
            "manifest_generated": false,
            "bundle_exported": false,
            "channel_promotion_attempted": false,
            "channel_promotion_happened": false,
            "channel_verify_attempted": false,
            "channel_name": null,
            "channel_dir": null,
            "channel_metadata_path": null,
            "channel_promote_verdict": null,
            "channel_promote_reason": null,
            "channel_verify_verdict": null,
            "channel_verify_reason": null,
            "execution_untouched": true,
            "activation_authorized": false,
            "not_authorized_summary": "artifact-only"
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&payload).expect("release artifact json"),
        )
        .expect("write release artifact");
    }

    fn write_latest_pointer(
        latest_pointer_dir: &Path,
        release_archive_dir: &Path,
        selected_release_artifact_path: &Path,
        released_at: Option<&str>,
        released_at_source: &str,
        selected_generation_id: Option<&str>,
        release_verdict: &str,
    ) {
        fs::create_dir_all(latest_pointer_dir).expect("create latest pointer dir");
        let canonical_release_archive_dir = fs::canonicalize(release_archive_dir)
            .unwrap_or_else(|_| release_archive_dir.to_path_buf());
        let canonical_selected_release_artifact_path =
            fs::canonicalize(selected_release_artifact_path)
                .unwrap_or_else(|_| selected_release_artifact_path.to_path_buf());
        let pointer_path = latest_pointer_dir.join(format!(
            "{}.json",
            activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
        ));
        let payload = json!({
            "pointer_version": "1",
            "pointer_name": activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME,
            "source_release_archive_dir": canonical_release_archive_dir.display().to_string(),
            "selected_release_artifact_path": canonical_selected_release_artifact_path.display().to_string(),
            "selected_release_artifact_file_name": selected_release_artifact_path.file_name().unwrap().to_string_lossy(),
            "release_artifact_mode": "artifact_release",
            "released_at": released_at,
            "released_at_source": released_at_source,
            "compat_loaded_without_released_at": released_at_source != "released_at",
            "deterministic_timestamp_available": released_at.is_some(),
            "ordered_history_confident": released_at.is_some(),
            "release_verdict": release_verdict,
            "release_reason": format!("release {release_verdict}"),
            "selected_generation_id": selected_generation_id,
            "channel_promotion_happened": false,
            "channel_metadata_path": null,
            "pointed_at": "2026-03-26T12:05:00Z",
            "build_version": "0.1.0",
            "git_commit": "deadbeef"
        });
        fs::write(
            pointer_path,
            serde_json::to_string_pretty(&payload).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn write_review_channel(
        review_channel_dir: &Path,
        review_archive_dir: &Path,
        generation: &ReviewGenerationFixture,
        channel_name: &str,
    ) {
        fs::create_dir_all(review_channel_dir).expect("create review channel dir");
        let payload = json!({
            "channel_version": "1",
            "channel_name": channel_name,
            "source_archive_dir": fs::canonicalize(review_archive_dir).expect("canonical review archive").display().to_string(),
            "selected_generation_id": generation.generation_id,
            "generation_identity": {
                "decision_packet_generated_at": "2026-03-26T12:00:00Z",
                "prod_config_fingerprint_sha256": "prod_fp",
                "non_prod_config_fingerprint_sha256": "non_prod_fp"
            },
            "decision_packet_paths": [generation.packet_path],
            "runbook_json_paths": [generation.runbook_json_path],
            "runbook_markdown_paths": [generation.runbook_markdown_path],
            "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized",
            "manifest_path": null,
            "bundle_path": null,
            "promoted_at": "2026-03-26T12:06:00Z",
            "build_version": "0.1.0",
            "git_commit": "deadbeef"
        });
        fs::write(
            review_channel_dir.join(format!("{channel_name}.json")),
            serde_json::to_string_pretty(&payload).expect("channel json"),
        )
        .expect("write review channel");
    }

    fn sample_packet_json(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": generated_at,
            "packet_version": "1",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "operator_note": null,
            "execution_enabled": false,
            "read_only_packet": true,
            "activation_authorized": false,
            "discussion_ready_only": true,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": "decision_packet_discussion_ready_but_not_authorized",
            "reason": "sample packet",
            "blockers": [],
            "warnings": ["planning-only"],
            "checklist_verdict": "activation_checklist_blocked_by_prod_stage3",
            "checklist_reason": "sample",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "prod_pre_activation_gate": {
                "verdict": "pre_activation_gates_green",
                "reason": "green",
                "planning_green": true,
                "blocked_by_stage3": false,
                "stage3_verdict": "fresh_current",
                "stage3_reason": "fresh",
                "stage3_captures_within_recent_horizon": 3,
                "stage3_latest_capture_age_seconds": 300,
                "stage4_readiness_verdict": "ready_for_execution_dry_run",
                "stage4_rehearsal_history_verdict": "sufficient_recent_rehearsal_evidence",
                "tiny_live_policy_verdict": "tiny_live_policy_bounded",
                "blockers": []
            },
            "launch_dossier": {
                "verdict": "activation_plan_ready_when_stage_gate_allows",
                "reason": "launch ready",
                "ready_when_stage_gate_allows": true,
                "activation_overlay_complete": true,
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "activation_overlay_change_count": 3,
                "rollback_overlay_change_count": 3,
                "drift_finding_count": 0,
                "blocker_count": 0,
                "first_blocker": null
            },
            "tiny_live_guardrails": {
                "verdict": "tiny_live_guardrails_bounded",
                "reason": "bounded",
                "bounded": true,
                "enabled": true,
                "blocker_count": 0,
                "first_blocker": null,
                "rollback_trigger_count": 1,
                "rollback_triggers": [{
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
                }]
            },
            "non_prod_readiness": {
                "verdict": "devnet_readiness_green",
                "reason": "green",
                "green": true,
                "prod_profile_refused": false,
                "config_env": "devnet",
                "blockers": [],
                "warnings": [],
                "dress_latest_record_age_seconds": 120,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 120,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": false
            }
        })
    }

    fn sample_runbook_json(
        generated_at: &str,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": generated_at,
            "runbook_version": "1",
            "prod_config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "json_output_path": null,
            "markdown_output_path": null,
            "execution_enabled": false,
            "read_only_runbook": true,
            "activation_authorized": false,
            "discussion_ready_only": true,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": "runbook_discussion_ready_but_not_authorized",
            "reason": "runbook sample",
            "blockers": [],
            "warnings": ["planning-only"],
            "not_authorized_disclaimer": "planning-only",
            "decision_packet_version": "1",
            "decision_packet_generated_at": decision_packet_generated_at,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "decision_packet_reason": "sample",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "prod_pre_activation_gate": {
                "verdict": "pre_activation_gates_green",
                "reason": "green",
                "planning_green": true,
                "blocked_by_stage3": false,
                "stage3_verdict": "fresh_current",
                "stage3_reason": "fresh",
                "stage3_captures_within_recent_horizon": 3,
                "stage3_latest_capture_age_seconds": 300,
                "stage4_readiness_verdict": "ready_for_execution_dry_run",
                "stage4_rehearsal_history_verdict": "sufficient_recent_rehearsal_evidence",
                "tiny_live_policy_verdict": "tiny_live_policy_bounded",
                "blockers": []
            },
            "launch_dossier": {
                "verdict": "activation_plan_ready_when_stage_gate_allows",
                "reason": "launch ready",
                "ready_when_stage_gate_allows": true,
                "activation_overlay_complete": true,
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "activation_overlay_change_count": 3,
                "rollback_overlay_change_count": 3,
                "drift_finding_count": 0,
                "blocker_count": 0,
                "first_blocker": null
            },
            "tiny_live_guardrails": {
                "verdict": "tiny_live_guardrails_bounded",
                "reason": "bounded",
                "bounded": true,
                "enabled": true,
                "blocker_count": 0,
                "first_blocker": null,
                "rollback_trigger_count": 1,
                "rollback_triggers": [{
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
                }]
            },
            "non_prod_readiness": {
                "verdict": "devnet_readiness_green",
                "reason": "green",
                "green": true,
                "prod_profile_refused": false,
                "config_env": "devnet",
                "blockers": [],
                "warnings": [],
                "dress_latest_record_age_seconds": 120,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 120,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": false
            },
            "section_order": [
                "current_state",
                "preflight_checks",
                "bounded_activation_candidate",
                "post_change_checks",
                "rollback_triggers",
                "rollback_procedure",
                "not_authorized_disclaimer"
            ]
        })
    }
}
