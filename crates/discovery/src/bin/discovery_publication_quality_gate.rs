use anyhow::{anyhow, bail, Context, Result};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_publication_quality_gate --config <path> --json";
const MAX_ACCEPTED_RUG_RATIO: f64 = 0.60;
const MIN_ACCEPTED_THIN_MARKET_VOLUME_SOL: f64 = 3.0;
const MIN_ACCEPTED_THIN_MARKET_UNIQUE_TRADERS: u32 = 10;

fn main() -> Result<()> {
    let cli = match Cli::parse(env::args().skip(1))? {
        Some(cli) => cli,
        None => {
            eprintln!("{USAGE}");
            return Ok(());
        }
    };

    let report = build_report(&cli.config_path);
    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", render_text_report(&report));
    }
    Ok(())
}

#[derive(Debug)]
struct Cli {
    config_path: PathBuf,
    json: bool,
}

impl Cli {
    fn parse<I>(args: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config_path: Option<PathBuf> = None;
        let mut json = false;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(None),
                "--config" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--config requires a path"))?;
                    config_path = Some(PathBuf::from(value));
                }
                "--json" => json = true,
                other => bail!("unrecognized argument: {other}"),
            }
        }

        let config_path = config_path.ok_or_else(|| anyhow!("--config is required"))?;
        Ok(Some(Self { config_path, json }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PublicationQualityGateReasonClass {
    PublicationQualityGateHardened,
    PublicationQualityGateOpenPositionRequiredMissing,
    PublicationQualityGateRugGateTooLoose,
    PublicationQualityGateThinMarketGateDisabled,
    PublicationQualityGateExecutionEnabled,
    PublicationQualityGateUnprovenDueToMissingEvidence,
}

#[derive(Debug, Serialize)]
struct PublicationQualityGateReport {
    publication_quality_gate_observed: bool,
    publication_quality_gate_reason_class: PublicationQualityGateReasonClass,
    publication_quality_gate_explanation: String,
    config_path: String,
    require_open_positions_for_publication: Option<bool>,
    max_rug_ratio: Option<f64>,
    thin_market_min_volume_sol: Option<f64>,
    thin_market_min_unique_traders: Option<u32>,
    execution_enabled: Option<bool>,
    quality_gates_hardened: bool,
    open_position_publication_required: bool,
    rug_gate_hardened: bool,
    thin_market_volume_gate_hardened: bool,
    thin_market_unique_trader_gate_hardened: bool,
    execution_disabled: bool,
    ready_for_publication_quality_rollout: bool,
}

#[derive(Debug)]
struct PublicationQualityEvidence {
    config_path: String,
    require_open_positions_for_publication: Option<bool>,
    max_rug_ratio: Option<f64>,
    thin_market_min_volume_sol: Option<f64>,
    thin_market_min_unique_traders: Option<u32>,
    execution_enabled: Option<bool>,
}

#[derive(Default)]
struct RequiredFieldPresence {
    require_open_positions_for_publication: bool,
    max_rug_ratio: bool,
    thin_market_min_volume_sol: bool,
    thin_market_min_unique_traders: bool,
    execution_enabled: bool,
}

fn build_report(config_path: &Path) -> PublicationQualityGateReport {
    match load_evidence(config_path) {
        Ok(evidence) => classify_evidence(evidence),
        Err(err) => PublicationQualityGateReport::unproven(
            config_path.display().to_string(),
            format!(
                "publication quality gate evidence is missing or unparseable: {err}; no runtime DBs or server state were inspected"
            ),
        ),
    }
}

fn load_evidence(config_path: &Path) -> Result<PublicationQualityEvidence> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed reading config {}", config_path.display()))?;
    let presence = required_field_presence(&raw);
    let config = load_from_path(config_path)
        .with_context(|| format!("failed parsing config {}", config_path.display()))?;

    Ok(PublicationQualityEvidence {
        config_path: config_path.display().to_string(),
        require_open_positions_for_publication: presence
            .require_open_positions_for_publication
            .then_some(config.discovery.require_open_positions_for_publication),
        max_rug_ratio: presence
            .max_rug_ratio
            .then_some(config.discovery.max_rug_ratio),
        thin_market_min_volume_sol: presence
            .thin_market_min_volume_sol
            .then_some(config.discovery.thin_market_min_volume_sol),
        thin_market_min_unique_traders: presence
            .thin_market_min_unique_traders
            .then_some(config.discovery.thin_market_min_unique_traders),
        execution_enabled: presence
            .execution_enabled
            .then_some(config.execution.enabled),
    })
}

fn required_field_presence(raw: &str) -> RequiredFieldPresence {
    let mut presence = RequiredFieldPresence::default();
    let mut section = "";

    for line in raw.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if let Some(name) = table_name(line) {
            section = name;
            continue;
        }

        match section {
            "discovery" => {
                presence.require_open_positions_for_publication |=
                    has_key(line, "require_open_positions_for_publication");
                presence.max_rug_ratio |= has_key(line, "max_rug_ratio");
                presence.thin_market_min_volume_sol |= has_key(line, "thin_market_min_volume_sol");
                presence.thin_market_min_unique_traders |=
                    has_key(line, "thin_market_min_unique_traders");
            }
            "execution" => {
                presence.execution_enabled |= has_key(line, "enabled");
            }
            _ => {}
        }
    }

    presence
}

fn table_name(line: &str) -> Option<&str> {
    line.strip_prefix('[')?.strip_suffix(']')
}

fn has_key(line: &str, key: &str) -> bool {
    let Some(rest) = line.strip_prefix(key) else {
        return false;
    };
    rest.trim_start().starts_with('=')
}

fn classify_evidence(evidence: PublicationQualityEvidence) -> PublicationQualityGateReport {
    let open_position_publication_required =
        evidence.require_open_positions_for_publication == Some(true);
    let rug_gate_hardened = evidence
        .max_rug_ratio
        .is_some_and(|value| value <= MAX_ACCEPTED_RUG_RATIO);
    let thin_market_volume_gate_hardened = evidence
        .thin_market_min_volume_sol
        .is_some_and(|value| value >= MIN_ACCEPTED_THIN_MARKET_VOLUME_SOL);
    let thin_market_unique_trader_gate_hardened = evidence
        .thin_market_min_unique_traders
        .is_some_and(|value| value >= MIN_ACCEPTED_THIN_MARKET_UNIQUE_TRADERS);
    let execution_disabled = evidence.execution_enabled == Some(false);
    let all_required_evidence_present = evidence.require_open_positions_for_publication.is_some()
        && evidence.max_rug_ratio.is_some()
        && evidence.thin_market_min_volume_sol.is_some()
        && evidence.thin_market_min_unique_traders.is_some()
        && evidence.execution_enabled.is_some();

    if !all_required_evidence_present {
        return PublicationQualityGateReport::from_evidence(
            evidence,
            false,
            PublicationQualityGateReasonClass::PublicationQualityGateUnprovenDueToMissingEvidence,
            "publication quality gate evidence is missing one or more required explicit config fields",
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
        );
    }

    if evidence.execution_enabled == Some(true) {
        return PublicationQualityGateReport::from_evidence(
            evidence,
            true,
            PublicationQualityGateReasonClass::PublicationQualityGateExecutionEnabled,
            "execution.enabled=true is non-green for this read-only publication quality proof",
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
        );
    }

    if !open_position_publication_required {
        return PublicationQualityGateReport::from_evidence(
            evidence,
            true,
            PublicationQualityGateReasonClass::PublicationQualityGateOpenPositionRequiredMissing,
            "discovery.require_open_positions_for_publication is not true",
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
        );
    }

    if !rug_gate_hardened {
        return PublicationQualityGateReport::from_evidence(
            evidence,
            true,
            PublicationQualityGateReasonClass::PublicationQualityGateRugGateTooLoose,
            "discovery.max_rug_ratio is above the accepted 0.60 ceiling",
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
        );
    }

    if !thin_market_volume_gate_hardened || !thin_market_unique_trader_gate_hardened {
        return PublicationQualityGateReport::from_evidence(
            evidence,
            true,
            PublicationQualityGateReasonClass::PublicationQualityGateThinMarketGateDisabled,
            "one or more discovery thin-market publication gates are below the accepted production floor",
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
        );
    }

    PublicationQualityGateReport::from_evidence(
        evidence,
        true,
        PublicationQualityGateReasonClass::PublicationQualityGateHardened,
        "publication quality gates are hardened in the supplied config; this proof is read-only and does not inspect runtime DBs or server state",
        open_position_publication_required,
        rug_gate_hardened,
        thin_market_volume_gate_hardened,
        thin_market_unique_trader_gate_hardened,
        execution_disabled,
    )
}

impl PublicationQualityGateReport {
    fn from_evidence(
        evidence: PublicationQualityEvidence,
        observed: bool,
        reason_class: PublicationQualityGateReasonClass,
        explanation: &str,
        open_position_publication_required: bool,
        rug_gate_hardened: bool,
        thin_market_volume_gate_hardened: bool,
        thin_market_unique_trader_gate_hardened: bool,
        execution_disabled: bool,
    ) -> Self {
        let quality_gates_hardened = open_position_publication_required
            && rug_gate_hardened
            && thin_market_volume_gate_hardened
            && thin_market_unique_trader_gate_hardened
            && execution_disabled;

        Self {
            publication_quality_gate_observed: observed,
            publication_quality_gate_reason_class: reason_class,
            publication_quality_gate_explanation: explanation.to_string(),
            config_path: evidence.config_path,
            require_open_positions_for_publication: evidence.require_open_positions_for_publication,
            max_rug_ratio: evidence.max_rug_ratio,
            thin_market_min_volume_sol: evidence.thin_market_min_volume_sol,
            thin_market_min_unique_traders: evidence.thin_market_min_unique_traders,
            execution_enabled: evidence.execution_enabled,
            quality_gates_hardened,
            open_position_publication_required,
            rug_gate_hardened,
            thin_market_volume_gate_hardened,
            thin_market_unique_trader_gate_hardened,
            execution_disabled,
            ready_for_publication_quality_rollout: quality_gates_hardened,
        }
    }

    fn unproven(config_path: String, explanation: String) -> Self {
        Self {
            publication_quality_gate_observed: false,
            publication_quality_gate_reason_class:
                PublicationQualityGateReasonClass::PublicationQualityGateUnprovenDueToMissingEvidence,
            publication_quality_gate_explanation: explanation,
            config_path,
            require_open_positions_for_publication: None,
            max_rug_ratio: None,
            thin_market_min_volume_sol: None,
            thin_market_min_unique_traders: None,
            execution_enabled: None,
            quality_gates_hardened: false,
            open_position_publication_required: false,
            rug_gate_hardened: false,
            thin_market_volume_gate_hardened: false,
            thin_market_unique_trader_gate_hardened: false,
            execution_disabled: false,
            ready_for_publication_quality_rollout: false,
        }
    }
}

fn render_text_report(report: &PublicationQualityGateReport) -> String {
    format!(
        "publication_quality_gate_observed={}\npublication_quality_gate_reason_class={:?}\nquality_gates_hardened={}\nready_for_publication_quality_rollout={}\n{}",
        report.publication_quality_gate_observed,
        report.publication_quality_gate_reason_class,
        report.quality_gates_hardened,
        report.ready_for_publication_quality_rollout,
        report.publication_quality_gate_explanation
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn green_config_with_accepted_values_returns_hardened() {
        let fixture = ConfigFixture::from_live("green");

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateHardened
        );
        assert!(report.publication_quality_gate_observed);
        assert!(report.quality_gates_hardened);
        assert!(report.ready_for_publication_quality_rollout);
        assert_eq!(report.require_open_positions_for_publication, Some(true));
        assert_eq!(report.max_rug_ratio, Some(0.60));
        assert_eq!(report.thin_market_min_volume_sol, Some(3.0));
        assert_eq!(report.thin_market_min_unique_traders, Some(10));
        assert_eq!(report.execution_enabled, Some(false));
    }

    #[test]
    fn require_open_positions_false_returns_open_position_required_missing() {
        let fixture = ConfigFixture::from_live_with_replace(
            "open_positions_false",
            "require_open_positions_for_publication = true",
            "require_open_positions_for_publication = false",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateOpenPositionRequiredMissing
        );
        assert!(!report.quality_gates_hardened);
        assert!(!report.open_position_publication_required);
    }

    #[test]
    fn max_rug_ratio_above_ceiling_returns_rug_gate_too_loose() {
        let fixture = ConfigFixture::from_live_with_replace(
            "rug_too_loose",
            "max_rug_ratio = 0.60",
            "max_rug_ratio = 0.61",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateRugGateTooLoose
        );
        assert!(!report.rug_gate_hardened);
        assert!(!report.quality_gates_hardened);
    }

    #[test]
    fn thin_market_volume_below_floor_returns_thin_market_gate_disabled() {
        let fixture = ConfigFixture::from_live_with_replace(
            "thin_volume_disabled",
            "thin_market_min_volume_sol = 3.0",
            "thin_market_min_volume_sol = 2.9",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateThinMarketGateDisabled
        );
        assert!(!report.thin_market_volume_gate_hardened);
        assert!(!report.quality_gates_hardened);
    }

    #[test]
    fn thin_market_unique_traders_below_floor_returns_thin_market_gate_disabled() {
        let fixture = ConfigFixture::from_live_with_replace(
            "thin_traders_disabled",
            "thin_market_min_unique_traders = 10",
            "thin_market_min_unique_traders = 9",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateThinMarketGateDisabled
        );
        assert!(!report.thin_market_unique_trader_gate_hardened);
        assert!(!report.quality_gates_hardened);
    }

    #[test]
    fn execution_enabled_true_returns_execution_enabled() {
        let fixture = ConfigFixture::from_live_with_replace_in_section(
            "execution_enabled",
            "execution",
            "enabled = false",
            "enabled = true",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateExecutionEnabled
        );
        assert_eq!(report.execution_enabled, Some(true));
        assert!(!report.execution_disabled);
        assert!(!report.quality_gates_hardened);
    }

    #[test]
    fn missing_or_unparseable_config_returns_unproven() {
        let missing = PathBuf::from("/definitely/not/a/copybot/config.toml");
        let missing_report = build_report(&missing);
        assert_eq!(
            missing_report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateUnprovenDueToMissingEvidence
        );
        assert!(!missing_report.publication_quality_gate_observed);

        let fixture = ConfigFixture::from_contents("unparseable", "not valid toml = [");
        let unparseable_report = build_report(&fixture.config_path);
        assert_eq!(
            unparseable_report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateUnprovenDueToMissingEvidence
        );
        assert!(!unparseable_report.publication_quality_gate_observed);
    }

    #[test]
    fn missing_required_field_returns_unproven_instead_of_using_defaults() {
        let fixture = ConfigFixture::from_live_with_removed_line(
            "missing_open_position_gate",
            "require_open_positions_for_publication",
        );

        let report = build_report(&fixture.config_path);

        assert_eq!(
            report.publication_quality_gate_reason_class,
            PublicationQualityGateReasonClass::PublicationQualityGateUnprovenDueToMissingEvidence
        );
        assert_eq!(report.require_open_positions_for_publication, None);
        assert!(!report.quality_gates_hardened);
    }

    #[test]
    fn json_output_contains_all_required_fields() {
        let fixture = ConfigFixture::from_live("json_fields");
        let report = build_report(&fixture.config_path);
        let value = serde_json::to_value(report).expect("serialize report");
        let object = value.as_object().expect("report JSON object");

        for field in [
            "publication_quality_gate_observed",
            "publication_quality_gate_reason_class",
            "publication_quality_gate_explanation",
            "config_path",
            "require_open_positions_for_publication",
            "max_rug_ratio",
            "thin_market_min_volume_sol",
            "thin_market_min_unique_traders",
            "execution_enabled",
            "quality_gates_hardened",
            "open_position_publication_required",
            "rug_gate_hardened",
            "thin_market_volume_gate_hardened",
            "thin_market_unique_trader_gate_hardened",
            "execution_disabled",
            "ready_for_publication_quality_rollout",
        ] {
            assert!(object.contains_key(field), "missing JSON field {field}");
        }

        assert_eq!(
            object
                .get("publication_quality_gate_reason_class")
                .and_then(Value::as_str),
            Some("publication_quality_gate_hardened")
        );
    }

    struct ConfigFixture {
        _temp_dir: TempDir,
        config_path: PathBuf,
    }

    impl ConfigFixture {
        fn from_live(label: &str) -> Self {
            Self::from_contents(label, &live_config_contents())
        }

        fn from_live_with_replace(label: &str, old: &str, new: &str) -> Self {
            let contents = live_config_contents();
            assert!(
                contents.contains(old),
                "live config fixture must contain replacement target {old:?}"
            );
            Self::from_contents(label, &contents.replacen(old, new, 1))
        }

        fn from_live_with_replace_in_section(
            label: &str,
            section: &str,
            old: &str,
            new: &str,
        ) -> Self {
            let contents = replace_in_section(&live_config_contents(), section, old, new);
            Self::from_contents(label, &contents)
        }

        fn from_live_with_removed_line(label: &str, key: &str) -> Self {
            let contents = live_config_contents()
                .lines()
                .filter(|line| !line.trim_start().starts_with(key))
                .collect::<Vec<_>>()
                .join("\n");
            Self::from_contents(label, &contents)
        }

        fn from_contents(label: &str, contents: &str) -> Self {
            let temp_dir = tempfile::Builder::new()
                .prefix(&format!("publication_quality_gate_{label}_"))
                .tempdir()
                .expect("create temp dir");
            let config_path = temp_dir.path().join("copybot.toml");
            fs::write(&config_path, contents).expect("write fixture config");
            Self {
                _temp_dir: temp_dir,
                config_path,
            }
        }
    }

    fn live_config_contents() -> String {
        fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/live.toml"),
        )
        .expect("read configs/live.toml")
    }

    fn replace_in_section(contents: &str, section: &str, old: &str, new: &str) -> String {
        let mut current_section = "";
        let mut replaced = false;
        let mut output = Vec::new();

        for line in contents.lines() {
            let trimmed = line.trim();
            if let Some(name) = table_name(trimmed) {
                current_section = name;
            }
            if current_section == section && !replaced && trimmed == old {
                output.push(line.replacen(old, new, 1));
                replaced = true;
            } else {
                output.push(line.to_string());
            }
        }

        assert!(
            replaced,
            "section [{section}] must contain replacement target {old:?}"
        );
        output.join("\n")
    }
}
