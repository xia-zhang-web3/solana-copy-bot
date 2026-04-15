use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const TOP_WRAPPER_BIN: &str = "copybot_tiny_live_activation_package_clerestory_certificate_latest";
const PLAN_READY_VERDICT: &str = "tiny_live_package_clerestory_certificate_latest_plan_ready";
const VERIFY_OK_VERDICT: &str = "tiny_live_package_clerestory_certificate_latest_verify_ok";
const DEFAULT_TOP_WRAPPER_SESSION_DIR: &str =
    "tiny_live_activation_package_clerestory_certificate_latest.session";
const NON_AUTHORIZING_EXPLANATION: &str = "Stage 3 production truth remains the hard gate; this closure surface is read-only, planning-safe, and non-authorizing.";

fn main() -> Result<()> {
    let config = match Config::parse(env::args().skip(1))? {
        Some(config) => config,
        None => {
            eprintln!("{USAGE}");
            return Ok(());
        }
    };

    let report = run_report(&config);
    if config.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", render_text_report(&report));
    }
    Ok(())
}

const USAGE: &str = r#"Usage:
  copybot_tiny_live_activation_package_latest_chain_closure --plan-latest-chain-closure --root <dir> --json
  copybot_tiny_live_activation_package_latest_chain_closure --verify-latest-chain-closure --root <dir> [--session-dir <dir>] --json"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    PlanLatestChainClosure,
    VerifyLatestChainClosure,
}

impl Mode {
    fn top_wrapper_mode(self) -> &'static str {
        match self {
            Self::PlanLatestChainClosure => "plan_latest_clerestory_certificate",
            Self::VerifyLatestChainClosure => "verify_latest_clerestory_certificate",
        }
    }

    fn expected_verdict(self) -> &'static str {
        match self {
            Self::PlanLatestChainClosure => PLAN_READY_VERDICT,
            Self::VerifyLatestChainClosure => VERIFY_OK_VERDICT,
        }
    }
}

#[derive(Debug)]
struct Config {
    mode: Mode,
    root: PathBuf,
    session_dir: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn parse<I>(args: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut mode: Option<Mode> = None;
        let mut root: Option<PathBuf> = None;
        let mut session_dir: Option<PathBuf> = None;
        let mut json = false;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(None),
                "--plan-latest-chain-closure" => set_mode(&mut mode, Mode::PlanLatestChainClosure)?,
                "--verify-latest-chain-closure" => {
                    set_mode(&mut mode, Mode::VerifyLatestChainClosure)?
                }
                "--root" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--root requires a directory"))?;
                    root = Some(PathBuf::from(value));
                }
                "--session-dir" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--session-dir requires a directory"))?;
                    session_dir = Some(PathBuf::from(value));
                }
                "--json" => json = true,
                other => bail!("unrecognized argument: {other}"),
            }
        }

        let Some(mode) = mode else {
            return Ok(None);
        };
        let root = root.ok_or_else(|| anyhow!("--root is required"))?;
        if mode == Mode::PlanLatestChainClosure && session_dir.is_some() {
            bail!("--session-dir is not accepted for --plan-latest-chain-closure");
        }

        Ok(Some(Self {
            mode,
            root,
            session_dir,
            json,
        }))
    }
}

fn set_mode(slot: &mut Option<Mode>, next: Mode) -> Result<()> {
    if slot.replace(next).is_some() {
        bail!("exactly one latest-chain-closure mode must be selected");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LatestChainClosureReasonClass {
    LatestChainClosureClosedThroughClerestory,
    LatestChainClosureTopWrapperMissingOrFailed,
    LatestChainClosureTopWrapperNotGreen,
    LatestChainClosureTopStepMismatch,
    LatestChainClosureActivationAuthorizedDrift,
    LatestChainClosureUnprovenDueToMissingEvidence,
}

#[derive(Debug, Serialize)]
struct LatestChainClosureReport {
    latest_chain_closure_observed: bool,
    latest_chain_closure_reason_class: LatestChainClosureReasonClass,
    latest_chain_closure_explanation: String,
    top_accepted_package_step: String,
    top_wrapper_bin: String,
    top_wrapper_mode: Option<String>,
    top_wrapper_verdict: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    top_wrapper_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_chain_closed_through_clerestory: bool,
    stage3_gate_overridden: bool,
    activation_authorized: bool,
    planning_safe_only: bool,
}

#[derive(Debug, Default, Deserialize)]
struct TopWrapperReportView {
    mode: Option<String>,
    verdict: Option<String>,
    reason: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    activation_authorized: Option<bool>,
}

fn run_report(config: &Config) -> LatestChainClosureReport {
    let verify_session_dir = match config.mode {
        Mode::PlanLatestChainClosure => config.session_dir.clone(),
        Mode::VerifyLatestChainClosure => Some(
            config
                .session_dir
                .clone()
                .unwrap_or_else(|| config.root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR)),
        ),
    };

    let top_wrapper_result = match config.mode {
        Mode::PlanLatestChainClosure => run_top_wrapper_plan(&config.root),
        Mode::VerifyLatestChainClosure => {
            let session_dir = verify_session_dir
                .as_ref()
                .expect("verify_session_dir is always populated for verify mode");
            run_top_wrapper_verify(session_dir)
        }
    };

    match top_wrapper_result {
        Ok(value) => match serde_json::from_value::<TopWrapperReportView>(value) {
            Ok(view) => classify_top_wrapper_report(config.mode, view, verify_session_dir.as_deref()),
            Err(err) => failure_report(
                config.mode,
                LatestChainClosureReasonClass::LatestChainClosureTopWrapperMissingOrFailed,
                format!(
                    "failed parsing accepted top latest-clerestory wrapper JSON: {err}. {NON_AUTHORIZING_EXPLANATION}"
                ),
                verify_session_dir.as_deref(),
            ),
        },
        Err(err) => failure_report(
            config.mode,
            LatestChainClosureReasonClass::LatestChainClosureTopWrapperMissingOrFailed,
            format!(
                "failed invoking accepted top latest-clerestory wrapper: {err}. {NON_AUTHORIZING_EXPLANATION}"
            ),
            verify_session_dir.as_deref(),
        ),
    }
}

fn run_top_wrapper_plan(root: &Path) -> Result<Value> {
    run_json_command(
        TOP_WRAPPER_BIN,
        &[
            "--plan-latest-clerestory-certificate",
            "--root",
            &root.display().to_string(),
            "--json",
        ],
    )
}

fn run_top_wrapper_verify(session_dir: &Path) -> Result<Value> {
    run_json_command(
        TOP_WRAPPER_BIN,
        &[
            "--verify-latest-clerestory-certificate",
            "--session-dir",
            &session_dir.display().to_string(),
            "--json",
        ],
    )
}

fn run_json_command(binary: &str, args: &[&str]) -> Result<Value> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("failed to spawn {binary}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        bail!(
            "{binary} exited with status {:?}; stderr={stderr:?}; stdout={stdout:?}",
            output.status.code()
        );
    }

    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON from {binary}"))
}

fn classify_top_wrapper_report(
    mode: Mode,
    view: TopWrapperReportView,
    fallback_session_dir: Option<&Path>,
) -> LatestChainClosureReport {
    let top_wrapper_mode = view.mode.clone();
    let top_wrapper_session_dir = view
        .session_dir
        .clone()
        .or_else(|| fallback_session_dir.map(|session_dir| session_dir.display().to_string()));

    if view.activation_authorized == Some(true) {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureActivationAuthorizedDrift,
            format!(
                "accepted top latest-clerestory wrapper reported activation_authorized=true; closure refuses authorization drift. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    }

    let Some(activation_authorized) = view.activation_authorized else {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report activation_authorized=false. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    };
    if activation_authorized {
        unreachable!("activation_authorized=true handled above");
    }

    let Some(reported_mode) = view.mode.as_deref() else {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report mode {:?}. {NON_AUTHORIZING_EXPLANATION}",
                mode.top_wrapper_mode()
            ),
        );
    };
    if reported_mode != mode.top_wrapper_mode() {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper reported mode {reported_mode:?}, expected {:?}. {NON_AUTHORIZING_EXPLANATION}",
                mode.top_wrapper_mode()
            ),
        );
    }

    let Some(latest_top_step_name) = view.latest_top_step_name.as_deref() else {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report latest_top_step_name. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    };
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureTopStepMismatch,
            format!(
                "accepted top latest-clerestory wrapper reported latest_top_step_name={latest_top_step_name:?}, expected {TOP_ACCEPTED_PACKAGE_STEP:?}. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    }

    let Some(verdict) = view.verdict.as_deref() else {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report a verdict. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    };
    if verdict != mode.expected_verdict() {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureTopWrapperNotGreen,
            format!(
                "accepted top latest-clerestory wrapper verdict {verdict:?} did not match expected {:?}; top wrapper reason={:?}. {NON_AUTHORIZING_EXPLANATION}",
                mode.expected_verdict(),
                view.reason
            ),
        );
    }

    if is_missing(&view.latest_top_session_dir) {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report latest_top_session_dir. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    }

    if is_missing(&top_wrapper_session_dir) {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report top wrapper session_dir. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    }

    if is_missing(&view.downstream_decision_packet_session_dir) {
        return report_from_view(
            mode,
            &view,
            top_wrapper_mode,
            top_wrapper_session_dir,
            false,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence,
            format!(
                "accepted top latest-clerestory wrapper did not report propagated downstream_decision_packet_session_dir. {NON_AUTHORIZING_EXPLANATION}"
            ),
        );
    }

    report_from_view(
        mode,
        &view,
        top_wrapper_mode,
        top_wrapper_session_dir,
        true,
        LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory,
        format!(
            "latest immutable tiny-live package chain is closed through accepted top step {TOP_ACCEPTED_PACKAGE_STEP:?} by the accepted latest-clerestory wrapper. {NON_AUTHORIZING_EXPLANATION}"
        ),
    )
}

fn is_missing(value: &Option<String>) -> bool {
    value.as_deref().is_none_or(|value| value.trim().is_empty())
}

fn report_from_view(
    _mode: Mode,
    view: &TopWrapperReportView,
    top_wrapper_mode: Option<String>,
    top_wrapper_session_dir: Option<String>,
    closed: bool,
    reason_class: LatestChainClosureReasonClass,
    explanation: String,
) -> LatestChainClosureReport {
    LatestChainClosureReport {
        latest_chain_closure_observed: closed,
        latest_chain_closure_reason_class: reason_class,
        latest_chain_closure_explanation: explanation,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        top_wrapper_bin: TOP_WRAPPER_BIN.to_string(),
        top_wrapper_mode,
        top_wrapper_verdict: view.verdict.clone(),
        latest_top_step_name: view.latest_top_step_name.clone(),
        latest_top_session_dir: view.latest_top_session_dir.clone(),
        top_wrapper_session_dir,
        downstream_decision_packet_session_dir: view.downstream_decision_packet_session_dir.clone(),
        latest_chain_closed_through_clerestory: closed,
        stage3_gate_overridden: false,
        activation_authorized: false,
        planning_safe_only: true,
    }
}

fn failure_report(
    mode: Mode,
    reason_class: LatestChainClosureReasonClass,
    explanation: String,
    fallback_session_dir: Option<&Path>,
) -> LatestChainClosureReport {
    LatestChainClosureReport {
        latest_chain_closure_observed: false,
        latest_chain_closure_reason_class: reason_class,
        latest_chain_closure_explanation: explanation,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        top_wrapper_bin: TOP_WRAPPER_BIN.to_string(),
        top_wrapper_mode: Some(mode.top_wrapper_mode().to_string()),
        top_wrapper_verdict: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        top_wrapper_session_dir: fallback_session_dir.map(|path| path.display().to_string()),
        downstream_decision_packet_session_dir: None,
        latest_chain_closed_through_clerestory: false,
        stage3_gate_overridden: false,
        activation_authorized: false,
        planning_safe_only: true,
    }
}

fn render_text_report(report: &LatestChainClosureReport) -> String {
    format!(
        "latest_chain_closure_observed={}\nlatest_chain_closure_reason_class={:?}\nlatest_chain_closed_through_clerestory={}\nstage3_gate_overridden={}\nactivation_authorized={}\nplanning_safe_only={}\n{}",
        report.latest_chain_closure_observed,
        report.latest_chain_closure_reason_class,
        report.latest_chain_closed_through_clerestory,
        report.stage3_gate_overridden,
        report.activation_authorized,
        report.planning_safe_only,
        report.latest_chain_closure_explanation
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    static PATH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn parse_rejects_plan_mode_session_dir_before_top_wrapper_can_run() {
        let err = Config::parse([
            "--plan-latest-chain-closure".to_string(),
            "--root".to_string(),
            "/bounded/root".to_string(),
            "--session-dir".to_string(),
            "/must/not/be/accepted".to_string(),
            "--json".to_string(),
        ])
        .expect_err("plan mode must reject --session-dir");

        assert!(
            err.to_string().contains("--session-dir is not accepted"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn plan_mode_returns_closed_through_clerestory_when_top_wrapper_plan_ready() {
        let fixture = Fixture::new("plan_closed");
        let root = fixture.path("root");
        let propagated_anchor = fixture.path("propagated/latest-choir/decision_packet_session");
        let top_session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            top_session.as_path(),
            propagated_anchor.as_path(),
        );
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory
        );
        assert!(report.latest_chain_closure_observed);
        assert!(report.latest_chain_closed_through_clerestory);
        assert!(!report.stage3_gate_overridden);
        assert!(!report.activation_authorized);
        assert!(report.planning_safe_only);
        assert_eq!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(propagated_anchor.display().to_string().as_str())
        );
    }

    #[test]
    fn plan_mode_rejects_top_step_below_or_different_from_clerestory_certificate() {
        for top_step in ["choir_receipt", "not_clerestory_certificate"] {
            let fixture = Fixture::new(top_step);
            let root = fixture.path("root");
            let plan_json = top_wrapper_json(
                "plan_latest_clerestory_certificate",
                PLAN_READY_VERDICT,
                top_step,
                false,
                root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
                fixture.path("propagated/decision_packet_session").as_path(),
            );
            let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

            let report = run_report(&Config {
                mode: Mode::PlanLatestChainClosure,
                root,
                session_dir: None,
                json: true,
            });

            assert_eq!(
                report.latest_chain_closure_reason_class,
                LatestChainClosureReasonClass::LatestChainClosureTopStepMismatch
            );
            assert!(!report.latest_chain_closed_through_clerestory);
        }
    }

    #[test]
    fn plan_mode_rejects_activation_authorized_true() {
        let fixture = Fixture::new("activation_authorized_drift");
        let root = fixture.path("root");
        let plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            true,
            root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
            fixture.path("propagated/decision_packet_session").as_path(),
        );
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureActivationAuthorizedDrift
        );
        assert!(!report.latest_chain_closed_through_clerestory);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn verify_mode_uses_explicit_session_dir_when_provided() {
        let fixture = Fixture::new("verify_explicit_session");
        let root = fixture.path("root");
        let explicit_session = fixture.path("operator/explicit_clerestory.session");
        let verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            VERIFY_OK_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            explicit_session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&explicit_session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: Some(explicit_session.clone()),
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory
        );
        assert_eq!(
            report.top_wrapper_session_dir.as_deref(),
            Some(explicit_session.display().to_string().as_str())
        );
    }

    #[test]
    fn verify_mode_defaults_session_dir_from_root_when_omitted() {
        let fixture = Fixture::new("verify_default_session");
        let root = fixture.path("root");
        let default_session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            VERIFY_OK_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            default_session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&default_session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory
        );
        assert_eq!(
            report.top_wrapper_session_dir.as_deref(),
            Some(default_session.display().to_string().as_str())
        );
    }

    #[test]
    fn verify_mode_rejects_non_green_top_wrapper_verify_verdict() {
        let fixture = Fixture::new("verify_non_green");
        let root = fixture.path("root");
        let session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            "tiny_live_package_clerestory_certificate_latest_verify_invalid",
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureTopWrapperNotGreen
        );
        assert!(!report.latest_chain_closed_through_clerestory);
    }

    #[test]
    fn closure_report_preserves_propagated_downstream_decision_packet_anchor_without_synthesis() {
        let fixture = Fixture::new("propagated_anchor");
        let root = fixture.path("root");
        let top_session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let propagated_anchor = fixture.path(
            "latest_choir_real_lineage/tiny_live_activation_package_transept_certificate_latest.decision_packet_session",
        );
        let synthetic_anchor = top_session.join(
            "tiny_live_activation_package_clerestory_certificate_latest.choir_receipt_latest_session/tiny_live_activation_package_choir_receipt_latest.decision_packet_session",
        );
        let plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            top_session.as_path(),
            propagated_anchor.as_path(),
        );
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(propagated_anchor.display().to_string().as_str())
        );
        assert_ne!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(synthetic_anchor.display().to_string().as_str())
        );
    }

    #[test]
    fn green_top_wrapper_without_downstream_decision_packet_anchor_is_unproven() {
        let fixture = Fixture::new("missing_downstream_anchor");
        let root = fixture.path("root");
        let mut plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
            fixture.path("propagated/decision_packet_session").as_path(),
        );
        remove_json_field(&mut plan_json, "downstream_decision_packet_session_dir");
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence
        );
        assert!(!report.latest_chain_closed_through_clerestory);
        assert!(report.downstream_decision_packet_session_dir.is_none());
    }

    #[test]
    fn green_top_wrapper_with_wrong_mode_is_unproven() {
        let fixture = Fixture::new("wrong_mode");
        let root = fixture.path("root");
        let plan_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
            fixture.path("propagated/decision_packet_session").as_path(),
        );
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence
        );
        assert!(!report.latest_chain_closed_through_clerestory);
    }

    #[test]
    fn green_top_wrapper_without_latest_top_session_dir_is_unproven() {
        let fixture = Fixture::new("missing_latest_top_session");
        let root = fixture.path("root");
        let mut plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
            fixture.path("propagated/decision_packet_session").as_path(),
        );
        remove_json_field(&mut plan_json, "latest_top_session_dir");
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence
        );
        assert!(!report.latest_chain_closed_through_clerestory);
    }

    #[test]
    fn plan_mode_without_top_wrapper_session_dir_is_unproven() {
        let fixture = Fixture::new("missing_plan_session");
        let root = fixture.path("root");
        let mut plan_json = top_wrapper_json(
            "plan_latest_clerestory_certificate",
            PLAN_READY_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR).as_path(),
            fixture.path("propagated/decision_packet_session").as_path(),
        );
        remove_json_field(&mut plan_json, "session_dir");
        let _path_guard = fixture.install_fake_top_wrapper(&plan_json, &plan_json, None);

        let report = run_report(&Config {
            mode: Mode::PlanLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence
        );
        assert!(!report.latest_chain_closed_through_clerestory);
        assert!(report.top_wrapper_session_dir.is_none());
    }

    #[test]
    fn verify_mode_uses_session_fallback_when_output_omits_session_dir_but_still_requires_anchor() {
        let fixture = Fixture::new("verify_session_fallback_requires_anchor");
        let root = fixture.path("root");
        let default_session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let mut verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            VERIFY_OK_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            default_session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        remove_json_field(&mut verify_json, "session_dir");
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&default_session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory
        );
        assert_eq!(
            report.top_wrapper_session_dir.as_deref(),
            Some(default_session.display().to_string().as_str())
        );
    }

    #[test]
    fn verify_mode_uses_explicit_session_fallback_when_output_omits_session_dir_with_anchor() {
        let fixture = Fixture::new("verify_explicit_session_fallback_requires_anchor");
        let root = fixture.path("root");
        let explicit_session = fixture.path("operator/explicit_clerestory.session");
        let mut verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            VERIFY_OK_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            explicit_session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        remove_json_field(&mut verify_json, "session_dir");
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&explicit_session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: Some(explicit_session.clone()),
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureClosedThroughClerestory
        );
        assert_eq!(
            report.top_wrapper_session_dir.as_deref(),
            Some(explicit_session.display().to_string().as_str())
        );
        assert!(report
            .downstream_decision_packet_session_dir
            .as_deref()
            .is_some_and(|path| path.contains("propagated/latest-choir")));
    }

    #[test]
    fn verify_mode_with_session_fallback_still_rejects_missing_downstream_anchor() {
        let fixture = Fixture::new("verify_fallback_missing_anchor");
        let root = fixture.path("root");
        let default_session = root.join(DEFAULT_TOP_WRAPPER_SESSION_DIR);
        let mut verify_json = top_wrapper_json(
            "verify_latest_clerestory_certificate",
            VERIFY_OK_VERDICT,
            TOP_ACCEPTED_PACKAGE_STEP,
            false,
            default_session.as_path(),
            fixture
                .path("propagated/latest-choir/decision_packet_session")
                .as_path(),
        );
        remove_json_field(&mut verify_json, "session_dir");
        remove_json_field(&mut verify_json, "downstream_decision_packet_session_dir");
        let _path_guard =
            fixture.install_fake_top_wrapper(&verify_json, &verify_json, Some(&default_session));

        let report = run_report(&Config {
            mode: Mode::VerifyLatestChainClosure,
            root,
            session_dir: None,
            json: true,
        });

        assert_eq!(
            report.latest_chain_closure_reason_class,
            LatestChainClosureReasonClass::LatestChainClosureUnprovenDueToMissingEvidence
        );
        assert!(!report.latest_chain_closed_through_clerestory);
        assert_eq!(
            report.top_wrapper_session_dir.as_deref(),
            Some(default_session.display().to_string().as_str())
        );
        assert!(report.downstream_decision_packet_session_dir.is_none());
    }

    fn top_wrapper_json(
        mode: &str,
        verdict: &str,
        latest_top_step_name: &str,
        activation_authorized: bool,
        session_dir: &Path,
        downstream_decision_packet_session_dir: &Path,
    ) -> Value {
        json!({
            "mode": mode,
            "verdict": verdict,
            "reason": "fake bounded top wrapper evidence",
            "latest_top_step_name": latest_top_step_name,
            "latest_top_session_dir": "/accepted/latest/clerestory_certificate.session",
            "session_dir": session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": downstream_decision_packet_session_dir.display().to_string(),
            "activation_authorized": activation_authorized
        })
    }

    fn remove_json_field(value: &mut Value, field: &str) {
        value
            .as_object_mut()
            .expect("fixture top wrapper JSON must be an object")
            .remove(field);
    }

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before epoch")
                .as_nanos();
            let root = env::temp_dir().join(format!(
                "copybot_latest_chain_closure_{}_{}_{}",
                label,
                std::process::id(),
                nanos
            ));
            fs::create_dir_all(&root).expect("create fixture root");
            Self { root }
        }

        fn path(&self, relative: &str) -> PathBuf {
            self.root.join(relative)
        }

        fn install_fake_top_wrapper(
            &self,
            plan_json: &Value,
            verify_json: &Value,
            expected_verify_session_dir: Option<&Path>,
        ) -> PathGuard {
            let bin_dir = self.path("bin");
            fs::create_dir_all(&bin_dir).expect("create fake bin dir");

            let plan_path = self.path("plan.json");
            let verify_path = self.path("verify.json");
            fs::write(
                &plan_path,
                serde_json::to_string(plan_json).expect("serialize plan json"),
            )
            .expect("write plan json");
            fs::write(
                &verify_path,
                serde_json::to_string(verify_json).expect("serialize verify json"),
            )
            .expect("write verify json");

            let expected_session = expected_verify_session_dir
                .map(|path| path.display().to_string())
                .unwrap_or_default();
            let script_path = bin_dir.join(TOP_WRAPPER_BIN);
            let script = format!(
                r#"#!/bin/sh
set -eu
mode=""
prev=""
session=""
for arg in "$@"; do
  if [ "$arg" = "--plan-latest-clerestory-certificate" ]; then
    mode="plan"
  fi
  if [ "$arg" = "--verify-latest-clerestory-certificate" ]; then
    mode="verify"
  fi
  if [ "$prev" = "--session-dir" ]; then
    session="$arg"
  fi
  prev="$arg"
done
if [ "$mode" = "plan" ]; then
  cat {plan_path}
  exit 0
fi
if [ "$mode" = "verify" ]; then
  if [ -n {expected_session} ] && [ "$session" != {expected_session} ]; then
    echo "unexpected session-dir: $session" >&2
    exit 17
  fi
  cat {verify_path}
  exit 0
fi
echo "unknown fake top wrapper mode" >&2
exit 18
"#,
                plan_path = shell_quote(&plan_path.display().to_string()),
                verify_path = shell_quote(&verify_path.display().to_string()),
                expected_session = shell_quote(&expected_session),
            );
            fs::write(&script_path, script).expect("write fake top wrapper");
            let mut permissions = fs::metadata(&script_path)
                .expect("fake top wrapper metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).expect("chmod fake top wrapper");

            PathGuard::prepend(bin_dir)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    struct PathGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
        previous_path: Option<std::ffi::OsString>,
    }

    impl PathGuard {
        fn prepend(bin_dir: PathBuf) -> Self {
            let guard = PATH_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .expect("lock PATH guard");
            let previous_path = env::var_os("PATH");
            let mut paths = vec![bin_dir];
            if let Some(previous) = previous_path.as_ref() {
                paths.extend(env::split_paths(previous));
            }
            let new_path = env::join_paths(paths).expect("join PATH");
            env::set_var("PATH", new_path);
            Self {
                _guard: guard,
                previous_path,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            if let Some(previous_path) = self.previous_path.take() {
                env::set_var("PATH", previous_path);
            } else {
                env::remove_var("PATH");
            }
        }
    }

    fn shell_quote(raw: &str) -> String {
        format!("'{}'", raw.replace('\'', "'\\''"))
    }
}
