use super::{Mode, WrapperReport};

pub(super) fn render_human_report(report: &WrapperReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!(
            "verdict={}",
            serde_json::to_string(&report.verdict)
                .unwrap()
                .trim_matches('"')
        ),
        format!("reason={}", report.reason),
    ];
    if let Some(path) = &report.output_path {
        lines.push(format!("output_path={path}"));
    }
    if let Some(path) = &report.wrapper_path {
        lines.push(format!("wrapper_path={path}"));
    }
    lines.push(format!("wrapper_version={}", report.wrapper_version));
    if let Some(backend_command) = &report.backend_command {
        lines.push(format!("backend_command={backend_command}"));
    }
    if let Some(timeout_ms) = report.timeout_ms {
        lines.push(format!("timeout_ms={timeout_ms}"));
    }
    lines.push(format!(
        "supported_actions={}",
        report.supported_actions.join(",")
    ));
    lines.push(format!(
        "status_schema_version={}",
        report.status_schema_version
    ));
    if let Some(executable) = report.executable {
        lines.push(format!("executable={executable}"));
    }
    if let Some(exact) = report.exact_content_matches_expected {
        lines.push(format!("exact_content_matches_expected={exact}"));
    }
    if !report.mismatches.is_empty() {
        lines.push(format!("mismatches={}", report.mismatches.join(" | ")));
    }
    lines.push(format!("note={}", report.explicit_statement));
    lines.join("\n")
}

pub(super) fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::RenderWrapper => "render_wrapper",
        Mode::InstallWrapper => "install_wrapper",
        Mode::VerifyWrapper => "verify_wrapper",
    }
}
