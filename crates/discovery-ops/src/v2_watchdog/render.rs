use super::types::WatchdogOutput;

pub(super) fn render_human(output: &WatchdogOutput) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "discovery_v2_watchdog state={} active_follow_wallets={} published_wallets={} publication_age_seconds={:?}",
        output.state.as_str(),
        output.active_follow_wallet_count,
        output.published_wallet_count,
        output.publication_age_seconds
    ));
    lines.push(format!("config={}", output.config_path));
    lines.push(format!("db={}", output.db_path));
    lines.push(format!(
        "publication_mode={:?} reason={:?} last_published_at={:?}",
        output.publication_runtime_mode,
        output.publication_reason,
        output.publication_last_published_at
    ));
    lines.push(format!(
        "fresh={} identity_matches={} cursor_fresh={} warn_age_seconds={} max_age_seconds={}",
        output.publication_fresh,
        output.publication_identity_matches,
        output.publication_cursor_fresh,
        output.publication_warn_age_seconds,
        output.publication_max_age_seconds
    ));
    if output.findings.is_empty() {
        lines.push("findings=[]".to_string());
    } else {
        lines.push("findings:".to_string());
        for finding in &output.findings {
            lines.push(format!(
                "- severity={} code={} detail={}",
                finding.severity.as_str(),
                finding.code,
                finding.detail
            ));
        }
    }
    lines.join("\n")
}
