pub(crate) fn simulation_error_class(value: Option<&str>) -> String {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return "missing".to_string();
    };
    let lower = raw.to_ascii_lowercase();
    if lower.contains("no_routes_found")
        || lower.contains("no routes found")
        || lower.contains("terminal_failed_sell_no_route")
        || lower.contains("terminal_sell_no_route")
    {
        return "terminal_no_route".to_string();
    }
    if lower.contains("bonding curve for mint not found") {
        return "pump_fun_bonding_curve_not_found".to_string();
    }
    if lower.contains("market not found")
        || (lower.contains("market ") && lower.contains(" not found"))
    {
        return "market_not_found".to_string();
    }
    if let Some(hex) = custom_program_error_code(&lower) {
        return format!("custom_program_error:{hex}");
    }
    if lower.contains("insufficient funds") {
        return "insufficient_funds".to_string();
    }
    if lower.contains("account required") && lower.contains("missing")
        || lower.contains("missing account")
        || lower.contains("not enough account")
    {
        return "missing_account".to_string();
    }
    if lower.contains("account not found") || lower.contains("could not find account") {
        return "account_not_found".to_string();
    }
    if lower.contains("blockhash") {
        return "blockhash".to_string();
    }
    if lower.contains("slippage") {
        return "slippage".to_string();
    }
    "other".to_string()
}

fn custom_program_error_code(lower: &str) -> Option<String> {
    let marker = "custom program error:";
    let (_, tail) = lower.split_once(marker)?;
    let code = tail
        .trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';' || ch == '}')
        .next()?
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '.' || ch == ':');
    code.strip_prefix("0x")
        .filter(|hex| !hex.is_empty() && hex.chars().all(|ch| ch.is_ascii_hexdigit()))
        .map(|hex| format!("0x{hex}"))
}
