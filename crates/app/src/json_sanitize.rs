pub(crate) fn sanitize_json_value(value: &str) -> String {
    let json_string = serde_json::Value::String(value.to_string()).to_string();
    json_string[1..json_string.len().saturating_sub(1)].to_string()
}
