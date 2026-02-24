use chrono::{DateTime, Utc};

pub(crate) fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::parse_rfc3339_utc;

    #[test]
    fn parse_rfc3339_utc_parses_valid_timestamp() {
        let parsed = parse_rfc3339_utc("2026-02-24T00:00:00Z");
        assert!(parsed.is_some(), "valid RFC3339 timestamp should parse");
    }

    #[test]
    fn parse_rfc3339_utc_rejects_invalid_timestamp() {
        let parsed = parse_rfc3339_utc("not-a-timestamp");
        assert!(parsed.is_none(), "invalid timestamp must fail parsing");
    }
}
