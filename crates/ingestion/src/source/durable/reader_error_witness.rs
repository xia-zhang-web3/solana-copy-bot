//! Bounded safe evidence; incomplete text is explicit and never a transport verdict.
use serde::Serialize;
use std::{
    error::Error,
    fmt::{self, Write},
};
const SCAN_BYTES: usize = 8192;
const CAUSE_SCAN: usize = 16;
const CAUSES: usize = 16;
const SAFE_TEXT_JSON_BUDGET: usize = 56 * 1024;
#[derive(Debug, Clone, Serialize)]
pub struct SafeText {
    pub text: String,
    pub original_bytes: Option<usize>,
    pub original_chars: Option<usize>,
    pub retained_bytes: usize,
    pub retained_chars: usize,
    pub redacted: bool,
    pub truncated: bool,
    pub necessary_detail: &'static str,
}
#[derive(Debug, Clone, Serialize)]
pub struct ErrorWitness {
    pub code: Option<String>,
    pub message: SafeText,
    pub causes: Vec<SafeText>,
    pub causes_total: Option<usize>,
    pub causes_observed_minimum: usize,
    pub causes_retained: usize,
    pub causes_dropped: Option<usize>,
    pub cause_count_availability: &'static str,
    pub opaque_details_bytes: Option<usize>,
    pub opaque_details: &'static str,
    pub typed_h2_reason: &'static str,
    pub transport_origin: &'static str,
}
struct BoundedDisplay {
    text: String,
    full: bool,
}
impl Write for BoundedDisplay {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let left = SCAN_BYTES.saturating_sub(self.text.len());
        let mut n = left.min(s.len());
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        self.text.push_str(&s[..n]);
        if n < s.len() {
            self.full = false;
            return Err(fmt::Error);
        }
        Ok(())
    }
}
fn display(error: &(dyn Error + 'static), secrets: &[String], limit: usize) -> SafeText {
    let mut value = BoundedDisplay {
        text: String::new(),
        full: true,
    };
    let result = write!(&mut value, "{error}");
    let mut out = safe(&value.text, secrets, limit);
    if result.is_err() || !value.full {
        out.original_bytes = None;
        out.original_chars = None;
        out.truncated = true;
        out.necessary_detail = "UNAVAILABLE_BOUNDED_DISPLAY";
    }
    out
}
pub(super) fn safe(value: &str, secrets: &[String], limit: usize) -> SafeText {
    let mut n = SCAN_BYTES.min(value.len());
    while !value.is_char_boundary(n) {
        n -= 1;
    }
    let scanned_all = n == value.len();
    let mut clean = value[..n].to_owned();
    let mut redacted = false;
    for secret in secrets.iter().filter(|s| !s.is_empty()) {
        if clean.contains(secret) {
            redacted = true;
            clean = clean.replace(secret, "[redacted]");
        }
    }
    let mut normalized = String::new();
    for line in clean.lines() {
        let line = line
            .split_whitespace()
            .map(|word| {
                if word.contains("://") || word.len() > 64 {
                    redacted = true;
                    "[redacted]"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        let lower = line.to_ascii_lowercase();
        if [
            "authorization",
            "x-token",
            "x_token",
            "bearer",
            "api-key",
            "api_key",
            "headers",
            "metadata",
            "cookie",
            "password",
            "secret",
            "token=",
            "token:",
        ]
        .iter()
        .any(|s| lower.contains(s))
        {
            redacted = true;
            normalized.push_str("[redacted sensitive detail] ");
        } else {
            normalized.extend(line.chars().filter(|c| !c.is_control()));
            normalized.push(' ');
        }
    }
    let before = normalized.trim();
    let truncated = !scanned_all || before.chars().count() > limit;
    let text = before.chars().take(limit).collect::<String>();
    SafeText {
        original_bytes: Some(value.len()),
        original_chars: scanned_all.then(|| value.chars().count()),
        retained_bytes: text.len(),
        retained_chars: text.chars().count(),
        text,
        redacted,
        truncated,
        necessary_detail: if redacted || truncated {
            "UNAVAILABLE_REDACTED_OR_TRUNCATED"
        } else {
            "AVAILABLE_AS_EXPOSED"
        },
    }
}
// Serialized text budget leaves8KiB for16cause metadata and event envelope.
fn budgeted(mut value: SafeText, remaining: &mut usize) -> SafeText {
    let input = std::mem::take(&mut value.text);
    let mut charged = 2usize;
    for ch in input.chars() {
        let cost = serde_json::to_string(&ch.to_string()).map_or(6, |s| s.len().saturating_sub(2));
        if charged.saturating_add(cost) > *remaining {
            break;
        }
        value.text.push(ch);
        charged = charged.saturating_add(cost);
    }
    if value.text.len() != input.len() {
        value.truncated = true;
        value.necessary_detail = "UNAVAILABLE_TOTAL_DIAGNOSTIC_CAP";
    }
    *remaining = remaining.saturating_sub(charged);
    value.retained_bytes = value.text.len();
    value.retained_chars = value.text.chars().count();
    value
}

impl ErrorWitness {
    fn collect(
        code: Option<String>,
        message: SafeText,
        mut cause: Option<&(dyn Error + 'static)>,
        secrets: &[String],
        details: Option<usize>,
    ) -> Self {
        let mut remaining = SAFE_TEXT_JSON_BUDGET;
        let message = budgeted(message, &mut remaining);
        let mut causes = Vec::new();
        let mut count = 0;
        let mut seen: Vec<&(dyn Error + 'static)> = Vec::new();
        let mut cycle = false;
        while let Some(error) = cause {
            if seen.iter().any(|prior| std::ptr::eq(*prior, error)) {
                cycle = true;
                break;
            }
            if count == CAUSE_SCAN {
                break;
            }
            seen.push(error);
            if count < CAUSES {
                causes.push(budgeted(display(error, secrets, 8192), &mut remaining));
            }
            count += 1;
            cause = error.source();
        }
        let total = cause.is_none().then_some(count);
        Self {
            code,
            message,
            causes_retained: causes.len(),
            causes,
            causes_total: total,
            causes_observed_minimum: count,
            causes_dropped: total.map(|n| n.saturating_sub(CAUSES)),
            cause_count_availability: if total.is_some() {
                "EXACT_AS_EXPOSED"
            } else {
                if cycle {
                    "UNAVAILABLE_CYCLE"
                } else {
                    "UNAVAILABLE_SCAN_BOUND"
                }
            },
            opaque_details_bytes: details,
            opaque_details: if details.is_some_and(|n| n > 0) {
                "UNAVAILABLE_OPAQUE_NOT_EXPORTED"
            } else {
                "ABSENT_OR_NOT_APPLICABLE"
            },
            typed_h2_reason: "UNAVAILABLE_NOT_EXPOSED_BY_THIS_API",
            transport_origin: "UNAVAILABLE_STATUS_DOES_NOT_IDENTIFY_PHYSICAL_ORIGIN",
        }
    }
    pub(super) fn status(status: &tonic::Status, secrets: &[String]) -> Self {
        Self::collect(
            Some(format!("{:?}", status.code())),
            safe(status.message(), secrets, 8192),
            status.source(),
            secrets,
            Some(status.details().len()),
        )
    }
    pub(super) fn error(error: &anyhow::Error, secrets: &[String]) -> Self {
        Self::collect(
            None,
            display(error.as_ref(), secrets, 8192),
            error.source(),
            secrets,
            None,
        )
    }
}
