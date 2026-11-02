//! Default upstream collector boundaries; full saved envelopes are tested separately.
use crate::source::http_recovery::complete_anchor::collector_logs_agree as agrees;

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).into()).collect()
}

#[test]
fn collector_budget_counts_utf8_bytes_and_uses_strict_limit() {
    let prefix = "é".repeat(4_999); // 9,998 bytes, not 4,999.
    let http = vec![
        prefix.clone(),
        "x".into(),
        "y".into(),
        "z".into(),
        "".into(),
    ];
    let expected = vec![
        prefix.clone(),
        "x".into(),
        "Log truncated".into(),
        "".into(),
    ];
    assert!(agrees(&http, &expected));
    assert!(!agrees(&http, &http));
    assert!(!agrees(&http, &[prefix, "Log truncated".into()]));
    for size in [10_000, 10_001] {
        assert!(agrees(&["a".repeat(size)], &strings(&["Log truncated"])));
    }
    assert!(!agrees(&["a".repeat(9_999)], &strings(&["Log truncated"])));
}

#[test]
fn collector_budget_skips_multiple_noncontiguous_records_without_spending_bytes() {
    let prefix = "a".repeat(9_980);
    let http = vec![
        prefix.clone(),
        "b".repeat(20),
        "kept".into(),
        "c".repeat(16),
        "again".into(),
        "d".repeat(11),
        "last".into(),
    ];
    let expected = vec![
        prefix,
        "Log truncated".into(),
        "kept".into(),
        "again".into(),
        "last".into(),
    ];
    assert!(agrees(&http, &expected));
    for mode in [
        "missing",
        "extra",
        "reordered",
        "changed",
        "second_marker",
        "non_bare_marker",
    ] {
        let mut live = expected.clone();
        match mode {
            "missing" => {
                live.remove(3);
            }
            "extra" => live.push("extra".into()),
            "reordered" => live.swap(2, 3),
            "changed" => live[2].push('x'),
            "second_marker" => live.push("Log truncated".into()),
            "non_bare_marker" => live[1].push(' '),
            _ => unreachable!(),
        }
        assert!(!agrees(&http, &live), "{mode}");
    }
    assert!(!agrees(
        &strings(&["Log truncated"]),
        &strings(&["Log truncated"])
    ));
    assert!(!agrees(
        &strings(&["Log truncated extra"]),
        &strings(&["Log truncated"])
    ));
    assert!(!agrees(&[], &[]));
}
