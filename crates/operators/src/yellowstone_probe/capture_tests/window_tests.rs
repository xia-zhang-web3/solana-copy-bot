use super::*;

fn window_args() -> Vec<String> {
    let mut a = args(Path::new("fresh-window"));
    a.extend(["--config".into(), "unused.toml".into()]);
    a.extend(["--capture-profile".into(), "window-v1".into()]);
    a
}

#[test]
fn window_profile_zero_n_and_n_plus_one_limits_are_explicit() {
    let mut a = window_args();
    for (flag, n) in [
        ("--duration-ms", 60_000),
        ("--max-messages", 4096),
        ("--max-message-bytes", 8_388_608),
        ("--max-total-bytes", 67_108_864),
    ] {
        replace(&mut a, flag, n);
    }
    let c = parse_args_from(a.clone()).unwrap().capture.unwrap();
    assert_eq!(c.messages, 4096);
    assert_eq!(c.metadata_reserve(), 2_097_152);
    assert_eq!(c.transport_bytes(), 8_388_608);
    for (flag, n) in [
        ("--duration-ms", 60_001),
        ("--max-messages", 4097),
        ("--max-message-bytes", 8_388_609),
        ("--max-total-bytes", 67_108_865),
    ] {
        for value in [0, n] {
            let mut invalid = a.clone();
            replace(&mut invalid, flag, value);
            assert!(parse_args_from(invalid).is_err(), "{flag}={value}");
        }
    }
    replace(&mut a, "--max-total-bytes", 2_097_152);
    assert!(parse_args_from(a.clone()).is_err());
    replace(&mut a, "--max-total-bytes", 2_097_153);
    assert!(parse_args_from(a).is_ok());
}

#[test]
fn window_profile_does_not_widen_either_old_profile() {
    for profile in [None, Some("diagnostic-v1")] {
        let mut a = args(Path::new("old-profile"));
        a.extend(["--config".into(), "unused.toml".into()]);
        if let Some(name) = profile {
            a.extend(["--capture-profile".into(), name.into()]);
        }
        let c = parse_args_from(a.clone()).unwrap().capture.unwrap();
        assert_eq!(c.messages, 256);
        assert_eq!(c.metadata_reserve(), 262_144);
        replace(&mut a, "--max-messages", 257);
        assert!(parse_args_from(a).is_err());
    }
    let mut a = window_args();
    replace(&mut a, "--capture-profile", "window-v2");
    assert!(parse_args_from(a).is_err());
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh capture directory"]
async fn actual_cli_window_accepts_above_256_without_widening_old_profiles() {
    for (name, profile, limit, expected, reason) in [
        (
            "window-above-256",
            Some("window-v1"),
            4096,
            257,
            "stream_closed",
        ),
        (
            "window-old-diagnostic",
            Some("diagnostic-v1"),
            256,
            256,
            "message_count_limit",
        ),
        ("window-old-legacy", None, 256, 256, "message_count_limit"),
    ] {
        let result = cli_case_profile(
            name,
            vec![transaction(); 257],
            "window-fast",
            &[("--max-messages", limit), ("--duration-ms", 3000)],
            profile,
        )
        .await;
        let m = &result["capture"]["manifest"];
        assert_eq!(m["stop_reason"], reason);
        assert_eq!(m["messages_received"], expected);
        assert_eq!(m["envelopes_written"], expected);
        assert_eq!(
            m["metadata_reserve_bytes"],
            if limit == 4096 { 2_097_152 } else { 262_144 }
        );
        assert_eq!(
            m["messages"][0]["sha256"],
            sha256(&transaction().encode_to_vec())
        );
    }
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh capture directory"]
async fn actual_cli_window_count_budget_retains_4096_metadata_rows() {
    let result = cli_case_profile(
        "window-4096",
        vec![ping(); 4097],
        "window-fast",
        &[("--max-messages", 4096), ("--duration-ms", 3000)],
        Some("window-v1"),
    )
    .await;
    let m = &result["capture"]["manifest"];
    assert_eq!(m["stop_reason"], "message_count_limit");
    assert_eq!(m["messages_received"], 4096);
    assert_eq!(m["messages"].as_array().unwrap().len(), 4096);
    assert_eq!(m["envelopes_written"], 0);
    assert_eq!(m["complete"], false);
    let size = std::fs::metadata(root().join("window-4096/manifest.json"))
        .unwrap()
        .len();
    assert!(size > 262_144 && size <= 2_097_152);
    assert!(size <= m["limits"]["total_bytes"].as_u64().unwrap());
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh capture directory"]
async fn actual_cli_window_reserve_n_and_n_plus_one_cutoffs_are_unchanged() {
    let size = transaction().encoded_len() as u64;
    for (name, total, accepted) in [
        ("window-reserve-n", 2_097_152 + size, true),
        ("window-reserve-n-plus-one", 2_097_152 + size - 1, false),
    ] {
        let result = cli_case_profile(
            name,
            vec![transaction()],
            "close",
            &[("--max-total-bytes", total)],
            Some("window-v1"),
        )
        .await;
        let m = &result["capture"]["manifest"];
        assert_eq!(m["metadata_reserve_bytes"], 2_097_152);
        assert_eq!(
            m["stop_reason"],
            if accepted {
                "stream_closed"
            } else {
                "total_output_limit"
            }
        );
        assert_eq!(m["payload_bytes"], if accepted { size } else { 0 });
        assert!(m["payload_bytes"].as_u64().unwrap() + 2_097_152 <= total);
    }
}
