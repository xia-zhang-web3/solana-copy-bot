use super::*;
use std::ffi::OsString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());
static TEMP_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn ingestion_yellowstone_defaults_are_applied() {
    let ingestion = IngestionConfig::default();
    assert_eq!(ingestion.yellowstone_grpc_url, "REPLACE_ME");
    assert_eq!(ingestion.yellowstone_x_token, "REPLACE_ME");
    assert_eq!(ingestion.yellowstone_connect_timeout_ms, 5_000);
    assert_eq!(ingestion.yellowstone_subscribe_timeout_ms, 15_000);
    assert_eq!(ingestion.yellowstone_stream_buffer_capacity, 2_048);
    assert_eq!(ingestion.yellowstone_reconnect_initial_ms, 500);
    assert_eq!(ingestion.yellowstone_reconnect_max_ms, 8_000);
    assert!(ingestion.yellowstone_program_ids.is_empty());
}

#[test]
fn shadow_defaults_use_conservative_min_holders_floor() {
    let shadow = ShadowConfig::default();
    assert_eq!(shadow.min_holders, 5);
}

#[test]
fn discovery_defaults_use_storage_mitigation_limits() {
    let discovery = DiscoveryConfig::default();
    assert_eq!(discovery.fetch_refresh_seconds, 60);
    assert_eq!(discovery.refresh_seconds, 600);
    assert_eq!(discovery.metric_snapshot_interval_seconds, 1_800);
    assert_eq!(discovery.max_bootstrap_snapshot_age_seconds, 43_200);
    assert_eq!(discovery.max_window_swaps_in_memory, 60_000);
    assert_eq!(discovery.max_fetch_swaps_per_cycle, 20_000);
    assert_eq!(discovery.max_fetch_pages_per_cycle, 5);
    assert_eq!(discovery.fetch_time_budget_ms, 15_000);
    assert_eq!(discovery.observed_swaps_retention_days, 7);
    assert!(!discovery.scoring_aggregates_write_enabled);
    assert!(!discovery.scoring_aggregates_enabled);
}

#[test]
fn recent_raw_journal_defaults_are_explicit_and_bounded() {
    let journal = RecentRawJournalConfig::default();
    assert_eq!(journal.path, "state/discovery_recent_raw.db");
    assert_eq!(journal.retention_safety_buffer_days, 2);
    assert_eq!(journal.writer_queue_capacity_batches, 64);
    assert_eq!(journal.replay_batch_size, 1_024);
}

#[test]
fn recent_raw_gap_fill_defaults_are_explicit_and_bounded() {
    let gap_fill = RecentRawGapFillConfig::default();
    assert_eq!(gap_fill.source, "helius_rpc");
    assert_eq!(gap_fill.helius_http_url, "");
    assert_eq!(gap_fill.output_dir, "state/discovery_restore/gap_fill");
    assert_eq!(gap_fill.output_retention, 16);
    assert_eq!(gap_fill.request_timeout_ms, 20_000);
    assert_eq!(gap_fill.signature_page_size, 1_000);
    assert_eq!(gap_fill.max_signature_pages_per_wallet, 64);
}

#[test]
fn recent_raw_gap_fill_helius_defaults_are_explicit_and_bounded() {
    let gap_fill = RecentRawGapFillHeliusConfig::default();
    assert_eq!(gap_fill.helius_http_url, "");
    assert_eq!(
        gap_fill.output_dir,
        "state/discovery_restore/gap_fill_helius"
    );
    assert_eq!(gap_fill.output_retention, 16);
    assert_eq!(gap_fill.request_timeout_ms, 20_000);
    assert_eq!(gap_fill.page_size, 100);
    assert_eq!(gap_fill.max_pages_per_wallet, 64);
}

#[test]
fn program_history_validation_defaults_are_quicknode_first_and_bounded() {
    let validation = ProgramHistoryValidationConfig::default();
    assert_eq!(validation.source, "quicknode_blocks_rpc");
    assert_eq!(
        validation.http_url,
        "https://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro/REPLACE_ON_SERVER/"
    );
    assert_eq!(validation.request_timeout_ms, 20_000);
    assert_eq!(validation.max_requests_per_second, 100);
    assert_eq!(validation.retry_429_max_attempts, 4);
    assert_eq!(validation.retry_429_backoff_ms, 250);
    assert_eq!(validation.block_batch_size, 512);
    assert_eq!(validation.phase_a_max_slots_to_scan, 4_096);
    assert_eq!(validation.phase_a_sampling_segments, 8);
    assert_eq!(validation.phase_a_max_blocks_per_window, 12);
    assert_eq!(validation.max_slots_to_scan, 20_000);
    assert_eq!(validation.sampling_segments, 8);
    assert_eq!(validation.block_time_probe_slots, 128);
    assert_eq!(validation.raydium_program_ids.len(), 2);
    assert_eq!(validation.pumpswap_program_ids.len(), 1);
}

#[test]
fn runtime_restore_ops_defaults_are_explicit_and_operational() {
    let ops = RuntimeRestoreOpsConfig::default();
    assert_eq!(ops.artifact_dir, "state/discovery_restore/artifacts");
    assert_eq!(ops.artifact_retention, 288);
    assert_eq!(ops.artifact_cadence_minutes, 10);
    assert_eq!(
        ops.journal_snapshot_dir,
        "state/discovery_restore/recent_raw"
    );
    assert_eq!(ops.journal_snapshot_retention, 144);
    assert_eq!(ops.journal_snapshot_cadence_minutes, 10);
    assert_eq!(ops.drill_workspace_dir, "state/discovery_restore/drills");
}

#[test]
fn history_retention_defaults_are_explicit_and_safe() {
    let retention = HistoryRetentionConfig::default();
    assert!(!retention.enabled);
    assert_eq!(retention.sweep_seconds, 3_600);
    assert_eq!(retention.protected_history_days, 30);
    assert_eq!(retention.risk_events_days, 30);
    assert_eq!(retention.copy_signals_days, 30);
    assert_eq!(retention.orders_days, 30);
    assert_eq!(retention.fills_days, 30);
    assert_eq!(retention.shadow_closed_trades_days, 90);
}

#[test]
fn parse_from_path_uses_disabled_history_retention_for_legacy_config_without_block() {
    with_temp_config_file("", |config_path| {
        let config = load_from_path(config_path).expect("legacy config without block must parse");
        assert!(!config.history_retention.enabled);
    });
}

#[test]
fn parse_from_path_reads_runtime_restore_ops_block() {
    with_temp_config_file(
        r#"
[recent_raw_gap_fill]
helius_http_url = "https://gap-fill.helius.example/?api-key=test"
output_dir = "restore/gap-fill"
output_retention = 8
request_timeout_ms = 15000
signature_page_size = 500
max_signature_pages_per_wallet = 12

[recent_raw_gap_fill_helius]
helius_http_url = "https://gap-fill-helius.example/?api-key=test"
output_dir = "restore/gap-fill-helius"
output_retention = 6
request_timeout_ms = 12000
page_size = 80
max_pages_per_wallet = 10

[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
request_timeout_ms = 11000
max_requests_per_second = 90
retry_429_max_attempts = 6
retry_429_backoff_ms = 400
block_batch_size = 400
phase_a_max_slots_to_scan = 2048
phase_a_sampling_segments = 4
phase_a_max_blocks_per_window = 10
max_slots_to_scan = 12000
sampling_segments = 6
block_time_probe_slots = 64
raydium_program_ids = ["raydium-a", "raydium-b"]
pumpswap_program_ids = ["pump-a"]

[runtime_restore_ops]
artifact_dir = "restore/artifacts"
artifact_retention = 32
artifact_cadence_minutes = 5
journal_snapshot_dir = "restore/journal"
journal_snapshot_retention = 24
journal_snapshot_cadence_minutes = 10
drill_workspace_dir = "restore/drills"

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let config = load_from_path(config_path).expect("config must parse");
            assert_eq!(
                config.recent_raw_gap_fill.helius_http_url,
                "https://gap-fill.helius.example/?api-key=test"
            );
            assert_eq!(config.recent_raw_gap_fill.output_dir, "restore/gap-fill");
            assert_eq!(config.recent_raw_gap_fill.output_retention, 8);
            assert_eq!(config.recent_raw_gap_fill.request_timeout_ms, 15_000);
            assert_eq!(config.recent_raw_gap_fill.signature_page_size, 500);
            assert_eq!(
                config.recent_raw_gap_fill.max_signature_pages_per_wallet,
                12
            );
            assert_eq!(
                config.recent_raw_gap_fill_helius.helius_http_url,
                "https://gap-fill-helius.example/?api-key=test"
            );
            assert_eq!(
                config.recent_raw_gap_fill_helius.output_dir,
                "restore/gap-fill-helius"
            );
            assert_eq!(config.recent_raw_gap_fill_helius.output_retention, 6);
            assert_eq!(config.recent_raw_gap_fill_helius.request_timeout_ms, 12_000);
            assert_eq!(config.recent_raw_gap_fill_helius.page_size, 80);
            assert_eq!(config.recent_raw_gap_fill_helius.max_pages_per_wallet, 10);
            assert_eq!(
                config.program_history_validation.http_url,
                "https://quicknode.example/?api-key=test"
            );
            assert_eq!(config.program_history_validation.request_timeout_ms, 11_000);
            assert_eq!(
                config.program_history_validation.max_requests_per_second,
                90
            );
            assert_eq!(config.program_history_validation.retry_429_max_attempts, 6);
            assert_eq!(config.program_history_validation.retry_429_backoff_ms, 400);
            assert_eq!(config.program_history_validation.block_batch_size, 400);
            assert_eq!(
                config.program_history_validation.phase_a_max_slots_to_scan,
                2_048
            );
            assert_eq!(
                config.program_history_validation.phase_a_sampling_segments,
                4
            );
            assert_eq!(
                config
                    .program_history_validation
                    .phase_a_max_blocks_per_window,
                10
            );
            assert_eq!(config.program_history_validation.max_slots_to_scan, 12_000);
            assert_eq!(config.program_history_validation.sampling_segments, 6);
            assert_eq!(config.program_history_validation.block_time_probe_slots, 64);
            assert_eq!(
                config.program_history_validation.raydium_program_ids,
                vec!["raydium-a".to_string(), "raydium-b".to_string()]
            );
            assert_eq!(
                config.program_history_validation.pumpswap_program_ids,
                vec!["pump-a".to_string()]
            );
            assert_eq!(config.runtime_restore_ops.artifact_dir, "restore/artifacts");
            assert_eq!(config.runtime_restore_ops.artifact_retention, 32);
            assert_eq!(config.runtime_restore_ops.artifact_cadence_minutes, 5);
            assert_eq!(
                config.runtime_restore_ops.journal_snapshot_dir,
                "restore/journal"
            );
            assert_eq!(config.runtime_restore_ops.journal_snapshot_retention, 24);
            assert_eq!(
                config.runtime_restore_ops.journal_snapshot_cadence_minutes,
                10
            );
            assert_eq!(
                config.runtime_restore_ops.drill_workspace_dir,
                "restore/drills"
            );
        },
    );
}

#[test]
fn live_server_template_exposes_recent_raw_gap_fill_contract() {
    let template_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ops/server_templates/live.server.toml.example");
    let config = load_from_path(&template_path).expect("live server template must parse");
    assert_eq!(config.recent_raw_gap_fill.source, "helius_rpc");
    assert!(
        !config.recent_raw_gap_fill.helius_http_url.trim().is_empty(),
        "template must expose a non-empty recent_raw_gap_fill.helius_http_url placeholder"
    );
    assert_eq!(
        config.recent_raw_gap_fill.output_dir,
        "state/discovery_restore/gap_fill"
    );
    assert!(
        !config
            .recent_raw_gap_fill_helius
            .helius_http_url
            .trim()
            .is_empty(),
        "template must expose a non-empty recent_raw_gap_fill_helius.helius_http_url placeholder"
    );
    assert_eq!(
        config.recent_raw_gap_fill_helius.output_dir,
        "state/discovery_restore/gap_fill_helius"
    );
    assert_eq!(
        config.program_history_validation.source,
        "quicknode_blocks_rpc"
    );
    assert!(
        !config.program_history_validation.http_url.trim().is_empty(),
        "template must expose a non-empty program_history_validation.http_url placeholder"
    );
    assert_eq!(
        config.program_history_validation.max_requests_per_second,
        100
    );
    assert_eq!(config.program_history_validation.retry_429_max_attempts, 4);
    assert_eq!(config.program_history_validation.retry_429_backoff_ms, 250);
    assert_eq!(
        config.program_history_validation.phase_a_max_slots_to_scan,
        4_096
    );
    assert_eq!(
        config.program_history_validation.phase_a_sampling_segments,
        8
    );
    assert_eq!(
        config
            .program_history_validation
            .phase_a_max_blocks_per_window,
        12
    );
    assert_eq!(config.program_history_validation.sampling_segments, 8);
}

#[test]
fn load_from_path_rejects_invalid_recent_raw_gap_fill_bounds() {
    with_temp_config_file(
        r#"
[recent_raw_gap_fill]
signature_page_size = 1001

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("oversized signature page must fail")
                .to_string();
            assert!(
                err.contains(
                    "recent_raw_gap_fill.signature_page_size (1001) must be between 1 and 1000"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_invalid_recent_raw_gap_fill_helius_bounds() {
    with_temp_config_file(
        r#"
[recent_raw_gap_fill_helius]
page_size = 101

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("oversized helius page size must fail")
                .to_string();
            assert!(
                err.contains(
                    "recent_raw_gap_fill_helius.page_size (101) must be between 1 and 100"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_invalid_program_history_validation_bounds() {
    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
block_batch_size = 1006

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("oversized block batch size must fail")
                .to_string();
            assert!(
                err.contains(
                    "program_history_validation.block_batch_size (1006) must be between 1 and 1005"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_program_history_validation_rate_limit_above_quicknode_contract() {
    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
max_requests_per_second = 126

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("max requests per second above 125 must fail")
                .to_string();
            assert!(
                err.contains(
                    "program_history_validation.max_requests_per_second (126) must be between 1 and 125"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_program_history_validation_zero_retry_knobs() {
    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
retry_429_max_attempts = 0

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("zero retry attempts must fail")
                .to_string();
            assert!(
                err.contains("program_history_validation.retry_429_max_attempts (0) must be >= 1"),
                "unexpected error: {err}"
            );
        },
    );

    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
retry_429_backoff_ms = 0

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("zero retry backoff must fail")
                .to_string();
            assert!(
                err.contains("program_history_validation.retry_429_backoff_ms (0) must be >= 1"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_program_history_validation_sampling_segments_above_budget() {
    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
max_slots_to_scan = 4
sampling_segments = 8

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("sampling segments above budget must fail")
                .to_string();
            assert!(
                err.contains(
                    "program_history_validation.sampling_segments (8) must be <= program_history_validation.max_slots_to_scan (4)"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_program_history_validation_invalid_phase_a_bounds() {
    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
phase_a_sampling_segments = 9
phase_a_max_slots_to_scan = 8

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("phase_a sampling segments above budget must fail")
                .to_string();
            assert!(
                err.contains(
                    "program_history_validation.phase_a_sampling_segments (9) must be <= program_history_validation.phase_a_max_slots_to_scan (8)"
                ),
                "unexpected error: {err}"
            );
        },
    );

    with_temp_config_file(
        r#"
[program_history_validation]
http_url = "https://quicknode.example/?api-key=test"
phase_a_max_blocks_per_window = 0

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("phase_a max blocks per window must fail when zero")
                .to_string();
            assert!(
                err.contains(
                    "program_history_validation.phase_a_max_blocks_per_window (0) must be >= 1"
                ),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_path_rejects_runtime_restore_ops_with_cadence_slower_than_freshness_gate() {
    with_temp_config_file(
        r#"
[runtime_restore_ops]
artifact_cadence_minutes = 30

[discovery]
metric_snapshot_interval_seconds = 1800
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("artifact cadence at freshness gate boundary must fail")
                .to_string();
            assert!(
                err.contains("runtime_restore_ops.artifact_cadence_minutes (30) must be < freshness gate bucket minutes (30)"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_max_slippage_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
        "rpc:50,RPC:75",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_tip_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
        "rpc:100,RPC:200",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_cu_limit_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT",
        "rpc:300000,RPC:350000",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_route_cu_price_keys() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS",
        "rpc:1000,RPC:2000",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_submit_allowed_routes() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
        "rpc,RPC",
    );
}

#[test]
fn load_from_env_rejects_duplicate_normalized_submit_route_order() {
    assert_duplicate_normalized_route_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER",
        "jito,JITO",
    );
}

#[test]
fn load_from_env_rejects_malformed_route_map_token() {
    assert_route_map_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
        "rpc:50,jito",
        "malformed token",
    );
}

#[test]
fn load_from_env_rejects_invalid_route_map_numeric_value() {
    assert_route_map_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
        "rpc:not-a-number",
        "invalid numeric value",
    );
}

#[test]
fn load_from_env_rejects_empty_submit_allowed_routes_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
        ", ,",
        "must contain at least one route entry",
    );
}

#[test]
fn load_from_env_rejects_empty_submit_route_map_override() {
    assert_route_map_env_rejected_contains(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
        ", ,",
        "must contain at least one route:value entry",
    );
}

#[test]
fn load_from_env_rejects_empty_yellowstone_program_ids_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS",
        "\"\", '' ,",
        "must contain at least one non-empty value",
    );
}

#[test]
fn load_from_env_rejects_duplicate_helius_http_urls_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS",
        "https://rpc.example.com, https://rpc.example.com",
        "duplicate value after normalization",
    );
}

#[test]
fn load_from_env_rejects_duplicate_program_ids_override() {
    assert_string_list_env_rejected_contains(
        "SOLANA_COPY_BOT_PROGRAM_IDS",
        "program-a, program-a",
        "duplicate value after normalization",
    );
}

#[test]
fn load_from_env_rejects_incomplete_route_policy_for_allowed_routes() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_EXECUTION_ENABLED", "true", || {
                with_env_var(
                    "SOLANA_COPY_BOT_EXECUTION_MODE",
                    "adapter_submit_confirm",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
                            "paper,rpc",
                            || {
                                let err = load_from_env_or_default(config_path)
                                    .expect_err(
                                        "missing route policy for allowed route must fail at config load",
                                    )
                                    .to_string();
                                assert!(
                                    err.contains(
                                        "execution.submit_route_max_slippage_bps is missing cap for allowed route=rpc",
                                    ),
                                    "unexpected error: {err}"
                                );
                            },
                        );
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_applies_risk_and_shadow_quality_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL", "0.99", || {
                with_env_var(
                    "SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS",
                    "60",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
                            "9.5",
                            || {
                                with_env_var(
                                    "SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED",
                                    "false",
                                    || {
                                        with_env_var(
                                            "SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS",
                                            "42",
                                            || {
                                                with_env_var(
                                    "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_MICRO_LAMPORTS",
                                    "12345",
                                    || {
                                        with_env_var(
                                            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED",
                                            "true",
                                            || {
                                                with_env_var(
                                                    "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_PERCENTILE",
                                                    "90",
                                                    || {
                                                        with_env_var(
                                                            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_ENABLED",
                                                            "true",
                                                            || {
                                                                with_env_var(
                                                                    "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_MULTIPLIER_BPS",
                                                                    "15000",
                                                                    || {
                                                                        with_env_var(
                                                                            "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_FEE_OVERHEAD_BPS",
                                                                            "750",
                                                                            || {
                                                                                let (cfg, _) = load_from_env_or_default(config_path)
                                                                                    .expect("load config with env overrides");
                                                                                assert!((cfg.risk.max_position_sol - 0.99).abs() <= f64::EPSILON);
                                                                                assert_eq!(cfg.risk.execution_buy_cooldown_seconds, 60);
                                                                                assert!(
                                                                                    (cfg.risk.shadow_soft_exposure_resume_below_sol - 9.5).abs()
                                                                                        <= f64::EPSILON
                                                                                );
                                                                                assert!(!cfg.risk.shadow_killswitch_enabled);
                                                                                assert_eq!(cfg.shadow.min_holders, 42);
                                                                                assert_eq!(
                                                                                    cfg.execution.pretrade_max_priority_fee_lamports,
                                                                                    12_345
                                                                                );
                                                                                assert_eq!(
                                                                                    cfg.execution.pretrade_max_fee_overhead_bps,
                                                                                    750
                                                                                );
                                                                                assert!(cfg.execution.submit_dynamic_cu_price_enabled);
                                                                                assert_eq!(
                                                                                    cfg.execution.submit_dynamic_cu_price_percentile,
                                                                                    90
                                                                                );
                                                                                assert!(
                                                                                    cfg.execution
                                                                                        .submit_dynamic_tip_lamports_enabled
                                                                                );
                                                                                assert_eq!(
                                                                                    cfg.execution
                                                                                        .submit_dynamic_tip_lamports_multiplier_bps,
                                                                                    15_000
                                                                                );
                                                                            },
                                                                        );
                                                                    },
                                                                );
                                                            },
                                                        );
                                                    },
                                                );
                                                    },
                                                );
                                            },
                                        );
                                            },
                                        );
                                    },
                                );
                            },
                        );
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_shadow_min_holders_below_floor_when_enabled() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS", "1", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("shadow min_holders below floor must fail config load")
                    .to_string();
                assert!(
                    err.contains(
                        "shadow.min_holders (1) must be either 0 (disable holder gate) or >= 5"
                    ),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_allows_shadow_min_holders_zero_to_disable_holder_gate() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS", "0", || {
                let (cfg, _) = load_from_env_or_default(config_path)
                    .expect("shadow min_holders=0 should remain allowed");
                assert_eq!(cfg.shadow.min_holders, 0);
            });
        });
    });
}

#[test]
fn load_from_env_allows_discovery_runtime_storage_mitigation_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_FETCH_REFRESH_SECONDS",
                "120",
                || {
                    with_env_var("SOLANA_COPY_BOT_DISCOVERY_REFRESH_SECONDS", "900", || {
                        with_env_var(
                            "SOLANA_COPY_BOT_DISCOVERY_RUG_LOOKAHEAD_SECONDS",
                            "600",
                            || {
                                with_env_var(
                                    "SOLANA_COPY_BOT_DISCOVERY_METRIC_SNAPSHOT_INTERVAL_SECONDS",
                                    "2700",
                                    || {
                                        with_env_var(
                                            "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
                                            "50000",
                                            || {
                                                with_env_var(
                                                "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE",
                                                "15000",
                                                || {
                                                    with_env_var(
                                                        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_PAGES_PER_CYCLE",
                                                        "6",
                                                        || {
                                                            with_env_var(
                                                                "SOLANA_COPY_BOT_DISCOVERY_FETCH_TIME_BUDGET_MS",
                                                                "21000",
                                                                || {
                                                                    with_env_var(
                                                                        "SOLANA_COPY_BOT_DISCOVERY_OBSERVED_SWAPS_RETENTION_DAYS",
                                                                        "60",
                                                                        || {
                                                                            let (cfg, _) = load_from_env_or_default(config_path)
                                                                                .expect("load config with discovery mitigation overrides");
                                                                            assert_eq!(cfg.discovery.fetch_refresh_seconds, 120);
                                                                            assert_eq!(cfg.discovery.refresh_seconds, 900);
                                                                            assert_eq!(cfg.discovery.rug_lookahead_seconds, 600);
                                                                            assert_eq!(cfg.discovery.metric_snapshot_interval_seconds, 2_700);
                                                                            assert_eq!(cfg.discovery.max_window_swaps_in_memory, 50_000);
                                                                            assert_eq!(cfg.discovery.max_fetch_swaps_per_cycle, 15_000);
                                                                            assert_eq!(cfg.discovery.max_fetch_pages_per_cycle, 6);
                                                                            assert_eq!(cfg.discovery.fetch_time_budget_ms, 21_000);
                                                                            assert_eq!(cfg.discovery.observed_swaps_retention_days, 60);
                                                                        },
                                                                    );
                                                                },
                                                            );
                                                        },
                                                    );
                                                },
                                            );
                                            },
                                        );
                                    },
                                );
                            },
                        );
                    });
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_discovery_publish_cadence_faster_than_fetch() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_FETCH_REFRESH_SECONDS",
                "120",
                || {
                    with_env_var("SOLANA_COPY_BOT_DISCOVERY_REFRESH_SECONDS", "60", || {
                        let err = load_from_env_or_default(config_path)
                            .expect_err("publish cadence faster than fetch cadence must fail");
                        assert!(
                        err.to_string().contains(
                            "discovery.refresh_seconds (60) must be >= discovery.fetch_refresh_seconds (120)"
                        ),
                        "unexpected error: {err:#}"
                    );
                    });
                },
            );
        });
    });
}

#[test]
fn load_from_env_allows_discovery_retention_shorter_than_scoring_window() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_OBSERVED_SWAPS_RETENTION_DAYS",
                "7",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("short retention should now be allowed");
                    assert_eq!(cfg.discovery.scoring_window_days, 30);
                    assert_eq!(cfg.discovery.observed_swaps_retention_days, 7);
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_discovery_aggregate_flag_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_SCORING_AGGREGATES_WRITE_ENABLED",
                "true",
                || {
                    with_env_var(
                        "SOLANA_COPY_BOT_DISCOVERY_SCORING_AGGREGATES_ENABLED",
                        "true",
                        || {
                            let (cfg, _) = load_from_env_or_default(config_path)
                                .expect("aggregate flag overrides should load");
                            assert!(cfg.discovery.scoring_aggregates_write_enabled);
                            assert!(cfg.discovery.scoring_aggregates_enabled);
                        },
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_discovery_aggregate_reads_without_writes() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_SCORING_AGGREGATES_ENABLED",
                "true",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("aggregate reads without writes must fail config load")
                        .to_string();
                    assert!(
                        err.contains(
                            "discovery.scoring_aggregates_enabled requires discovery.scoring_aggregates_write_enabled = true"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_discovery_fetch_cap_above_window_cap() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
                "10000",
                || {
                    with_env_var(
                        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE",
                        "10001",
                        || {
                            let err = load_from_env_or_default(config_path)
                                .expect_err("fetch cap above window cap must fail config load")
                                .to_string();
                            assert!(
                            err.contains(
                                "discovery.max_fetch_swaps_per_cycle (10001) must be <= discovery.max_window_swaps_in_memory (10000)"
                            ),
                            "unexpected error: {err}"
                        );
                        },
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_path_rejects_zero_discovery_fetch_page_budget() {
    with_temp_config_file(
        r#"
[discovery]
max_fetch_pages_per_cycle = 0
"#,
        |config_path| {
            let err = load_from_path(config_path)
                .expect_err("zero discovery fetch page budget must fail config load")
                .to_string();
            assert!(
                err.contains("discovery.max_fetch_pages_per_cycle (0) must be >= 1"),
                "unexpected error: {err}"
            );
        },
    );
}

#[test]
fn load_from_env_rejects_zero_discovery_fetch_time_budget_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_FETCH_TIME_BUDGET_MS",
                "0",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("zero discovery fetch time budget must fail config load")
                        .to_string();
                    assert!(
                        err.contains("discovery.fetch_time_budget_ms (0) must be >= 1"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_discovery_refresh_seconds_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_DISCOVERY_REFRESH_SECONDS", "abc", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid discovery refresh override must fail config load")
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_DISCOVERY_REFRESH_SECONDS"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_discovery_window_cap_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
                "12.5",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("invalid discovery window cap override must fail config load")
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_discovery_retention_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_OBSERVED_SWAPS_RETENTION_DAYS",
                "-1",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("invalid discovery retention override must fail config load")
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_DISCOVERY_OBSERVED_SWAPS_RETENTION_DAYS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_history_retention_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_HISTORY_RETENTION_ENABLED", "true", || {
                with_env_var(
                    "SOLANA_COPY_BOT_HISTORY_RETENTION_SWEEP_SECONDS",
                    "7200",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_HISTORY_RETENTION_PROTECTED_HISTORY_DAYS",
                            "45",
                            || {
                                with_env_var(
                                    "SOLANA_COPY_BOT_HISTORY_RETENTION_RISK_EVENTS_DAYS",
                                    "60",
                                    || {
                                        with_env_var(
                                            "SOLANA_COPY_BOT_HISTORY_RETENTION_COPY_SIGNALS_DAYS",
                                            "75",
                                            || {
                                                with_env_var(
                                                    "SOLANA_COPY_BOT_HISTORY_RETENTION_ORDERS_DAYS",
                                                    "50",
                                                    || {
                                                        with_env_var(
                                                    "SOLANA_COPY_BOT_HISTORY_RETENTION_FILLS_DAYS",
                                                    "50",
                                                    || {
                                                        with_env_var(
                                                            "SOLANA_COPY_BOT_HISTORY_RETENTION_SHADOW_CLOSED_TRADES_DAYS",
                                                            "120",
                                                            || {
                                                                let (cfg, _) =
                                                                    load_from_env_or_default(
                                                                        config_path,
                                                                    )
                                                                    .expect(
                                                                        "history retention env overrides must load",
                                                                    );
                                                                assert!(
                                                                    cfg.history_retention.enabled
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .sweep_seconds,
                                                                    7_200
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .protected_history_days,
                                                                    45
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .risk_events_days,
                                                                    60
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .copy_signals_days,
                                                                    75
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .orders_days,
                                                                    50
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .fills_days,
                                                                    50
                                                                );
                                                                assert_eq!(
                                                                    cfg.history_retention
                                                                        .shadow_closed_trades_days,
                                                                    120
                                                                );
                                                            },
                                                        );
                                                    },
                                                );
                                                    },
                                                );
                                            },
                                        );
                                    },
                                );
                            },
                        );
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_history_retention_order_fill_mismatch() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_HISTORY_RETENTION_ENABLED", "true", || {
                with_env_var(
                    "SOLANA_COPY_BOT_HISTORY_RETENTION_ORDERS_DAYS",
                    "40",
                    || {
                        with_env_var("SOLANA_COPY_BOT_HISTORY_RETENTION_FILLS_DAYS", "30", || {
                            let err = load_from_env_or_default(config_path)
                                .expect_err(
                                    "history retention fills_days mismatch must fail config load",
                                )
                                .to_string();
                            assert!(
                                err.contains(
                                    "history_retention.fills_days (30) must equal history_retention.orders_days (40)"
                                ),
                                "unexpected error: {err}"
                            );
                        });
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_history_retention_copy_signal_horizon_shorter_than_orders() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_HISTORY_RETENTION_ENABLED", "true", || {
                with_env_var(
                    "SOLANA_COPY_BOT_HISTORY_RETENTION_COPY_SIGNALS_DAYS",
                    "20",
                    || {
                        with_env_var(
                            "SOLANA_COPY_BOT_HISTORY_RETENTION_ORDERS_DAYS",
                            "30",
                            || {
                                with_env_var(
                                    "SOLANA_COPY_BOT_HISTORY_RETENTION_FILLS_DAYS",
                                    "30",
                                    || {
                                        let err = load_from_env_or_default(config_path)
                                            .expect_err(
                                                "copy_signals horizon shorter than orders must fail config load",
                                            )
                                            .to_string();
                                        assert!(
                                            err.contains(
                                                "history_retention.copy_signals_days (20) must be >= history_retention.orders_days (30)"
                                            ),
                                            "unexpected error: {err}"
                                        );
                                    },
                                );
                            },
                        );
                    },
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_poll_interval_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_EXECUTION_POLL_INTERVAL_MS", "12.5", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid execution poll interval override must fail config load")
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_EXECUTION_POLL_INTERVAL_MS"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_pretrade_min_sol_reserve_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE",
                "abc",
                || {
                    let err = load_from_env_or_default(config_path)
                    .expect_err(
                        "invalid execution pretrade min sol reserve override must fail config load",
                    )
                    .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_pretrade_max_fee_overhead_bps_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_FEE_OVERHEAD_BPS",
                "abc",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid execution pretrade max fee overhead override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_FEE_OVERHEAD_BPS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_pretrade_fee_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_LAMPORTS",
                "-1",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid execution pretrade fee lamports override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains(
                            "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_LAMPORTS"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_max_submit_attempts_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_MAX_SUBMIT_ATTEMPTS",
                "nope",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid execution max submit attempts override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_EXECUTION_MAX_SUBMIT_ATTEMPTS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_yellowstone_connect_timeout_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS",
                "12.5",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid yellowstone connect timeout override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_fetch_concurrency_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY", "abc", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err(
                        "invalid ingestion fetch_concurrency override must fail config load",
                    )
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_tx_fetch_retries_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRIES", "-1", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid ingestion tx_fetch_retries override must fail config load")
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRIES"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_global_rpc_rps_limit_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT",
                "many",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid ingestion global_rpc_rps_limit override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_shadow_min_token_age_seconds_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_SHADOW_MIN_TOKEN_AGE_SECONDS",
                "soon",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid shadow min_token_age_seconds override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_SHADOW_MIN_TOKEN_AGE_SECONDS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_shadow_min_liquidity_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL", "high", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid shadow min_liquidity_sol override must fail config load")
                    .to_string();
                assert!(
                    err.contains("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL"),
                    "unexpected error: {err}"
                );
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_max_concurrent_positions_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS",
                "-1",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk max_concurrent_positions override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_execution_buy_cooldown_seconds_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS",
                "cooldown",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk execution buy cooldown override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_soft_exposure_cap_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL",
                "cap",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow soft exposure cap override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_soft_exposure_resume_below_sol_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
                "resume",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow soft exposure resume threshold override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_risk_shadow_stale_close_recovery_zero_price_enabled_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED",
                "true",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("load config with recovery override");
                    assert!(cfg.risk.shadow_stale_close_recovery_zero_price_enabled);
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_stale_close_recovery_zero_price_enabled_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED",
                "sometimes",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err(
                            "invalid risk shadow stale close recovery override must fail config load",
                        )
                        .to_string();
                    assert!(
                        err.contains(
                            "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_universe_breach_cycles_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES",
                "many",
                || {
                    let err = load_from_env_or_default(config_path)
                    .expect_err(
                        "invalid risk shadow universe breach cycles override must fail config load",
                    )
                    .to_string();
                    assert!(
                        err.contains("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_path_rejects_invalid_ingestion_queue_overflow_policy() {
    with_temp_config_file(
        r#"
[ingestion]
queue_overflow_policy = "drop_newest"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("invalid ingestion queue_overflow_policy in config must fail")
                    .to_string();
                assert!(
                    err.contains("ingestion.queue_overflow_policy"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn load_from_path_rejects_invalid_ingestion_source() {
    with_temp_config_file(
        r#"
[ingestion]
source = "laserstream"
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_path(config_path)
                    .expect_err("invalid ingestion.source in config must fail")
                    .to_string();
                assert!(err.contains("ingestion.source"), "unexpected error: {err}");
            });
        },
    );
}

#[test]
fn load_from_env_normalizes_ingestion_source_alias() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_SOURCE", "yellowstone", || {
                let (cfg, _) = load_from_env_or_default(config_path)
                    .expect("known ingestion source alias should load");
                assert_eq!(cfg.ingestion.source, "yellowstone_grpc");
            });
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_source_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var("SOLANA_COPY_BOT_INGESTION_SOURCE", "laserstream", || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid ingestion.source override must fail config load")
                    .to_string();
                assert!(err.contains("ingestion.source"), "unexpected error: {err}");
            });
        });
    });
}

#[test]
fn load_from_env_normalizes_ingestion_queue_overflow_policy_alias() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY",
                "drop-oldest",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("known queue overflow alias should load");
                    assert_eq!(cfg.ingestion.queue_overflow_policy, "drop_oldest");
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_ingestion_queue_overflow_policy_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY",
                "drop_newest",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("invalid queue overflow policy override must fail config load")
                        .to_string();
                    assert!(
                        err.contains("ingestion.queue_overflow_policy"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_invalid_execution_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_EXECUTION_ENABLED",
        "tru",
        "invalid execution.enabled env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_invalid_shadow_quality_gates_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_SHADOW_QUALITY_GATES_ENABLED",
        "enabled",
        "invalid shadow quality_gates_enabled env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_invalid_risk_shadow_killswitch_enabled_override() {
    assert_bool_env_rejected(
        "SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED",
        "maybe",
        "invalid risk shadow_killswitch env override must fail config load",
    );
}

#[test]
fn load_from_env_rejects_sub_lamport_risk_cap() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL",
                "0.0000000001",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("sub-lamport risk cap must fail exact sizing validation")
                        .to_string();
                    assert!(
                        err.contains(
                            "risk.max_position_sol must be representable as at least 1 lamport"
                        ),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_non_finite_pretrade_reserve() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE",
                "NaN",
                || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("non-finite reserve must fail exact sizing validation")
                        .to_string();
                    assert!(
                        err.contains("execution.pretrade_min_sol_reserve must be finite"),
                        "unexpected error: {err}"
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_discovery_window_memory_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
                "222222",
                || {
                    with_env_var(
                        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE",
                        "111111",
                        || {
                            let (cfg, _) = load_from_env_or_default(config_path)
                                .expect("load config with discovery memory env overrides");
                            assert_eq!(cfg.discovery.max_window_swaps_in_memory, 222_222);
                            assert_eq!(cfg.discovery.max_fetch_swaps_per_cycle, 111_111);
                        },
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_dynamic_cu_price_api_overrides() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_PRIMARY_URL",
                "https://priority.example.com/v1/fees",
                || {
                    with_env_var(
                        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_FALLBACK_URL",
                        "https://priority-fallback.example.com/v1/fees",
                        || {
                            with_env_var(
                                "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_AUTH_TOKEN",
                                "api-token",
                                || {
                                    with_env_var(
                                        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_AUTH_TOKEN_FILE",
                                        "/tmp/priority_api.token",
                                        || {
                                            let (cfg, _) = load_from_env_or_default(config_path)
                                                .expect("load config with dynamic cu api env overrides");
                                            assert_eq!(
                                                cfg.execution.submit_dynamic_cu_price_api_primary_url,
                                                "https://priority.example.com/v1/fees"
                                            );
                                            assert_eq!(
                                                cfg.execution.submit_dynamic_cu_price_api_fallback_url,
                                                "https://priority-fallback.example.com/v1/fees"
                                            );
                                            assert_eq!(
                                                cfg.execution.submit_dynamic_cu_price_api_auth_token,
                                                "api-token"
                                            );
                                            assert_eq!(
                                                cfg.execution.submit_dynamic_cu_price_api_auth_token_file,
                                                "/tmp/priority_api.token"
                                            );
                                        },
                                    );
                                },
                            );
                        },
                    );
                },
            );
        });
    });
}

#[test]
fn load_from_env_applies_submit_fastlane_enabled_override() {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(
                "SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED",
                "true",
                || {
                    let (cfg, _) = load_from_env_or_default(config_path)
                        .expect("load config with submit fastlane env override");
                    assert!(cfg.execution.submit_fastlane_enabled);
                },
            );
        });
    });
}

#[test]
fn load_from_env_rejects_follow_top_n_below_shadow_min_active_wallets() {
    with_temp_config_file(
        r#"
[discovery]
follow_top_n = 10

[risk]
shadow_universe_min_active_follow_wallets = 15
"#,
        |config_path| {
            with_clean_copybot_env(|| {
                let err = load_from_env_or_default(config_path)
                    .expect_err("impossible shadow universe config must fail")
                    .to_string();
                assert!(
                    err.contains("discovery.follow_top_n (10) must be >= risk.shadow_universe_min_active_follow_wallets (15)"),
                    "unexpected error: {err}"
                );
            });
        },
    );
}

#[test]
fn execution_config_debug_redacts_secret_values() {
    let mut execution = ExecutionConfig::default();
    execution.submit_adapter_auth_token = "adapter-secret-token".to_string();
    execution.submit_adapter_hmac_secret = "hmac-secret-value".to_string();
    execution.submit_dynamic_cu_price_api_auth_token = "priority-fee-secret".to_string();
    execution.submit_adapter_http_url =
        "https://adapter.example.com/submit?api-key=adapter-query-secret".to_string();

    let execution_debug = format!("{execution:?}");
    assert!(
        !execution_debug.contains("adapter-secret-token"),
        "debug output leaked adapter token: {execution_debug}"
    );
    assert!(
        !execution_debug.contains("hmac-secret-value"),
        "debug output leaked hmac secret: {execution_debug}"
    );
    assert!(
        !execution_debug.contains("priority-fee-secret"),
        "debug output leaked dynamic CU auth token: {execution_debug}"
    );
    assert!(
        execution_debug.contains("[REDACTED]"),
        "debug output should show redaction marker: {execution_debug}"
    );
    assert!(
        !execution_debug.contains("adapter-query-secret"),
        "debug output leaked adapter URL query secret: {execution_debug}"
    );
    assert!(
        execution_debug.contains("https://adapter.example.com/submit?<redacted>"),
        "debug output should redact adapter URL query: {execution_debug}"
    );

    let mut ingestion = IngestionConfig::default();
    ingestion.helius_ws_url = "wss://mainnet.helius-rpc.com/?api-key=helius-ws-secret".to_string();
    ingestion.helius_http_url =
        "https://mainnet.helius-rpc.com/?api-key=helius-http-secret".to_string();
    ingestion.helius_http_urls =
        vec!["https://backup.helius.example/?api-key=backup-secret".to_string()];
    ingestion.yellowstone_x_token = "yellowstone-secret".to_string();
    let ingestion_debug = format!("{ingestion:?}");
    for secret in [
        "helius-ws-secret",
        "helius-http-secret",
        "backup-secret",
        "yellowstone-secret",
    ] {
        assert!(
            !ingestion_debug.contains(secret),
            "ingestion debug output leaked secret={secret}: {ingestion_debug}"
        );
    }
    assert!(
        ingestion_debug.contains("wss://mainnet.helius-rpc.com/?<redacted>"),
        "ingestion debug output should redact ws URL query: {ingestion_debug}"
    );
    assert!(
        ingestion_debug.contains("https://mainnet.helius-rpc.com/?<redacted>"),
        "ingestion debug output should redact http URL query: {ingestion_debug}"
    );
    assert!(
        ingestion_debug.contains("[REDACTED]"),
        "ingestion debug output should show redaction marker: {ingestion_debug}"
    );

    let mut discovery = DiscoveryConfig::default();
    discovery.helius_http_url =
        "https://discovery.helius.example/?api-key=discovery-secret".to_string();
    let discovery_debug = format!("{discovery:?}");
    assert!(
        !discovery_debug.contains("discovery-secret"),
        "discovery debug output leaked URL secret: {discovery_debug}"
    );
    assert!(
        discovery_debug.contains("https://discovery.helius.example/?<redacted>"),
        "discovery debug output should redact URL query: {discovery_debug}"
    );

    let mut shadow = ShadowConfig::default();
    shadow.helius_http_url = "https://shadow.helius.example/?api-key=shadow-secret".to_string();
    let shadow_debug = format!("{shadow:?}");
    assert!(
        !shadow_debug.contains("shadow-secret"),
        "shadow debug output leaked URL secret: {shadow_debug}"
    );
    assert!(
        shadow_debug.contains("https://shadow.helius.example/?<redacted>"),
        "shadow debug output should redact URL query: {shadow_debug}"
    );

    let app_debug = format!(
        "{:?}",
        AppConfig {
            discovery,
            ingestion,
            shadow,
            execution,
            ..AppConfig::default()
        }
    );
    assert!(
        !app_debug.contains("adapter-secret-token"),
        "AppConfig debug output leaked adapter token: {app_debug}"
    );
    assert!(
        !app_debug.contains("yellowstone-secret"),
        "AppConfig debug output leaked ingestion token: {app_debug}"
    );
    for secret in ["discovery-secret", "shadow-secret"] {
        assert!(
            !app_debug.contains(secret),
            "AppConfig debug output leaked nested URL secret={secret}: {app_debug}"
        );
    }
}

fn assert_duplicate_normalized_route_env_rejected(env_name: &'static str, env_value: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("duplicate normalized route keys should fail")
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains("duplicate route after normalization"),
                    "error should describe duplicate normalization, got: {err}"
                );
            });
        });
    });
}

fn assert_route_map_env_rejected_contains(env_name: &'static str, env_value: &str, needle: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid route map env should fail")
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains(needle),
                    "error should contain '{needle}', got: {err}"
                );
            });
        });
    });
}

fn assert_string_list_env_rejected_contains(env_name: &'static str, env_value: &str, needle: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err("invalid string-list env should fail")
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains(needle),
                    "error should contain '{needle}', got: {err}"
                );
            });
        });
    });
}

fn assert_bool_env_rejected(env_name: &'static str, env_value: &str, context: &str) {
    with_temp_config_file("", |config_path| {
        with_clean_copybot_env(|| {
            with_env_var(env_name, env_value, || {
                let err = load_from_env_or_default(config_path)
                    .expect_err(context)
                    .to_string();
                assert!(
                    err.contains(env_name),
                    "error should mention env var, got: {err}"
                );
                assert!(
                    err.contains("must be a valid bool"),
                    "error should describe bool parse failure, got: {err}"
                );
            });
        });
    });
}

fn with_env_var<T>(key: &'static str, value: &str, run: impl FnOnce() -> T) -> T {
    let previous = std::env::var_os(key);
    std::env::set_var(key, value);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    restore_env_var(key, previous);
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn restore_env_var(key: &'static str, previous: Option<OsString>) {
    match previous {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
}

fn with_clean_copybot_env<T>(run: impl FnOnce() -> T) -> T {
    // Serialize all SOLANA_COPY_BOT_* env mutations in this test module.
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let saved: Vec<(OsString, OsString)> = std::env::vars_os()
        .filter(|(key, _)| key.to_string_lossy().starts_with("SOLANA_COPY_BOT_"))
        .collect();
    for (key, _) in &saved {
        std::env::remove_var(key);
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    for (key, value) in saved {
        std::env::set_var(key, value);
    }
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn with_temp_config_file<T>(contents: &str, run: impl FnOnce(&Path) -> T) -> T {
    let path = unique_temp_path();
    fs::write(&path, contents).expect("write temp config");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&path)));
    let _ = fs::remove_file(&path);
    match outcome {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn unique_temp_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let seq = TEMP_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("copybot-config-test-{pid}-{nanos}-{seq}.toml"))
}
