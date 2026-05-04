#[allow(dead_code)]
impl HeliusWsSource {
    pub fn new(config: &IngestionConfig) -> Result<Self> {
        let mut interested_program_ids: HashSet<String> =
            config.subscribe_program_ids.iter().cloned().collect();
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.raydium_program_ids.iter().cloned());
            interested_program_ids.extend(config.pumpswap_program_ids.iter().cloned());
        }

        if interested_program_ids.is_empty() {
            return Err(anyhow!(
                "helius_ws requires program IDs in subscribe_program_ids/raydium_program_ids/pumpswap_program_ids"
            ));
        }

        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.tx_request_timeout_ms.max(500)))
            .build()
            .context("failed building reqwest client")?;

        let mut candidates = Vec::new();
        for url in &config.helius_http_urls {
            let trimmed = url.trim();
            if !trimmed.is_empty() && !candidates.iter().any(|existing| existing == trimmed) {
                candidates.push(trimmed.to_string());
            }
        }
        if candidates.is_empty() {
            let trimmed = config.helius_http_url.trim();
            if !trimmed.is_empty() {
                candidates.push(trimmed.to_string());
            }
        }

        let mut http_urls = Vec::new();
        for candidate in candidates {
            let redacted_candidate = redacted_url_for_log(&candidate);
            if !(candidate.starts_with("http://") || candidate.starts_with("https://")) {
                warn!(
                    url = %redacted_candidate,
                    "dropping ingestion HTTP URL without explicit http(s):// prefix"
                );
                continue;
            }

            let parsed = match Url::parse(&candidate) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(
                        url = %redacted_candidate,
                        error = %error,
                        "dropping invalid ingestion HTTP URL"
                    );
                    continue;
                }
            };

            let scheme = parsed.scheme();
            if scheme != "http" && scheme != "https" {
                warn!(
                    url = %redacted_candidate,
                    scheme = %scheme,
                    "dropping ingestion HTTP URL with unsupported scheme"
                );
                continue;
            }
            if parsed.host_str().is_none() {
                warn!(
                    url = %redacted_candidate,
                    "dropping ingestion HTTP URL without host"
                );
                continue;
            }

            if !http_urls.iter().any(|existing| existing == &candidate) {
                http_urls.push(candidate);
            }
        }
        if http_urls.is_empty() {
            return Err(anyhow!(
                "no valid ingestion HTTP URL configured (check helius_http_url / helius_http_urls)"
            ));
        }

        let endpoint_rps_limit = effective_per_endpoint_rps_limit(
            config.per_endpoint_rpc_rps_limit,
            config.global_rpc_rps_limit,
            http_urls.len(),
        );
        if endpoint_rps_limit != config.per_endpoint_rpc_rps_limit {
            warn!(
                configured_per_endpoint_rps = config.per_endpoint_rpc_rps_limit,
                effective_per_endpoint_rps = endpoint_rps_limit,
                global_rps = config.global_rpc_rps_limit,
                endpoint_count = http_urls.len(),
                "adjusted per-endpoint RPC limiter to avoid self-throttling with a single endpoint"
            );
        }
        let endpoint_burst = endpoint_rps_limit.max(1);
        let http_endpoints = http_urls
            .into_iter()
            .map(|url| {
                Arc::new(HeliusEndpoint {
                    url,
                    limiter: TokenBucketLimiter::new(endpoint_rps_limit, endpoint_burst),
                })
            })
            .collect::<Vec<_>>();
        let global_rps_limit = config.global_rpc_rps_limit;
        let global_http_limiter =
            TokenBucketLimiter::new(global_rps_limit, global_rps_limit.max(1));
        let raw_queue_policy = config.queue_overflow_policy.trim();
        let queue_overflow_policy = QueueOverflowPolicy::parse(raw_queue_policy);
        let normalized_queue_policy = raw_queue_policy.to_ascii_lowercase();
        if !raw_queue_policy.is_empty()
            && normalized_queue_policy != "block"
            && normalized_queue_policy != "drop_oldest"
            && normalized_queue_policy != "drop-oldest"
        {
            warn!(
                policy = %raw_queue_policy,
                "unknown ingestion.queue_overflow_policy; falling back to block"
            );
        }

        let runtime_config = HeliusRuntimeConfig {
            ws_url: config.helius_ws_url.clone(),
            http_endpoints,
            http_endpoint_rr: AtomicUsize::new(0),
            global_http_limiter,
            reconnect_initial_ms: config.reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .reconnect_max_ms
                .max(config.reconnect_initial_ms.max(200)),
            tx_fetch_retries: config.tx_fetch_retries,
            tx_fetch_retry_base_ms: config.tx_fetch_retry_delay_ms.max(50),
            tx_fetch_retry_max_ms: config
                .tx_fetch_retry_max_ms
                .max(config.tx_fetch_retry_delay_ms.max(50)),
            tx_fetch_retry_jitter_ms: config.tx_fetch_retry_jitter_ms,
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            seen_signatures_ttl: Duration::from_millis(config.seen_signatures_ttl_ms.max(1_000)),
            prefetch_stale_drop: if config.prefetch_stale_drop_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(config.prefetch_stale_drop_ms.max(1)))
            },
            interested_program_ids,
            raydium_program_ids: config.raydium_program_ids.iter().cloned().collect(),
            pumpswap_program_ids: config.pumpswap_program_ids.iter().cloned().collect(),
            http_client,
            telemetry: Arc::new(IngestionTelemetry::default()),
        };

        Ok(Self {
            runtime_config: Arc::new(runtime_config),
            fetch_concurrency: config.fetch_concurrency.max(1),
            ws_queue_capacity: config.ws_queue_capacity.max(128),
            queue_overflow_policy,
            output_queue_capacity: config.output_queue_capacity.max(64),
            reorder: ReorderBuffer::new(
                config.reorder_hold_ms.max(1),
                config.reorder_max_buffer.max(16),
            ),
            telemetry_report_seconds: config.telemetry_report_seconds.max(5),
            pipeline: None,
        })
    }
}
