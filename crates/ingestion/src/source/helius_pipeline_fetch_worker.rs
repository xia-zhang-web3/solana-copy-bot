pub(super) async fn fetch_worker_loop(
    worker_id: usize,
    runtime_config: Arc<HeliusRuntimeConfig>,
    notification_queue: Arc<NotificationQueue>,
    out_tx: mpsc::Sender<FetchedObservation>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    fetch_to_output_depth: Arc<AtomicUsize>,
) {
    loop {
        let notification = notification_queue.pop().await;

        let Some(notification) = notification else {
            debug!(
                worker_id,
                "fetch worker exiting because notification queue is closed"
            );
            return;
        };
        decrement_atomic_usize(&ws_to_fetch_depth);

        if runtime_config
            .prefetch_stale_drop
            .is_some_and(|limit| notification.enqueued_at.elapsed() > limit)
        {
            runtime_config
                .telemetry
                .prefetch_stale_dropped
                .fetch_add(1, Ordering::Relaxed);
            runtime_config
                .telemetry
                .fetch_failed
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        match fetch_swap_with_retries(runtime_config.as_ref(), notification).await {
            Ok(Some(fetched)) => {
                if out_tx.send(fetched).await.is_err() {
                    warn!(worker_id, "output channel closed; stopping fetch worker");
                    return;
                }
                increment_atomic_usize(&fetch_to_output_depth);
            }
            Ok(None) => {
                runtime_config
                    .telemetry
                    .fetch_no_swap
                    .fetch_add(1, Ordering::Relaxed);
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(worker_id, error = %error, "fetch worker failed to parse transaction");
            }
        }
    }
}

async fn connect_ws_stream(
    runtime_config: &HeliusRuntimeConfig,
    request_id: &mut u64,
) -> Result<HeliusWsStream> {
    let redacted_ws_url = redacted_url_for_log(&runtime_config.ws_url);
    let (mut ws, _response) = connect_async(&runtime_config.ws_url)
        .await
        .with_context(|| format!("failed connecting to {redacted_ws_url}"))?;

    for program_id in &runtime_config.interested_program_ids {
        *request_id = request_id.saturating_add(1);
        let request = json!({
            "jsonrpc": "2.0",
            "id": *request_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [program_id]},
                {"commitment": "confirmed"}
            ]
        });
        ws.send(Message::Text(request.to_string().into()))
            .await
            .with_context(|| format!("failed sending logsSubscribe for {program_id}"))?;
    }

    info!(
        ws_url = %redacted_ws_url,
        http_endpoints = runtime_config.http_endpoints.len(),
        programs = runtime_config.interested_program_ids.len(),
        "helius ws connected and subscriptions sent"
    );

    Ok(ws)
}
