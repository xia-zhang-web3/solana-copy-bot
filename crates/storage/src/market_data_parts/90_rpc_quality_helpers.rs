use super::*;

pub(super) fn recent_raw_journal_write_summary(
    state: &RecentRawJournalStateRow,
    batch_rows: usize,
    inserted_rows: usize,
) -> RecentRawJournalWriteSummary {
    RecentRawJournalWriteSummary {
        batch_rows,
        inserted_rows,
        covered_since: state.covered_since,
        covered_through_cursor: state.covered_through_cursor.clone(),
        row_count: state.row_count,
        last_batch_completed_at: state.last_batch_completed_at,
        ..RecentRawJournalWriteSummary::default()
    }
}

pub(super) fn advance_recent_raw_journal_state_for_batch(
    state: &mut RecentRawJournalStateRow,
    processed_swaps: &[SwapEvent],
    inserted_rows: usize,
    completed_at: DateTime<Utc>,
) {
    if processed_swaps.is_empty() {
        return;
    }
    if inserted_rows > 0 {
        if let Some(first_swap) = processed_swaps.first() {
            state.covered_since = Some(match state.covered_since {
                Some(existing) if existing <= first_swap.ts_utc => existing,
                _ => first_swap.ts_utc,
            });
        }
        state.row_count = state.row_count.saturating_add(inserted_rows);
    }
    if let Some(last_swap) = processed_swaps.last() {
        let last_cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        let should_advance_cursor = state
            .covered_through_cursor
            .as_ref()
            .map(|cursor| discovery_runtime_cursor_cmp(&last_cursor, cursor).is_gt())
            .unwrap_or(state.row_count > 0 || inserted_rows > 0);
        if should_advance_cursor {
            state.covered_through_cursor = Some(last_cursor);
        }
    }
    state.last_batch_rows = inserted_rows;
    state.last_batch_completed_at = Some(completed_at);
    state.updated_at = Some(completed_at);
}

pub(super) fn duration_ms_ceil(duration: StdDuration) -> u64 {
    let micros = duration.as_micros();
    if micros == 0 {
        0
    } else {
        micros.div_ceil(1000).min(u128::from(u64::MAX)) as u64
    }
}

pub(super) fn rpc_result(payload: &Value) -> &Value {
    payload.get("result").unwrap_or(payload)
}

pub(super) fn post_helius_json(
    client: &Client,
    helius_http_url: &str,
    payload: &Value,
) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed helius rpc request to {helius_http_url}"))?;
    let status = response.status();
    let body = response
        .json::<Value>()
        .context("failed parsing helius rpc response json")?;
    if !status.is_success() {
        return Err(anyhow!("helius rpc returned http status {status}: {body}"));
    }
    Ok(body)
}

pub(super) fn fetch_token_holders(
    client: &Client,
    helius_http_url: &str,
    mint: &str,
) -> Result<u64> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            TOKEN_PROGRAM_ID,
            {
                "encoding": "jsonParsed"
                ,
                "filters": [
                    { "dataSize": 165 },
                    { "memcmp": { "offset": 0, "bytes": mint } }
                ]
            },
        ],
    });
    let response = post_helius_json(client, helius_http_url, &payload)?;
    parse_token_holders_from_program_accounts_response(&response)
}

pub(super) fn parse_token_holders_from_program_accounts_response(response: &Value) -> Result<u64> {
    let rpc_result = rpc_result(response);
    let accounts = rpc_result
        .as_array()
        .or_else(|| rpc_result.get("value").and_then(Value::as_array))
        .ok_or_else(|| anyhow!("missing token accounts array in rpc response"))?;
    let mut unique_owners = HashSet::new();
    for (index, account) in accounts.iter().enumerate() {
        let info = account
            .get("account")
            .and_then(|value| value.get("data"))
            .and_then(|value| value.get("parsed"))
            .and_then(|value| value.get("info"))
            .ok_or_else(|| anyhow!("missing parsed token account info at index={index}"))?;
        let owner = info
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token account owner at index={index}"))?;
        let amount_raw = info
            .get("tokenAmount")
            .and_then(|value| value.get("amount"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token amount at index={index}"))?;
        let amount = amount_raw
            .parse::<u64>()
            .with_context(|| format!("invalid token amount at index={index}: {amount_raw}"))?;
        if amount > 0 {
            unique_owners.insert(owner.to_string());
        }
    }
    Ok(unique_owners.len() as u64)
}

pub(super) fn fetch_token_age_seconds(
    client: &Client,
    helius_http_url: &str,
    mint: &str,
    max_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<Option<u64>> {
    let now_ts = Utc::now().timestamp();
    let min_block_time = min_age_hint_seconds
        .and_then(|hint| now_ts.checked_sub(hint as i64))
        .unwrap_or(i64::MIN);

    let mut oldest_seen: Option<i64> = None;
    let mut before_sig: Option<String> = None;

    for _page in 0..max_pages {
        let mut options = json!({ "limit": 1000 });
        if let Some(before) = before_sig.as_deref() {
            options["before"] = Value::String(before.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [mint, options],
        });

        let response = post_helius_json(client, helius_http_url, &payload)?;
        let entries = rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("missing signatures array in helius response"))?;
        if entries.is_empty() {
            break;
        }

        for entry in entries {
            if let Some(value) = entry.get("blockTime").and_then(Value::as_i64) {
                oldest_seen = Some(oldest_seen.map_or(value, |current| current.min(value)));
            }
            if let Some(signature) = entry.get("signature").and_then(Value::as_str) {
                before_sig = Some(signature.to_string());
            }
        }

        if oldest_seen.is_some_and(|value| value <= min_block_time) {
            break;
        }
    }

    let Some(oldest) = oldest_seen else {
        return Ok(None);
    };

    if oldest > now_ts {
        return Ok(None);
    }

    Ok(Some((now_ts - oldest) as u64))
}
