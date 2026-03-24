use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapFillMissingSegment {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct GapFillPlan {
    pub requested_window_start: DateTime<Utc>,
    pub requested_window_end: DateTime<Utc>,
    pub required_window_start: Option<DateTime<Utc>>,
    pub derived_from_restore_state: bool,
    pub journal_available: bool,
    pub journal_covers_artifact_cursor: bool,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
}

#[derive(Debug, Clone, Default)]
pub struct ProgramIdConfig {
    pub interested_program_ids: HashSet<String>,
    pub raydium_program_ids: HashSet<String>,
    pub pumpswap_program_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct ParsedUiAmount {
    amount: f64,
    raw_amount: Option<String>,
    decimals: Option<u8>,
}

#[derive(Debug, Clone, Default)]
struct MintDelta {
    amount_delta: f64,
    raw_delta: Option<i128>,
    decimals: Option<u8>,
    exact_unavailable: bool,
}

pub fn resolve_gap_fill_plan(
    runtime_store: &SqliteStore,
    explicit_window_start: Option<DateTime<Utc>>,
    explicit_window_end: Option<DateTime<Utc>>,
) -> Result<GapFillPlan> {
    let restore_state = runtime_store.discovery_recent_raw_restore_state_read_only()?;
    if let (Some(requested_window_start), Some(requested_window_end)) =
        (explicit_window_start, explicit_window_end)
    {
        if requested_window_end <= requested_window_start {
            bail!("gap-fill window end must be strictly after start");
        }
        return Ok(GapFillPlan {
            requested_window_start,
            requested_window_end,
            required_window_start: restore_state.required_window_start,
            derived_from_restore_state: false,
            journal_available: restore_state.journal_available,
            journal_covers_artifact_cursor: restore_state.journal_covers_artifact_cursor,
            journal_covered_since: restore_state.journal_covered_since,
            journal_covered_through_cursor: restore_state.journal_covered_through_cursor,
        });
    }

    if restore_state.raw_coverage_satisfied {
        bail!("recent raw coverage is already satisfied; bounded gap fill is not required");
    }

    let required_window_start = restore_state.required_window_start.ok_or_else(|| {
        anyhow!("restore state is missing required_window_start; rerun runtime restore first")
    })?;
    let requested_window_end = restore_state.journal_covered_since.ok_or_else(|| {
        anyhow!(
            "restore state is missing journal_covered_since; bounded gap-fill cannot derive a missing window"
        )
    })?;
    if requested_window_end <= required_window_start {
        bail!(
            "restore state does not expose a bounded missing window: journal_covered_since ({}) must be after required_window_start ({})",
            requested_window_end.to_rfc3339(),
            required_window_start.to_rfc3339()
        );
    }

    Ok(GapFillPlan {
        requested_window_start: required_window_start,
        requested_window_end,
        required_window_start: Some(required_window_start),
        derived_from_restore_state: true,
        journal_available: restore_state.journal_available,
        journal_covers_artifact_cursor: restore_state.journal_covers_artifact_cursor,
        journal_covered_since: restore_state.journal_covered_since,
        journal_covered_through_cursor: restore_state.journal_covered_through_cursor,
    })
}

pub fn parse_transaction_to_swap(
    result: &Value,
    expected_wallet: &str,
    program_ids: &ProgramIdConfig,
) -> Result<Option<SwapEvent>> {
    let meta = match result.get("meta") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
        return Ok(None);
    }

    let account_keys = extract_account_keys(result);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signer_index = account_keys
        .iter()
        .position(|(_, is_signer)| *is_signer)
        .unwrap_or(0);
    let signer = account_keys
        .get(signer_index)
        .map(|(pubkey, _)| pubkey.as_str())
        .ok_or_else(|| anyhow!("missing signer in parsed account keys"))?;
    if signer != expected_wallet {
        return Ok(None);
    }

    let mut normalized_program_ids = extract_program_ids(result, meta);
    if normalized_program_ids.is_empty() {
        if program_ids.interested_program_ids.is_empty() {
            return Ok(None);
        }
        normalized_program_ids = program_ids.interested_program_ids.clone();
    } else if !program_ids.interested_program_ids.is_empty()
        && !normalized_program_ids
            .iter()
            .any(|program| program_ids.interested_program_ids.contains(program))
    {
        return Ok(None);
    }

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_json_balances(meta, signer_index, signer) {
            Some(value) => value,
            None => return Ok(None),
        };
    let logs = value_to_string_vec(meta.get("logMessages")).unwrap_or_default();
    let dex = detect_dex(
        &normalized_program_ids,
        &logs,
        &program_ids.raydium_program_ids,
        &program_ids.pumpswap_program_ids,
    )
    .ok_or_else(|| anyhow!("unable to classify dex for {}", expected_wallet))?;
    let ts_utc = result
        .get("blockTime")
        .and_then(Value::as_i64)
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    Ok(Some(SwapEvent {
        wallet: signer.to_string(),
        dex,
        token_in,
        token_out,
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        signature: result
            .pointer("/transaction/signatures/0")
            .and_then(Value::as_str)
            .or_else(|| result.get("signature").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string(),
        slot,
        ts_utc,
        exact_amounts: build_exact_swap_amounts(&amount_in, &amount_out),
    }))
}

pub fn reset_sqlite_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    for suffix in ["", "-wal", "-shm"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{}", path.display(), suffix))
        };
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("failed removing {}", candidate.display()))?;
        }
    }
    Ok(())
}

pub fn compute_missing_segments(
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    journal_state: &copybot_storage::RecentRawJournalStateRow,
) -> Vec<GapFillMissingSegment> {
    if journal_state.row_count == 0 {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: requested_window_end,
            reason: "source_uncovered_window".to_string(),
        }];
    }
    let Some(covered_since) = journal_state.covered_since else {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: requested_window_end,
            reason: "source_uncovered_window".to_string(),
        }];
    };
    if covered_since > requested_window_start {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: covered_since,
            reason: "source_lag".to_string(),
        }];
    }
    Vec::new()
}

pub fn min_ts_opt(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub fn max_cursor_opt(
    left: Option<&DiscoveryRuntimeCursor>,
    right: Option<&DiscoveryRuntimeCursor>,
) -> Option<DiscoveryRuntimeCursor> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if cmp_cursor(left, right) != Ordering::Less {
                Some(left.clone())
            } else {
                Some(right.clone())
            }
        }
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

pub fn cmp_cursor(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

pub fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

pub fn format_optional_cursor(value: Option<&DiscoveryRuntimeCursor>) -> String {
    value
        .map(|cursor| {
            format!(
                "{}:{}:{}",
                cursor.ts_utc.to_rfc3339(),
                cursor.slot,
                cursor.signature
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

pub fn post_helius_json(client: &Client, helius_http_url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed helius rpc request to {helius_http_url}"))?;
    let status = response.status();
    let body = response
        .text()
        .context("failed reading helius rpc response body")?;
    let parsed: Value =
        serde_json::from_str(&body).context("failed parsing helius rpc response json")?;
    if !status.is_success() {
        return Err(anyhow!("helius rpc returned http status {status}: {body}"));
    }
    Ok(parsed)
}

fn extract_account_keys(result: &Value) -> Vec<(String, bool)> {
    result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(|item| {
                    if let Some(pubkey) = item.as_str() {
                        return Some((pubkey.to_string(), false));
                    }
                    let pubkey = item.get("pubkey").and_then(Value::as_str)?;
                    let signer = item.get("signer").and_then(Value::as_bool).unwrap_or(false);
                    Some((pubkey.to_string(), signer))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn extract_program_ids(result: &Value, meta: &Value) -> HashSet<String> {
    let mut set = HashSet::new();

    if let Some(ixs) = result
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
    {
        for ix in ixs {
            if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                set.insert(program_id.to_string());
            }
        }
    }

    if let Some(inner) = meta.get("innerInstructions").and_then(Value::as_array) {
        for group in inner {
            if let Some(ixs) = group.get("instructions").and_then(Value::as_array) {
                for ix in ixs {
                    if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                        set.insert(program_id.to_string());
                    }
                }
            }
        }
    }

    for log in value_to_string_vec(meta.get("logMessages"))
        .unwrap_or_default()
        .iter()
    {
        if let Some(program_id) = extract_program_id_from_log(log) {
            set.insert(program_id);
        }
    }

    set
}

fn extract_program_id_from_log(log: &str) -> Option<String> {
    let mut parts = log.split_whitespace();
    if parts.next()? != "Program" {
        return None;
    }
    let program_id = parts.next()?.trim();
    if program_id.is_empty() {
        None
    } else {
        Some(program_id.to_string())
    }
}

fn infer_swap_from_json_balances(
    meta: &Value,
    signer_index: usize,
    signer: &str,
) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, MintDelta> = HashMap::new();

    let pre = meta
        .get("preTokenBalances")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let post = meta
        .get("postTokenBalances")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in pre {
        if item.get("owner").and_then(Value::as_str) == Some(signer) {
            let mint = item.get("mint").and_then(Value::as_str)?.to_string();
            let amount = parse_ui_amount_json(item.get("uiTokenAmount"))?;
            mint_deltas.entry(mint).or_default().apply_sub(&amount);
        }
    }
    for item in post {
        if item.get("owner").and_then(Value::as_str) == Some(signer) {
            let mint = item.get("mint").and_then(Value::as_str)?.to_string();
            let amount = parse_ui_amount_json(item.get("uiTokenAmount"))?;
            mint_deltas.entry(mint).or_default().apply_add(&amount);
        }
    }

    let mut token_in_candidates = Vec::new();
    let mut token_out_candidates = Vec::new();
    for (mint, delta) in &mint_deltas {
        if delta.amount_delta < -TOKEN_EPS {
            token_in_candidates.push((mint.clone(), delta.candidate()));
        } else if delta.amount_delta > TOKEN_EPS {
            token_out_candidates.push((mint.clone(), delta.candidate()));
        }
    }
    token_in_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(Ordering::Equal)
    });
    token_out_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(Ordering::Equal)
    });

    let sol_token_delta = mint_deltas
        .get(SOL_MINT)
        .map(|delta| delta.amount_delta)
        .unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
            return Some((
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta.abs(),
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
                out_mint,
                out_amt,
            ));
        }
    }
    if sol_token_delta > TOKEN_EPS {
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
            return Some((
                in_mint,
                in_amt,
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta,
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
            ));
        }
    }

    let sol_delta = signer_sol_delta(meta, signer_index);
    let sol_amount = sol_delta.as_ref().map(|value| value.amount).unwrap_or(0.0);
    let sol_exact = sol_delta.map(|value| ParsedUiAmount {
        amount: value.amount.abs(),
        raw_amount: value.raw_amount,
        decimals: value.decimals,
    });
    if sol_amount < -SOL_EPS {
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
            return Some((SOL_MINT.to_string(), sol_exact.clone()?, out_mint, out_amt));
        }
    }
    if sol_amount > SOL_EPS {
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_exact?));
        }
    }

    None
}

fn signer_sol_delta(meta: &Value, signer_index: usize) -> Option<ParsedUiAmount> {
    let pre_sol = meta
        .get("preBalances")
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)?;
    let post_sol = meta
        .get("postBalances")
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)?;
    let delta = post_sol as i128 - pre_sol as i128;
    Some(ParsedUiAmount {
        amount: delta as f64 / 1_000_000_000.0,
        raw_amount: None,
        decimals: None,
    })
}

fn dominant_non_sol_leg(entries: &[(String, ParsedUiAmount)]) -> Option<(String, ParsedUiAmount)> {
    const EPS: f64 = 1e-12;
    const SECOND_LEG_AMBIGUITY_RATIO: f64 = 0.15;
    let non_sol: Vec<(String, ParsedUiAmount)> = entries
        .iter()
        .filter(|(mint, value)| mint != SOL_MINT && value.amount > EPS)
        .cloned()
        .collect();
    let (primary_mint, primary_value) = non_sol.first()?.clone();
    if non_sol.len() >= 2 {
        let second_value = non_sol[1].1.amount;
        if second_value > primary_value.amount * SECOND_LEG_AMBIGUITY_RATIO {
            return None;
        }
    }
    Some((primary_mint, primary_value))
}

fn parse_ui_amount_json(ui_amount: Option<&Value>) -> Option<ParsedUiAmount> {
    let ui_amount = ui_amount?;
    if let Some(amount) = ui_amount.get("uiAmountString").and_then(Value::as_str) {
        let parsed = amount.parse::<f64>().ok()?;
        return parsed.is_finite().then(|| ParsedUiAmount {
            amount: parsed,
            raw_amount: ui_amount
                .get("amount")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            decimals: ui_amount
                .get("decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok()),
        });
    }
    if let Some(amount) = ui_amount.get("uiAmount").and_then(Value::as_f64) {
        return amount.is_finite().then_some(ParsedUiAmount {
            amount,
            raw_amount: ui_amount
                .get("amount")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            decimals: ui_amount
                .get("decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok()),
        });
    }
    let raw = ui_amount.get("amount").and_then(Value::as_str)?;
    let decimals = ui_amount.get("decimals").and_then(Value::as_u64)?;
    if decimals > 18 {
        return None;
    }
    let parsed_raw = raw.parse::<f64>().ok()?;
    let amount = parsed_raw / 10f64.powi(decimals as i32);
    amount.is_finite().then(|| ParsedUiAmount {
        amount,
        raw_amount: Some(raw.to_string()),
        decimals: u8::try_from(decimals).ok(),
    })
}

fn build_exact_swap_amounts(
    amount_in: &ParsedUiAmount,
    amount_out: &ParsedUiAmount,
) -> Option<ExactSwapAmounts> {
    Some(ExactSwapAmounts {
        amount_in_raw: amount_in.raw_amount.clone()?,
        amount_in_decimals: amount_in.decimals?,
        amount_out_raw: amount_out.raw_amount.clone()?,
        amount_out_decimals: amount_out.decimals?,
    })
}

fn value_to_string_vec(value: Option<&Value>) -> Option<Vec<String>> {
    Some(
        value?
            .as_array()?
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
    )
}

fn detect_dex(
    program_ids: &HashSet<String>,
    logs: &[String],
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> Option<String> {
    if program_ids
        .iter()
        .any(|program| raydium_program_ids.contains(program))
        || logs
            .iter()
            .any(|log| log.to_ascii_lowercase().contains("raydium"))
    {
        return Some("raydium".to_string());
    }
    if program_ids
        .iter()
        .any(|program| pumpswap_program_ids.contains(program))
        || logs
            .iter()
            .any(|log| log.to_ascii_lowercase().contains("pump"))
    {
        return Some("pumpswap".to_string());
    }
    None
}

impl MintDelta {
    fn apply_sub(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta -= amount.amount;
        self.apply_raw_delta(amount, -1);
    }

    fn apply_add(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta += amount.amount;
        self.apply_raw_delta(amount, 1);
    }

    fn apply_raw_delta(&mut self, amount: &ParsedUiAmount, sign: i8) {
        if self.exact_unavailable {
            return;
        }
        let Some(raw_amount) = amount.raw_amount.as_deref() else {
            self.invalidate_exact();
            return;
        };
        let Some(decimals) = amount.decimals else {
            self.invalidate_exact();
            return;
        };
        let Some(parsed_raw) = raw_amount.parse::<u64>().ok() else {
            self.invalidate_exact();
            return;
        };
        let parsed_raw = i128::from(parsed_raw);
        let Some(signed_raw) = parsed_raw.checked_mul(i128::from(sign)) else {
            self.invalidate_exact();
            return;
        };
        match self.decimals {
            Some(existing) if existing != decimals => self.invalidate_exact(),
            Some(_) => {
                if let Some(current) = self.raw_delta {
                    match current.checked_add(signed_raw) {
                        Some(next) => self.raw_delta = Some(next),
                        None => self.invalidate_exact(),
                    }
                } else {
                    self.invalidate_exact();
                }
            }
            None => {
                self.decimals = Some(decimals);
                self.raw_delta = Some(signed_raw);
            }
        }
    }

    fn invalidate_exact(&mut self) {
        self.raw_delta = None;
        self.decimals = None;
        self.exact_unavailable = true;
    }

    fn candidate(&self) -> ParsedUiAmount {
        ParsedUiAmount {
            amount: self.amount_delta.abs(),
            raw_amount: self.raw_delta.map(|value| value.abs().to_string()),
            decimals: self.decimals,
        }
    }
}
