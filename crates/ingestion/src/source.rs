use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{self, Interval};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct RawSwapObservation {
    pub signature: String,
    pub slot: u64,
    pub signer: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub program_ids: Vec<String>,
    pub dex_hint: String,
    pub ts_utc: DateTime<Utc>,
}

pub enum IngestionSource {
    Mock(MockSource),
    HeliusWs(HeliusWsSource),
}

impl IngestionSource {
    pub fn from_config(config: &IngestionConfig) -> Result<Self> {
        match config.source.to_lowercase().as_str() {
            "mock" => Ok(Self::Mock(MockSource::new(
                config.mock_interval_ms,
                config
                    .raydium_program_ids
                    .first()
                    .cloned()
                    .unwrap_or_default(),
            ))),
            "helius" | "helius_ws" => Ok(Self::HeliusWs(HeliusWsSource::new(config)?)),
            other => Err(anyhow!("unknown ingestion.source: {other}")),
        }
    }

    pub async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        match self {
            Self::Mock(source) => source.next_observation().await,
            Self::HeliusWs(source) => source.next_observation().await,
        }
    }
}

pub struct MockSource {
    interval: Interval,
    sequence: u64,
    raydium_program_id: String,
}

impl MockSource {
    pub fn new(interval_ms: u64, raydium_program_id: String) -> Self {
        Self {
            interval: time::interval(Duration::from_millis(interval_ms.max(100))),
            sequence: 0,
            raydium_program_id,
        }
    }

    async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        self.interval.tick().await;
        self.sequence = self.sequence.saturating_add(1);
        let n = self.sequence;

        Ok(Some(RawSwapObservation {
            signature: format!("mock-sig-{n}"),
            slot: 1_000_000 + n,
            signer: "MockLeaderWallet1111111111111111111111111111".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("MockTokenMint{n}"),
            amount_in: 0.5,
            amount_out: 1_000.0 + (n as f64),
            program_ids: vec![self.raydium_program_id.clone()],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        }))
    }
}

struct LogsNotification {
    signature: String,
    slot: u64,
    logs: Vec<String>,
    is_failed: bool,
}

type HeliusWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

pub struct HeliusWsSource {
    ws_url: String,
    http_url: String,
    reconnect_initial_ms: u64,
    reconnect_max_ms: u64,
    next_backoff_ms: u64,
    tx_fetch_retries: u32,
    tx_fetch_retry_delay_ms: u64,
    seen_signatures_limit: usize,
    request_id: u64,
    ws: Option<HeliusWsStream>,
    http_client: Client,
    interested_program_ids: HashSet<String>,
    raydium_program_ids: HashSet<String>,
    pumpswap_program_ids: HashSet<String>,
    seen_signatures_queue: VecDeque<String>,
    seen_signatures_set: HashSet<String>,
}

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

        Ok(Self {
            ws_url: config.helius_ws_url.clone(),
            http_url: config.helius_http_url.clone(),
            reconnect_initial_ms: config.reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .reconnect_max_ms
                .max(config.reconnect_initial_ms.max(200)),
            next_backoff_ms: config.reconnect_initial_ms.max(200),
            tx_fetch_retries: config.tx_fetch_retries,
            tx_fetch_retry_delay_ms: config.tx_fetch_retry_delay_ms.max(50),
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            request_id: 1000,
            ws: None,
            http_client,
            interested_program_ids,
            raydium_program_ids: config.raydium_program_ids.iter().cloned().collect(),
            pumpswap_program_ids: config.pumpswap_program_ids.iter().cloned().collect(),
            seen_signatures_queue: VecDeque::new(),
            seen_signatures_set: HashSet::new(),
        })
    }

    async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        loop {
            if self.ws.is_none() {
                if let Err(error) = self.connect_ws().await {
                    warn!(error = ?error, "helius ws connect failed");
                    self.sleep_with_backoff().await;
                    continue;
                }
            }

            let next_message = {
                let ws = self.ws.as_mut().expect("ws checked above");
                ws.next().await
            };

            match next_message {
                Some(Ok(Message::Text(text))) => {
                    if let Some(notification) = self.parse_logs_notification(&text) {
                        if notification.is_failed {
                            continue;
                        }
                        if self.is_seen_signature(&notification.signature) {
                            continue;
                        }
                        self.mark_seen_signature(notification.signature.clone());

                        if let Some(raw) = self.fetch_swap_with_retries(notification).await? {
                            self.next_backoff_ms = self.reconnect_initial_ms;
                            return Ok(Some(raw));
                        }
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if let Some(ws) = self.ws.as_mut() {
                        if let Err(error) = ws.send(Message::Pong(payload)).await {
                            warn!(error = %error, "failed to send ws pong");
                            self.ws = None;
                            self.sleep_with_backoff().await;
                        }
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    warn!(?frame, "helius ws closed");
                    self.ws = None;
                    self.sleep_with_backoff().await;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    warn!(error = %error, "helius ws stream error");
                    self.ws = None;
                    self.sleep_with_backoff().await;
                }
                None => {
                    warn!("helius ws stream ended");
                    self.ws = None;
                    self.sleep_with_backoff().await;
                }
            }
        }
    }

    async fn connect_ws(&mut self) -> Result<()> {
        if self.ws_url.contains("REPLACE_ME") || self.http_url.contains("REPLACE_ME") {
            return Err(anyhow!(
                "configure ingestion.helius_ws_url and ingestion.helius_http_url with real API key"
            ));
        }

        let (mut ws, _response) = connect_async(&self.ws_url)
            .await
            .with_context(|| format!("failed connecting to {}", self.ws_url))?;

        for program_id in self.interested_program_ids.iter() {
            self.request_id = self.request_id.saturating_add(1);
            let request = json!({
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [program_id]},
                    {"commitment": "confirmed"}
                ]
            });
            ws.send(Message::Text(request.to_string().into()))
                .await
                .with_context(|| format!("failed sending logsSubscribe for {}", program_id))?;
        }

        info!(
            ws_url = %self.ws_url,
            programs = self.interested_program_ids.len(),
            "helius ws connected and subscriptions sent"
        );
        self.ws = Some(ws);
        Ok(())
    }

    fn parse_logs_notification(&self, text: &str) -> Option<LogsNotification> {
        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(error) => {
                debug!(error = %error, "skipping invalid ws message json");
                return None;
            }
        };

        if let (Some(id), Some(result)) = (value.get("id"), value.get("result")) {
            if id.is_number() && result.is_number() {
                debug!(id = ?id, subscription = ?result, "logsSubscribe acknowledged");
            }
            return None;
        }

        let method = value.get("method").and_then(Value::as_str)?;
        if method != "logsNotification" {
            return None;
        }

        let params = value.get("params")?;
        let result = params.get("result")?;
        let context = result.get("context")?;
        let event = result.get("value")?;

        let signature = event.get("signature")?.as_str()?.to_string();
        let slot = context
            .get("slot")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let logs = event
            .get("logs")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let is_failed = event.get("err").map(|err| !err.is_null()).unwrap_or(false);

        Some(LogsNotification {
            signature,
            slot,
            logs,
            is_failed,
        })
    }

    async fn fetch_swap_with_retries(
        &self,
        notification: LogsNotification,
    ) -> Result<Option<RawSwapObservation>> {
        for attempt in 0..=self.tx_fetch_retries {
            match self
                .fetch_swap_from_signature(
                    &notification.signature,
                    notification.slot,
                    &notification.logs,
                )
                .await
            {
                Ok(Some(raw)) => return Ok(Some(raw)),
                Ok(None) => {}
                Err(error) => warn!(
                    error = %error,
                    signature = %notification.signature,
                    attempt,
                    "tx fetch attempt failed"
                ),
            }

            if attempt < self.tx_fetch_retries {
                time::sleep(Duration::from_millis(self.tx_fetch_retry_delay_ms)).await;
            }
        }
        Ok(None)
    }

    async fn fetch_swap_from_signature(
        &self,
        signature: &str,
        slot_hint: u64,
        logs_hint: &[String],
    ) -> Result<Option<RawSwapObservation>> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });

        let response: Value = self
            .http_client
            .post(&self.http_url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("failed getTransaction POST for {}", signature))?
            .error_for_status()
            .with_context(|| format!("non-success getTransaction status for {}", signature))?
            .json()
            .await
            .with_context(|| format!("failed parsing getTransaction json for {}", signature))?;

        if response.get("error").is_some() {
            debug!(signature, error = ?response.get("error"), "rpc returned error");
            return Ok(None);
        }

        let result = match response.get("result") {
            Some(value) if !value.is_null() => value,
            _ => return Ok(None),
        };
        let meta = match result.get("meta") {
            Some(value) if !value.is_null() => value,
            _ => return Ok(None),
        };
        if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
            return Ok(None);
        }

        let account_keys = Self::extract_account_keys(result);
        if account_keys.is_empty() {
            return Ok(None);
        }
        let signer_index = account_keys
            .iter()
            .position(|(_, is_signer)| *is_signer)
            .unwrap_or(0);
        let signer = account_keys
            .get(signer_index)
            .map(|(pubkey, _)| pubkey.clone())
            .ok_or_else(|| anyhow!("missing signer in parsed account keys"))?;

        let mut program_ids = Self::extract_program_ids(result, meta, logs_hint);
        if !program_ids
            .iter()
            .any(|program| self.interested_program_ids.contains(program))
        {
            return Ok(None);
        }
        if program_ids.is_empty() {
            program_ids.extend(self.interested_program_ids.iter().cloned());
        }

        let (token_in, amount_in, token_out, amount_out) =
            match Self::infer_swap_from_json_balances(meta, &account_keys, signer_index, &signer) {
                Some(value) => value,
                None => return Ok(None),
            };

        let block_time = result.get("blockTime").and_then(Value::as_i64);
        let ts_utc = block_time
            .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
            .unwrap_or_else(Utc::now);
        let slot = result
            .get("slot")
            .and_then(Value::as_u64)
            .unwrap_or(slot_hint);
        let logs = Self::value_to_string_vec(meta.get("logMessages"))
            .unwrap_or_else(|| logs_hint.to_vec());
        let dex_hint = self.detect_dex_hint(&program_ids, &logs);

        Ok(Some(RawSwapObservation {
            signature: signature.to_string(),
            slot,
            signer,
            token_in,
            token_out,
            amount_in,
            amount_out,
            program_ids: program_ids.into_iter().collect(),
            dex_hint,
            ts_utc,
        }))
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

    fn extract_program_ids(result: &Value, meta: &Value, logs_hint: &[String]) -> HashSet<String> {
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

        for log in logs_hint.iter().chain(
            Self::value_to_string_vec(meta.get("logMessages"))
                .unwrap_or_default()
                .iter(),
        ) {
            if let Some(program_id) = Self::extract_program_id_from_log(log) {
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
        account_keys: &[(String, bool)],
        signer_index: usize,
        signer: &str,
    ) -> Option<(String, f64, String, f64)> {
        let mut mint_deltas: HashMap<String, f64> = HashMap::new();

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
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                *mint_deltas.entry(mint).or_default() -= amount;
            }
        }
        for item in post {
            if item.get("owner").and_then(Value::as_str) == Some(signer) {
                let mint = item.get("mint").and_then(Value::as_str)?.to_string();
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                *mint_deltas.entry(mint).or_default() += amount;
            }
        }

        let mut token_in: Option<(String, f64)> = None;
        let mut token_out: Option<(String, f64)> = None;
        for (mint, delta) in mint_deltas {
            if delta < -1e-12 {
                let value = delta.abs();
                if token_in.as_ref().map(|(_, v)| value > *v).unwrap_or(true) {
                    token_in = Some((mint, value));
                }
            } else if delta > 1e-12 {
                if token_out.as_ref().map(|(_, v)| delta > *v).unwrap_or(true) {
                    token_out = Some((mint, delta));
                }
            }
        }

        if let (Some((in_mint, in_amt)), Some((out_mint, out_amt))) = (&token_in, &token_out) {
            if in_mint != out_mint {
                return Some((in_mint.clone(), *in_amt, out_mint.clone(), *out_amt));
            }
        }

        let pre_sol = meta
            .get("preBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)
            .map(|lamports| lamports as f64 / 1_000_000_000.0)?;
        let post_sol = meta
            .get("postBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)
            .map(|lamports| lamports as f64 / 1_000_000_000.0)?;
        let sol_delta = post_sol - pre_sol;

        if let Some((out_mint, out_amt)) = token_out {
            if sol_delta < -1e-8 {
                return Some((SOL_MINT.to_string(), sol_delta.abs(), out_mint, out_amt));
            }
        }
        if let Some((in_mint, in_amt)) = token_in {
            if sol_delta > 1e-8 {
                return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_delta));
            }
        }

        if account_keys.is_empty() {
            return None;
        }
        None
    }

    fn parse_ui_amount_json(ui_amount: Option<&Value>) -> Option<f64> {
        let ui_amount = ui_amount?;
        if let Some(amount) = ui_amount.get("uiAmountString").and_then(Value::as_str) {
            return amount.parse::<f64>().ok();
        }
        if let Some(amount) = ui_amount.get("uiAmount").and_then(Value::as_f64) {
            return Some(amount);
        }
        let raw = ui_amount.get("amount").and_then(Value::as_str)?;
        let decimals = ui_amount.get("decimals").and_then(Value::as_u64)?;
        if decimals > 18 {
            return None;
        }
        let raw = raw.parse::<f64>().ok()?;
        Some(raw / 10f64.powi(decimals as i32))
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

    fn detect_dex_hint(&self, program_ids: &HashSet<String>, logs: &[String]) -> String {
        if program_ids
            .iter()
            .any(|program| self.raydium_program_ids.contains(program))
            || logs
                .iter()
                .any(|log| log.to_ascii_lowercase().contains("raydium"))
        {
            return "raydium".to_string();
        }
        if program_ids
            .iter()
            .any(|program| self.pumpswap_program_ids.contains(program))
            || logs
                .iter()
                .any(|log| log.to_ascii_lowercase().contains("pump"))
        {
            return "pumpswap".to_string();
        }
        "unknown".to_string()
    }

    fn is_seen_signature(&self, signature: &str) -> bool {
        self.seen_signatures_set.contains(signature)
    }

    fn mark_seen_signature(&mut self, signature: String) {
        if self.seen_signatures_set.insert(signature.clone()) {
            self.seen_signatures_queue.push_back(signature);
        }
        while self.seen_signatures_queue.len() > self.seen_signatures_limit {
            if let Some(removed) = self.seen_signatures_queue.pop_front() {
                self.seen_signatures_set.remove(&removed);
            }
        }
    }

    async fn sleep_with_backoff(&mut self) {
        let delay = self
            .next_backoff_ms
            .clamp(self.reconnect_initial_ms, self.reconnect_max_ms);
        time::sleep(Duration::from_millis(delay)).await;
        self.next_backoff_ms = (delay.saturating_mul(2)).min(self.reconnect_max_ms);
    }
}
