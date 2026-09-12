#![allow(dead_code)]
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub const CLASSIC: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

#[derive(Clone, Debug)]
pub struct Account {
    pub id: u8,
    pub mint: u8,
    pub raw: u64,
    pub response: Quote,
}
#[derive(Clone, Copy, Debug)]
pub enum Quote {
    Known(u64),
    MissingAmount,
    Error,
    NoRoute,
    ThresholdError,
    Invalid(&'static str),
    Missing(&'static str),
}
impl Account {
    pub fn row(&self) -> Value {
        json!({"pubkey":key(self.id),"account":{"owner":CLASSIC,"data":{"parsed":{"type":"account","info":{"mint":key(self.mint),"owner":key(99),"tokenAmount":{"amount":self.raw.to_string(),"decimals":0,"uiAmountString":self.raw.to_string()}}}}}})
    }
}

pub struct Server {
    pub url: String,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<Vec<Value>>>>,
}
impl Server {
    pub fn start(accounts: Vec<Account>) -> Result<Self> {
        Self::with_fault(accounts, "")
    }
    pub fn with_fault(accounts: Vec<Account>, fault: &'static str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}", listener.local_addr()?);
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut captures = Vec::new();
            while !flag.load(Ordering::SeqCst) {
                ensure!(
                    Instant::now() < deadline,
                    "loopback listener deadline exceeded"
                );
                match listener.accept() {
                    Ok((mut stream, peer)) => {
                        ensure!(peer.ip().is_loopback());
                        let request = read_request(&mut stream)?;
                        let (status, body, pair) = respond(&request, &accounts, fault)?;
                        captures.push(json!({"request":request,"response_status":status,"response":body,"paired_account":pair}));
                        let raw = serde_json::to_vec(&body)?;
                        write!(stream,"HTTP/1.1 {status} audit\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",raw.len())?;
                        stream.write_all(&raw)?;
                        stream.flush()?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(captures)
        });
        Ok(Self {
            url,
            stop,
            handle: Some(handle),
        })
    }
    pub fn finish(mut self) -> Result<Vec<Value>> {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow!("HTTP handler panicked"))?
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
fn read_request(stream: &mut TcpStream) -> Result<Value> {
    // Accepted sockets can inherit the listener nonblocking mode on macOS.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut bytes = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        ensure!(
            Instant::now() < deadline && bytes.len() < 65536,
            "HTTP input bound"
        );
        let n = stream.read(&mut buf)?;
        ensure!(n > 0, "early HTTP EOF");
        bytes.extend_from_slice(&buf[..n]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end])?;
            let length = headers
                .lines()
                .filter_map(|l| l.split_once(':'))
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.trim().parse::<usize>())
                .transpose()?
                .unwrap_or(0);
            if bytes.len() < end + 4 + length {
                continue;
            }
            let mut first = headers
                .lines()
                .next()
                .context("request line")?
                .split_whitespace();
            let method = first.next().context("method")?;
            let path = first.next().context("path")?;
            let body = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&bytes[end + 4..end + 4 + length])?
            };
            return Ok(json!({"method":method,"path":path,"body":body}));
        }
    }
}
fn respond(req: &Value, accounts: &[Account], fault: &str) -> Result<(u16, Value, Value)> {
    if req["method"] == "POST" {
        ensure!(req["path"] == "/rpc");
        let body = &req["body"];
        ensure!(body["params"][0] == key(99), "unexpected RPC owner");
        let result = match body["method"].as_str() {
            Some("getBalance") => json!({"value":5_000_000_000u64}),
            Some("getTokenAccountsByOwner") => {
                let program = body["params"][1]["programId"].as_str().context("program")?;
                ensure!([CLASSIC, TOKEN2022].contains(&program));
                if fault == "partial_rpc" && program == TOKEN2022 {
                    return Ok((
                        503,
                        json!({"error":{"code":-1,"message":"synthetic unavailable"}}),
                        Value::Null,
                    ));
                }
                let mut rows: Vec<_> = accounts.iter().map(Account::row).collect();
                if program == CLASSIC && !rows.is_empty() {
                    let pointer = match fault {
                        "invalid_account" => Some("/pubkey"),
                        "invalid_mint" => Some("/account/data/parsed/info/mint"),
                        "invalid_raw" => Some("/account/data/parsed/info/tokenAmount/amount"),
                        "missing_decimals" => {
                            Some("/account/data/parsed/info/tokenAmount/decimals")
                        }
                        "wrong_owner" => Some("/account/data/parsed/info/owner"),
                        "wrong_program" => Some("/account/owner"),
                        _ => None,
                    };
                    if let Some(ptr) = pointer {
                        *rows[0].pointer_mut(ptr).unwrap() = if fault == "missing_decimals" {
                            Value::Null
                        } else {
                            json!("invalid")
                        };
                    }
                    if fault == "duplicate" {
                        rows.push(rows[0].clone());
                    }
                }
                json!({"value":if program==CLASSIC {rows} else {vec![]}})
            }
            other => bail!("unexpected RPC method {other:?}"),
        };
        return Ok((
            200,
            json!({"jsonrpc":"2.0","id":body["id"],"result":result}),
            Value::Null,
        ));
    }
    ensure!(req["method"] == "GET");
    let url = url::Url::parse(&format!(
        "http://127.0.0.1{}",
        req["path"].as_str().context("path")?
    ))?;
    ensure!(url.path() == "/quote");
    let params = url
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    ensure!(params.get("outputMint").map(|s| s.as_ref()) == Some(SOL));
    let matching: Vec<_> = accounts
        .iter()
        .filter(|a| {
            params.get("inputMint").is_some_and(|v| v == &key(a.mint))
                && params
                    .get("amount")
                    .is_some_and(|v| v == &a.raw.to_string())
        })
        .collect();
    ensure!(
        matching.len() == 1,
        "HTTP quote must map to exactly one synthetic account: {req}"
    );
    let a = matching[0];
    let (status, body) = match a.response {
        Quote::Known(out) => (
            200,
            json!({"inputMint":key(a.mint),"outputMint":SOL,"inAmount":a.raw.to_string(),"outAmount":out.to_string(),"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":format!("account-{}-amount-{}",a.id,a.raw)}}]}),
        ),
        Quote::Invalid(field) => {
            let mut v = json!({"inputMint":key(a.mint),"outputMint":SOL,"inAmount":a.raw.to_string(),"outAmount":"123"});
            v[field] = if field == "outAmount" {
                json!("-1")
            } else if field == "inAmount" {
                json!("999999")
            } else {
                json!(key(98))
            };
            (200, v)
        }
        Quote::Missing(field) => {
            let mut v = json!({"inputMint":key(a.mint),"outputMint":SOL,"inAmount":a.raw.to_string(),"outAmount":"123"});
            v.as_object_mut().unwrap().remove(field);
            (200, v)
        }
        Quote::ThresholdError => (
            400,
            json!({"errorCode":"CANNOT_COMPUTE_OTHER_AMOUNT_THRESHOLD"}),
        ),
        Quote::MissingAmount => (
            200,
            json!({"inputMint":key(a.mint),"outputMint":SOL,"inAmount":a.raw.to_string(),"routePlan":[]}),
        ),
        Quote::Error => (
            503,
            json!({"error":"synthetic quote unavailable","errorCode":"AUDIT_UNAVAILABLE"}),
        ),
        Quote::NoRoute => (
            400,
            json!({"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}),
        ),
    };
    Ok((
        status,
        body,
        json!({"token_account":key(a.id),"mint":key(a.mint),"amount_raw":a.raw.to_string(),"pairing_basis":"unique mint+amount from captured RPC row; HTTP has no account parameter"}),
    ))
}
