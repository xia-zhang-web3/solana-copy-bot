//! Bounded loopback HTTP. No tasks are detached; callers join and check server and runner.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub(crate) struct Request {
    socket: TcpStream,
    pub path: String,
    pub body: Value,
    pub received: DateTime<Utc>,
}
#[derive(Debug)]
pub(crate) struct Capture {
    pub path: String,
    pub body: Value,
    pub received: DateTime<Utc>,
    pub replied: DateTime<Utc>,
}
impl Request {
    pub fn pump(&self) -> bool {
        self.path.starts_with("/pump-fun/quote?")
    }
    pub fn query(&self, key: &str) -> String {
        reqwest::Url::parse(&format!("http://localhost{}", self.path))
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == key)
            .unwrap()
            .1
            .into_owned()
    }
    pub fn quote(&self) -> Value {
        let pump = self.pump();
        let sell = if pump {
            self.query("type") == "SELL"
        } else {
            self.query("outputMint") == super::b58_fixture::SOL
        };
        let token = self.query(if pump {
            "mint"
        } else if sell {
            "inputMint"
        } else {
            "outputMint"
        });
        let decimals = if token == "mint" { 3 } else { 6 };
        let output = match (sell, pump) {
            (true, true) => "300000000",
            (true, false) => "200000000",
            (false, true) => "2000000",
            (false, false) => "1000000",
        };
        let mut quote = json!({"inAmount":self.query("amount"),"outAmount":output,
            "routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]});
        if pump {
            quote["meta"] = json!({"isCompleted":false,"inDecimals":if sell {decimals} else {9},"outDecimals":if sell {9} else {decimals}});
            json!({"quote":quote})
        } else {
            quote
        }
    }
    pub async fn reply(mut self, status: u16, body: Value) -> Result<Capture> {
        // Different response times exercise payload/timing correlation, not an overlap timing threshold.
        let raw = body.to_string();
        let replied = Utc::now();
        let response = format!("HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{raw}",raw.len());
        tokio::time::timeout(Duration::from_secs(1), async {
            self.socket.write_all(response.as_bytes()).await?;
            self.socket.shutdown().await
        })
        .await??;
        Ok(Capture {
            path: self.path,
            body: self.body,
            received: self.received,
            replied,
        })
    }
    pub async fn cancelled(mut self) -> Result<()> {
        let mut byte = [0];
        let read =
            tokio::time::timeout(Duration::from_secs(1), self.socket.read(&mut byte)).await?;
        match read {
            Ok(0) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => Ok(()),
            other => {
                anyhow::bail!("pending HTTP must be cancelled after authority refusal: {other:?}")
            }
        }
    }
}
pub(crate) async fn accept(listener: &TcpListener) -> Result<Request> {
    let (mut socket, peer) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .context("barrier request missing before response")??;
    ensure!(peer.ip().is_loopback());
    let (path, body) = tokio::time::timeout(
        Duration::from_secs(1),
        super::rpc_simulation_http_fixture::read(&mut socket),
    )
    .await??;
    Ok(Request {
        socket,
        path,
        body,
        received: Utc::now(),
    })
}
pub(crate) async fn pair(listener: &TcpListener) -> Result<Vec<Request>> {
    let a = accept(listener).await?;
    let b = accept(listener).await?;
    ensure!(
        a.pump() != b.pump(),
        "both independent quote endpoints required"
    );
    ensure!([&a, &b].iter().all(|r| r.body.is_null()));
    Ok(vec![a, b])
}
pub(crate) async fn no_more(listener: &TcpListener) -> Result<()> {
    ensure!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err(),
        "unexpected extra HTTP/RPC"
    );
    Ok(())
}
#[derive(Default)]
pub(crate) struct Replies {
    pub generic_error: bool,
    pub pump_error: bool,
    pub pump_first: bool,
    pub retry: bool,
    pub generic_only: bool,
    pub decimals: Vec<Option<u8>>,
    pub pump_completed: bool,
    pub generic_fee: bool,
}
pub(crate) async fn serve(listener: TcpListener, options: Replies) -> Result<Vec<Capture>> {
    let mut requests = if options.generic_only {
        vec![accept(&listener).await?]
    } else {
        pair(&listener).await?
    };
    requests.sort_by_key(|r| r.pump() != options.pump_first);
    let mut captures = Vec::new();
    for r in requests {
        let pump = r.pump();
        let mut quote = r.quote();
        if pump && options.pump_completed {
            quote["quote"]["meta"]["isCompleted"] = json!(true);
        }
        if !pump && options.generic_fee {
            quote["platformFee"] = json!({"feeBps":100,"amount":"1"});
        }
        let (status, body) = if !pump && options.retry {
            (400, json!({"error":"TOKEN_NOT_TRADABLE"}))
        } else if if pump {
            options.pump_error
        } else {
            options.generic_error
        } {
            (
                503,
                json!({"error":if pump {"pump unavailable"} else {"generic unavailable"}}),
            )
        } else {
            (200, quote)
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        captures.push(r.reply(status, body).await?);
    }
    if options.retry {
        let r = accept(&listener).await?;
        ensure!(!r.pump());
        let body = r.quote();
        captures.push(r.reply(200, body).await?);
    }
    for decimals in options.decimals {
        let r = accept(&listener).await?;
        ensure!(r.body["method"] == "getTokenSupply");
        captures.push(
            r.reply(
                200,
                json!({"jsonrpc":"2.0","result":{"value":{"decimals":decimals}}}),
            )
            .await?,
        );
    }
    no_more(&listener).await?;
    Ok(captures)
}
