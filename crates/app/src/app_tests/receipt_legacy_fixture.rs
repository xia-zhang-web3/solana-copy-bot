use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Complete transaction fixture for pre-existing confirmation boundary tests.
/// Values are independent of the code under test; no quote lookup is performed.
pub(super) async fn answer_receipt(
    listener: &tokio::net::TcpListener,
    wallet: &str,
    signature: &str,
    side: &str,
    slot: u64,
    net_lamports: i64,
) {
    answer_receipt_inner(listener, wallet, signature, side, slot, net_lamports, None).await;
}

pub(super) async fn answer_receipt_fee(
    listener: &tokio::net::TcpListener,
    wallet: &str,
    signature: &str,
    side: &str,
    slot: u64,
    net_lamports: i64,
) {
    answer_receipt_inner(
        listener,
        wallet,
        signature,
        side,
        slot,
        net_lamports,
        Some(19000),
    )
    .await;
}

async fn answer_receipt_inner(
    listener: &tokio::net::TcpListener,
    wallet: &str,
    signature: &str,
    side: &str,
    slot: u64,
    net_lamports: i64,
    fee: Option<u64>,
) {
    let (mut socket, _) =
        tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
            .await
            .expect("getTransaction request deadline")
            .expect("getTransaction socket");
    let mut buf = [0; 8192];
    let read = socket.read(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf[..read]);
    assert!(text.contains("\"method\":\"getTransaction\""));
    assert!(text.contains(signature));
    let row = |raw: &str| {
        json!({"accountIndex":1,"owner":wallet,"mint":"TokenMint",
        "uiTokenAmount":{"amount":raw,"decimals":3}})
    };
    let mut value = json!({"result":{"slot":slot,"transaction":{"signatures":[signature],
        "message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":"token-account","signer":false,"writable":true}]}},
        "meta":{"err":null,"preBalances":[2_000_000_000_i64,2_039_280],
            "postBalances":[2_000_000_000_i64 + net_lamports,2_039_280],
            "preTokenBalances":[row(if side == "buy" {"0"} else {"10000"})],
            "postTokenBalances":[row(if side == "buy" {"10000"} else {"0"})]}}});
    if let Some(fee) = fee {
        value["result"]["meta"]["fee"] = json!(fee);
    }
    let body = value.to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
}

pub(super) fn confirmed_slot(body: &str) -> Option<u64> {
    let value: Value = serde_json::from_str(body).ok()?;
    let status = value.pointer("/result/value/0")?;
    if !status.get("err")?.is_null() {
        return None;
    }
    matches!(
        status["confirmationStatus"].as_str(),
        Some("confirmed" | "finalized")
    )
    .then(|| status["slot"].as_u64())
    .flatten()
}

/// Older submit/route tests prepare inventory without running an actual BUY.
/// Supply the known initial result explicitly; generic/import writers retain NULL.
pub(super) fn prepared_inventory_zero(path: &std::path::Path) -> anyhow::Result<()> {
    let changed = rusqlite::Connection::open(path)?.execute(
        "UPDATE positions SET pnl_lamports=0 WHERE state='open' AND token='TokenMint'",
        [],
    )?;
    anyhow::ensure!(changed == 1, "expected one prepared test position");
    Ok(())
}
