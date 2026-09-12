//! Complete, cryptographically checked loopback BUY transport for legacy route tests.
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

async fn read(listener: &TcpListener) -> (TcpStream, String, Value) {
    let (mut stream, _) =
        tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
            .await
            .expect("tiny fixture request deadline")
            .expect("tiny fixture socket");
    let (path, body) = super::rpc_simulation_http_fixture::read(&mut stream)
        .await
        .unwrap();
    (stream, path, body)
}
async fn reply(mut stream: TcpStream, value: Value) {
    let body = value.to_string();
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
}

pub(super) async fn serve(
    payer: [u8; 32],
    raw: u64,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let (stream, path, _) = read(&listener).await;
        assert!(path.starts_with("/quote?"));
        let query: std::collections::HashMap<_, _> =
            reqwest::Url::parse(&format!("http://localhost{path}"))
                .unwrap()
                .query_pairs()
                .into_owned()
                .collect();
        assert_eq!(query["amount"], "10000000");
        let quote = json!({"inputMint":query["inputMint"],"outputMint":query["outputMint"],"inAmount":"10000000", "outAmount":raw.to_string(),"otherAmountThreshold":(raw*95/100).to_string(),"swapMode":"ExactIn","slippageBps":query["slippageBps"].parse::<u64>().unwrap(),"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Metis"}}]});
        reply(stream, quote.clone()).await;
        let (stream, path, request) = read(&listener).await;
        assert_eq!(path, "/swap-instructions");
        assert_eq!(request["quoteResponse"], quote);
        assert_eq!(request["userPublicKey"], bs58::encode(payer).into_string());
        assert_eq!(request["prioritizationFeeLamports"], 22000);
        reply(
            stream,
            super::tiny_transport_fixture::bundle(payer, [9; 32], 10_000_000),
        )
        .await;
        let (stream, _, request) = read(&listener).await;
        assert_eq!(request["method"], "simulateTransaction");
        assert_eq!(request["params"][1]["sigVerify"], false);
        let simulated = crate::execution_transaction_wire::decode_message(
            request["params"][0].as_str().unwrap(),
            |_| Ok(()),
        )
        .unwrap();
        reply(stream, json!({"jsonrpc":"2.0","id":request["id"],"result":{"context":{"slot":42},"value":{"err":null,"logs":[]}}})).await;
        let mut methods = Vec::new();
        for _ in 0..4 {
            let (stream, _, request) = read(&listener).await;
            let method = request["method"].as_str().unwrap();
            assert!([
                "getFeeForMessage",
                "getMultipleAccounts",
                "getMinimumBalanceForRentExemption"
            ]
            .contains(&method));
            if method == "getFeeForMessage" {
                assert_eq!(
                    request["params"][0],
                    STANDARD.encode(&simulated.binding.message_bytes)
                );
            }
            methods.push(method.to_owned());
            reply(
                stream,
                super::tiny_transport_fixture::funding().reply(&request),
            )
            .await;
        }
        assert_eq!(methods.last().unwrap(), "getFeeForMessage");
        methods.sort();
        assert_eq!(
            methods,
            [
                "getFeeForMessage",
                "getFeeForMessage",
                "getMinimumBalanceForRentExemption",
                "getMultipleAccounts"
            ]
        );
        let (stream, _, request) = read(&listener).await;
        assert_eq!(request["method"], "sendTransaction");
        let bytes = STANDARD
            .decode(request["params"][0].as_str().unwrap())
            .unwrap();
        assert_eq!(bytes[65..], simulated.binding.message_bytes);
        let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65]).unwrap();
        ed25519_dalek::VerifyingKey::from_bytes(&payer)
            .unwrap()
            .verify_strict(&bytes[65..], &signature)
            .unwrap();
        let signature = bs58::encode(signature.to_bytes()).into_string();
        reply(
            stream,
            json!({"jsonrpc":"2.0","id":request["id"],"result":signature}),
        )
        .await;
        let (stream, _, request) = read(&listener).await;
        assert_eq!(request["method"], "getSignatureStatuses");
        assert_eq!(request["params"][0][0], signature);
        reply(stream, json!({"result":{"value":[{"slot":42,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}})).await;
        let (stream, _, request) = read(&listener).await;
        assert_eq!(request["method"], "getTransaction");
        assert_eq!(request["params"][0], signature);
        let wallet = bs58::encode(payer).into_string();
        let row = |amount: u64| json!({"accountIndex":1,"owner":wallet,"mint":"TokenMint","uiTokenAmount":{"amount":amount.to_string(),"decimals":3}});
        reply(stream, json!({"result":{"slot":42,"transaction":{"signatures":[signature],"message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},{"pubkey":"token-account","signer":false,"writable":true}]}},"meta":{"err":null,"fee":100000,"preBalances":[2000000000u64,2039280],"postBalances":[1989900000u64,2039280],"preTokenBalances":[row(0)],"postTokenBalances":[row(raw)]}}})).await;
    });
    Ok((url, task))
}

pub(super) fn assert_dispatch(
    store: &copybot_storage_core::SqliteStore,
    order: &copybot_storage_core::ExecutionCanaryOrder,
) -> Result<()> {
    let dispatch = store
        .load_execution_canary_dispatch(&order.order_id)?
        .expect("actual local send must have durable binding");
    assert_eq!(
        order.tx_signature.as_deref(),
        Some(dispatch.tx_signature.as_str())
    );
    assert_eq!(order.attempt, dispatch.attempt);
    Ok(())
}
