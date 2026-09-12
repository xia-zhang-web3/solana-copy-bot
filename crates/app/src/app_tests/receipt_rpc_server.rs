use super::{
    receipt_rpc_fixture::{Fault, ServerPolicy, Shared},
    receipt_rpc_request::read_request,
};
use anyhow::{bail, ensure, Context, Result};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
};

pub(super) async fn serve(
    listener: TcpListener,
    mut stop: oneshot::Receiver<()>,
    shared: Shared,
    policy: ServerPolicy,
) -> Result<()> {
    let mut handlers = JoinSet::new();
    let mut errors = Vec::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => break,
            joined = handlers.join_next(), if !handlers.is_empty() => record(joined.unwrap(), &mut errors),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(error) => { errors.push(format!("receipt fixture accept: {error}")); break; }
                };
                let shared = shared.clone();
                let active = shared.tasks.handler();
                handlers.spawn(async move {
                    let _active = active;
                    handle(stream, shared, policy).await
                });
            }
        }
    }
    drop(listener);
    while let Some(joined) = handlers.join_next().await {
        record(joined, &mut errors);
    }
    ensure!(
        errors.is_empty(),
        "receipt fixture server failures: {}",
        errors.join("; ")
    );
    ensure!(
        shared.cancellations_requested.load(Ordering::SeqCst)
            == shared.cancellations_completed.load(Ordering::SeqCst),
        "receipt fixture expected cancellation did not complete"
    );
    ensure!(
        shared.fault.lock().unwrap().is_none(),
        "receipt fixture fault injection was not consumed"
    );
    Ok(())
}
fn record(joined: Result<Result<()>, tokio::task::JoinError>, errors: &mut Vec<String>) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => errors.push(format!("{error:#}")),
        Err(error) => errors.push(format!("receipt fixture handler join: {error}")),
    }
}
async fn handle(mut stream: TcpStream, shared: Shared, policy: ServerPolicy) -> Result<()> {
    let request = tokio::time::timeout(policy.io, read_request(&mut stream))
        .await
        .context("receipt fixture request timeout")??;
    let method = request["method"]
        .as_str()
        .context("receipt fixture missing method")?;
    shared.calls.lock().unwrap().push(method.into());
    let (code, body, delay) = match method {
        "getSignatureStatuses" => (200, shared.status.lock().unwrap().to_string(), 0),
        "getTokenAccountsByOwner" => {
            let value = if request["params"][1]["programId"]
                == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            {
                shared.accounts.lock().unwrap().clone()
            } else {
                json!({"result":{"value":[]}})
            };
            (200, value.to_string(), 0)
        }
        "getTransaction" | "sendTransaction" => shared.response.lock().unwrap().clone(),
        other => bail!("receipt fixture unexpected RPC method {other}"),
    };
    if method == "getTransaction" {
        let fault = shared.fault.lock().unwrap().take();
        match fault {
            Some(Fault::Panic) => panic!("synthetic receipt child panic"),
            Some(Fault::Error) => bail!("synthetic receipt child error"),
            None => {}
        }
        if shared.cancel_next.swap(false, Ordering::SeqCst) {
            // Only this explicitly designated, fully parsed request permits peer cancellation.
            // Await EOF/reset instead of relying on sleep followed by an ignored write error.
            let mut byte = [0];
            let read = tokio::time::timeout(policy.io, stream.read(&mut byte))
                .await
                .context("receipt fixture expected cancellation timeout")?;
            match read {
                Ok(0) => {}
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
                Ok(_) => bail!("receipt fixture expected EOF, received more request bytes"),
                Err(e) => return Err(e).context("receipt fixture cancellation read"),
            }
            shared
                .cancellations_completed
                .fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
    }
    ensure!(
        delay <= 2000,
        "receipt fixture delay exceeds bounded policy"
    );
    tokio::time::sleep(Duration::from_millis(delay)).await;
    let reply = format!(
        "HTTP/1.1 {code} Test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    tokio::time::timeout(policy.io, stream.write_all(reply.as_bytes()))
        .await
        .context("receipt fixture response write timeout")?
        .context("receipt fixture response write")?;
    Ok(())
}
