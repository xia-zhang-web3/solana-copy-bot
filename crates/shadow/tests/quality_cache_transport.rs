#[path = "quality_cache_support/rpc.rs"]
mod rpc;

use anyhow::{ensure, Context, Result};
use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

#[test]
fn rpc_fixture_waits_for_segmented_request_body() -> Result<()> {
    let server = rpc::Rpc::start(true)?;
    let response = (|| -> Result<()> {
        for method in ["getProgramAccounts", "getSignaturesForAddress"] {
            let mut stream = TcpStream::connect(
                server
                    .url
                    .strip_prefix("http://")
                    .context("local HTTP URL")?,
            )?;
            stream.set_read_timeout(Some(Duration::from_secs(1)))?;
            stream.set_write_timeout(Some(Duration::from_secs(1)))?;
            let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":[]}}"#);
            write!(
                stream,
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )?;
            // A valid split TCP request must wait for its body within the existing
            // 500 ms socket deadline, even when the listener is nonblocking.
            thread::sleep(Duration::from_millis(100));
            stream.write_all(body.as_bytes())?;
            let mut reply = String::new();
            stream.read_to_string(&mut reply)?;
            ensure!(reply.starts_with("HTTP/1.1 200 OK\r\n"), "{reply}");
            let payload = reply.split_once("\r\n\r\n").context("HTTP body")?.1;
            let value: serde_json::Value = serde_json::from_str(payload)?;
            ensure!(value["result"].as_array().is_some(), "{value}");
        }
        Ok(())
    })();
    // Surface the server result first, so an EAGAIN is not hidden by a client EOF.
    let methods = server.finish().context("checked RPC server completion")?;
    response?;
    assert_eq!(methods, ["getProgramAccounts", "getSignaturesForAddress"]);
    Ok(())
}
