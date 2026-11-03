//! The exact client used by prequote must never contact a redirect destination.
#[allow(dead_code)]
#[path = "../src/execution_owned_sell_body.rs"]
mod body;
use anyhow::Result;
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};

#[tokio::test]
async fn finalized_prequote_client_does_not_follow_3xx() -> Result<()> {
    let client = body::closed_client()?;
    for code in [301, 302, 307, 308] {
        let origin = TcpListener::bind("127.0.0.1:0")?;
        let destination = TcpListener::bind("127.0.0.1:0")?;
        destination.set_nonblocking(true)?;
        let url = format!("http://{}/rpc", origin.local_addr()?);
        let location = format!("http://{}/must-not-receive-rpc", destination.local_addr()?);
        let server = std::thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = origin.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut request = [0; 4096];
            socket.read(&mut request)?;
            socket.write_all(format!("HTTP/1.1 {code} Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes())?;
            Ok(())
        });
        let response=client.post(&url).json(&serde_json::json!({"jsonrpc":"2.0","id":"owned-sell-genesis","method":"getGenesisHash"}))
            .timeout(Duration::from_secs(2)).send().await?;
        assert_eq!(response.status().as_u16(), code);
        assert_eq!(response.url().as_str(), url);
        assert_eq!(
            destination.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        server.join().unwrap()?;
    }
    Ok(())
}
