use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;
use tokio::{io::AsyncReadExt, net::TcpStream};

const HEADER_LIMIT: usize = 8 * 1024;
const BODY_LIMIT: usize = 64 * 1024;

pub(super) async fn read_request(stream: &mut TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    let (start, length) = loop {
        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            ensure!(end <= HEADER_LIMIT, "receipt fixture headers too large");
            let headers =
                std::str::from_utf8(&bytes[..end]).context("receipt fixture header UTF-8")?;
            ensure!(
                headers.lines().next() == Some("POST / HTTP/1.1"),
                "receipt fixture expected POST / HTTP/1.1"
            );
            let mut length = None;
            for line in headers.lines().skip(1) {
                let (key, value) = line
                    .split_once(':')
                    .context("receipt fixture malformed header")?;
                ensure!(
                    !key.eq_ignore_ascii_case("transfer-encoding"),
                    "receipt fixture transfer encoding unsupported"
                );
                if key.eq_ignore_ascii_case("content-length") {
                    ensure!(length.is_none(), "receipt fixture duplicate content length");
                    length = Some(
                        value
                            .trim()
                            .parse::<usize>()
                            .context("receipt fixture content length")?,
                    );
                }
            }
            let length = length.context("receipt fixture missing content length")?;
            ensure!(length <= BODY_LIMIT, "receipt fixture body too large");
            break (end + 4, length);
        }
        ensure!(
            bytes.len() <= HEADER_LIMIT,
            "receipt fixture headers too large"
        );
        read_more(stream, &mut bytes).await?;
    };
    while bytes.len() < start + length {
        read_more(stream, &mut bytes).await?;
    }
    ensure!(
        bytes.len() == start + length,
        "receipt fixture trailing request bytes"
    );
    let request: Value =
        serde_json::from_slice(&bytes[start..]).context("receipt fixture request JSON")?;
    ensure!(
        request["method"].as_str().is_some(),
        "receipt fixture missing method"
    );
    Ok(request)
}

async fn read_more(stream: &mut TcpStream, bytes: &mut Vec<u8>) -> Result<()> {
    let mut buf = [0; 4096];
    let n = stream
        .read(&mut buf)
        .await
        .context("receipt fixture request read")?;
    if n == 0 {
        bail!("receipt fixture EOF before complete request");
    }
    bytes.extend_from_slice(&buf[..n]);
    Ok(())
}
