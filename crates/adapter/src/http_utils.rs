use anyhow::{Context, Result, anyhow};
use std::net::IpAddr;

pub(crate) const MAX_HTTP_ERROR_BODY_READ_BYTES: usize = 4096;
pub(crate) const MAX_HTTP_JSON_BODY_READ_BYTES: usize = 64 * 1024;

pub(crate) struct ReadResponseBody {
    pub(crate) text: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) was_truncated: bool,
    pub(crate) read_error_class: Option<&'static str>,
}

pub(crate) fn validate_endpoint_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).context("invalid URL parse")?;
    validate_endpoint_scheme(&parsed)?;
    if parsed.host_str().is_none() {
        return Err(anyhow!("host missing"));
    }
    if parsed.username().len() > 0 || parsed.password().is_some() {
        return Err(anyhow!("URL credentials are not allowed"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(anyhow!("query/fragment are not allowed"));
    }
    Ok(())
}

fn validate_endpoint_scheme(parsed: &reqwest::Url) -> Result<()> {
    let scheme = parsed.scheme().to_ascii_lowercase();
    match scheme.as_str() {
        "https" => Ok(()),
        "http" if endpoint_host_is_local(parsed) => Ok(()),
        "http" => Err(anyhow!(
            "http scheme is allowed only for localhost/loopback endpoints"
        )),
        _ => Err(anyhow!("unsupported scheme {}", parsed.scheme())),
    }
}

fn endpoint_host_is_local(parsed: &reqwest::Url) -> bool {
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let normalized = host
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if normalized == "localhost" || normalized.ends_with(".localhost") {
        return true;
    }
    normalized
        .parse::<IpAddr>()
        .map(|addr| addr.is_loopback())
        .unwrap_or(false)
}

pub(crate) fn endpoint_identity(url: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(url).context("invalid URL parse")?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!("unsupported scheme {}", parsed.scheme()));
    }
    let host = parsed.host_str().ok_or_else(|| anyhow!("host missing"))?;
    let port = parsed.port_or_known_default().unwrap_or(0);
    let mut path = parsed.path().trim().to_string();
    if path.is_empty() {
        path = "/".to_string();
    }
    Ok(format!(
        "{}://{}:{}{}",
        scheme,
        host.to_ascii_lowercase(),
        port,
        path
    ))
}

pub(crate) fn redacted_endpoint_label(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return "unknown".to_string();
    }
    match reqwest::Url::parse(endpoint) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("unknown");
            match url.port() {
                Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
                None => format!("{}://{}", url.scheme(), host),
            }
        }
        Err(_) => "invalid_endpoint".to_string(),
    }
}

pub(crate) fn classify_request_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_redirect() {
        "redirect"
    } else if error.is_status() {
        "status"
    } else {
        "other"
    }
}

pub(crate) async fn read_response_body_limited(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> ReadResponseBody {
    if max_bytes == 0 {
        return ReadResponseBody {
            text: String::new(),
            bytes: Vec::new(),
            was_truncated: false,
            read_error_class: None,
        };
    }

    let mut body_bytes: Vec<u8> = Vec::with_capacity(max_bytes.min(1024));
    let mut was_truncated = false;

    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body_bytes.len() >= max_bytes {
                    was_truncated = true;
                    break;
                }
                let remaining = max_bytes.saturating_sub(body_bytes.len());
                if chunk.len() > remaining {
                    body_bytes.extend_from_slice(&chunk[..remaining]);
                    was_truncated = true;
                    break;
                }
                body_bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(error) => {
                let error_class = classify_request_error(&error);
                if body_bytes.is_empty() {
                    return ReadResponseBody {
                        text: format!("response body read failed class={}", error_class),
                        bytes: Vec::new(),
                        was_truncated: false,
                        read_error_class: Some(error_class),
                    };
                }
                let mut partial = String::from_utf8_lossy(body_bytes.as_slice()).to_string();
                partial.push_str(format!("...[body_read_failed:{}]", error_class).as_str());
                return ReadResponseBody {
                    text: partial,
                    bytes: body_bytes,
                    was_truncated: false,
                    read_error_class: Some(error_class),
                };
            }
        }
    }

    let mut text = String::from_utf8_lossy(body_bytes.as_slice()).to_string();
    if was_truncated {
        text.push_str("...[truncated]");
    }
    ReadResponseBody {
        text,
        bytes: body_bytes,
        was_truncated,
        read_error_class: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_HTTP_JSON_BODY_READ_BYTES, read_response_body_limited, validate_endpoint_url};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn validate_endpoint_url_rejects_plaintext_non_loopback_endpoint() {
        let error = validate_endpoint_url("http://rpc.example.com/upstream")
            .expect_err("external plaintext endpoint must reject");
        assert!(
            error
                .to_string()
                .contains("http scheme is allowed only for localhost/loopback endpoints"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_endpoint_url_allows_https_and_loopback_http() {
        validate_endpoint_url("https://rpc.example.com/upstream")
            .expect("https endpoint must be allowed");
        validate_endpoint_url("http://127.0.0.1:8080/upstream")
            .expect("loopback IPv4 endpoint must be allowed");
        validate_endpoint_url("http://localhost:8080/upstream")
            .expect("localhost endpoint must be allowed");
        validate_endpoint_url("http://[::1]:8080/upstream")
            .expect("loopback IPv6 endpoint must be allowed");
    }

    #[tokio::test]
    async fn read_response_body_limited_truncates_large_http_body() {
        let long_body = "z".repeat(MAX_HTTP_JSON_BODY_READ_BYTES + 1024);
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let Ok(addr) = listener.local_addr() else {
            return;
        };
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 2048];
                let _ = stream.read(&mut request_buf);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    long_body.len(),
                    long_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{}/body", addr))
            .send()
            .await
            .expect("request should succeed");
        let body = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
        assert!(body.was_truncated, "body must report truncation");
        assert_eq!(body.bytes.len(), MAX_HTTP_JSON_BODY_READ_BYTES);
        assert!(body.text.ends_with("...[truncated]"), "body={}", body.text);

        let _ = handle.join();
    }
}
