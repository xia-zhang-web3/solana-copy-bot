use anyhow::{anyhow, Context, Result};

pub(crate) const MAX_HTTP_ERROR_BODY_DETAIL_CHARS: usize = 1024;
pub(crate) const MAX_HTTP_ERROR_BODY_READ_BYTES: usize = 4096;
pub(crate) const MAX_HTTP_JSON_BODY_READ_BYTES: usize = 64 * 1024;

pub(crate) struct ReadResponseBody {
    pub(crate) text: String,
    pub(crate) was_truncated: bool,
}

pub(crate) fn validate_endpoint_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).context("invalid URL parse")?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!("unsupported scheme {}", parsed.scheme()));
    }
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

pub(crate) fn truncate_detail_chars(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if let Some((cutoff_idx, _)) = value.char_indices().nth(max_chars) {
        return format!("{}...[truncated]", &value[..cutoff_idx]);
    }
    value.to_string()
}

pub(crate) async fn read_response_body_limited(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> ReadResponseBody {
    if max_bytes == 0 {
        return ReadResponseBody {
            text: String::new(),
            was_truncated: false,
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
                        was_truncated: false,
                    };
                }
                let mut partial = String::from_utf8_lossy(body_bytes.as_slice()).to_string();
                partial.push_str(format!("...[body_read_failed:{}]", error_class).as_str());
                return ReadResponseBody {
                    text: partial,
                    was_truncated: false,
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
        was_truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::{read_response_body_limited, truncate_detail_chars};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn truncate_detail_chars_keeps_short_text_unchanged() {
        assert_eq!(truncate_detail_chars("short", 16), "short");
    }

    #[test]
    fn truncate_detail_chars_truncates_long_text_with_marker() {
        let input = "abcdefghij";
        let output = truncate_detail_chars(input, 5);
        assert_eq!(output, "abcde...[truncated]");
        assert!(!output.contains("fghij"));
    }

    #[test]
    fn truncate_detail_chars_preserves_utf8_boundaries() {
        let input = "a🙂b🙂c";
        let output = truncate_detail_chars(input, 3);
        assert_eq!(output, "a🙂b...[truncated]");
    }

    #[tokio::test]
    async fn read_response_body_limited_truncates_large_http_body() {
        let long_body = "z".repeat(5000);
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            // Some CI/container environments may deny local bind for test processes.
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
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    long_body.len(),
                    long_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{}/", addr))
            .send()
            .await
            .expect("send request");
        assert_eq!(response.status(), 503);

        let body = read_response_body_limited(response, 128).await;
        assert!(body.was_truncated, "truncated flag should be true");
        assert!(body.text.contains("...[truncated]"), "body={}", body.text);
        assert!(body.text.len() >= 128, "body length={}", body.text.len());
        let _ = handle.join();
    }
}
