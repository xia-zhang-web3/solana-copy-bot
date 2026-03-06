use anyhow::{anyhow, Context, Result};
use std::net::IpAddr;

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

#[cfg(test)]
mod tests {
    use super::validate_endpoint_url;

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
}
