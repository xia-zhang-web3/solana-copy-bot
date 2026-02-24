use anyhow::{anyhow, Context, Result};

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
