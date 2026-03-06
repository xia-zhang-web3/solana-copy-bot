use anyhow::{anyhow, Context, Result};
use std::fs;
use zeroize::Zeroizing;

use crate::secret_value::SecretValue;

pub(crate) fn resolve_secret_source(
    inline_name: &str,
    inline_value: Option<&str>,
    file_name: &str,
    file_value: Option<&str>,
) -> Result<Option<SecretValue>> {
    let inline_secret = inline_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .map(SecretValue::from);
    let file_path = file_value.map(str::trim).filter(|value| !value.is_empty());

    if inline_secret.is_some() && file_path.is_some() {
        return Err(anyhow!(
            "{} and {} cannot both be set",
            inline_name,
            file_name
        ));
    }

    if let Some(path) = file_path {
        let secret = read_trimmed_secret_file(path)
            .with_context(|| format!("{} invalid file source path={}", file_name, path))?;
        return Ok(Some(secret));
    }

    Ok(inline_secret)
}

fn read_trimmed_secret_file(path: &str) -> Result<SecretValue> {
    let raw = Zeroizing::new(
        fs::read_to_string(path)
            .with_context(|| format!("secret file not found/readable path={}", path))?,
    );
    ensure_secret_file_has_restrictive_permissions(path)?;
    let secret = raw.trim().to_string();
    if secret.is_empty() {
        return Err(anyhow!("secret file is empty path={}", path));
    }
    Ok(secret.into())
}

fn ensure_secret_file_has_restrictive_permissions(path: &str) -> Result<()> {
    if !secret_file_has_restrictive_permissions(path)? {
        return Err(anyhow!(
            "secret file must use owner-only permissions (0600/0400) path={}",
            path
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn secret_file_has_restrictive_permissions(path: &str) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        fs::metadata(path).with_context(|| format!("secret file stat failed path={}", path))?;
    Ok((metadata.permissions().mode() & 0o077) == 0)
}

#[cfg(not(unix))]
pub(crate) fn secret_file_has_restrictive_permissions(_path: &str) -> Result<bool> {
    Ok(true)
}
