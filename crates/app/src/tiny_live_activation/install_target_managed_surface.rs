use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

const WRAPPER_RELATIVE_PATH: &str = "usr/local/bin/copybot-live-service-control";
const TARGET_CONFIG_RELATIVE_PATH: &str = "etc/solana-copy-bot/live.server.toml";
const RUNTIME_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/runtime";
const BACKUP_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/backups";
const SESSION_DIR_RELATIVE_PATH: &str = "var/lib/solana-copy-bot/tiny-live/sessions";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallTargetManagedSurfacePaths {
    pub wrapper_path: PathBuf,
    pub wrapper_parent: PathBuf,
    pub target_config_path: PathBuf,
    pub target_config_parent: PathBuf,
    pub runtime_dir: PathBuf,
    pub backup_dir: PathBuf,
    pub session_dir: PathBuf,
}

pub fn derive_install_target_managed_surface_paths(
    install_root: &Path,
) -> Result<InstallTargetManagedSurfacePaths> {
    let wrapper_path = install_root.join(WRAPPER_RELATIVE_PATH);
    let target_config_path = install_root.join(TARGET_CONFIG_RELATIVE_PATH);
    Ok(InstallTargetManagedSurfacePaths {
        wrapper_parent: wrapper_path
            .parent()
            .ok_or_else(|| {
                anyhow!(
                    "derived wrapper path {} has no parent",
                    wrapper_path.display()
                )
            })?
            .to_path_buf(),
        target_config_parent: target_config_path
            .parent()
            .ok_or_else(|| {
                anyhow!(
                    "derived target config path {} has no parent",
                    target_config_path.display()
                )
            })?
            .to_path_buf(),
        wrapper_path,
        target_config_path,
        runtime_dir: install_root.join(RUNTIME_DIR_RELATIVE_PATH),
        backup_dir: install_root.join(BACKUP_DIR_RELATIVE_PATH),
        session_dir: install_root.join(SESSION_DIR_RELATIVE_PATH),
    })
}

pub fn validate_turn_green_session_dir(install_root: &Path, session_dir: &Path) -> Result<()> {
    if !install_root.is_absolute() {
        bail!("frozen install root must be absolute");
    }
    if !session_dir.is_absolute() {
        bail!("session dir must be absolute");
    }
    let managed_surface = derive_install_target_managed_surface_paths(install_root)?;
    for (label, path) in [
        ("live wrapper path", managed_surface.wrapper_path.as_path()),
        (
            "live wrapper parent",
            managed_surface.wrapper_parent.as_path(),
        ),
        (
            "live target config path",
            managed_surface.target_config_path.as_path(),
        ),
        (
            "live target config parent",
            managed_surface.target_config_parent.as_path(),
        ),
        ("live runtime dir", managed_surface.runtime_dir.as_path()),
        ("live backup dir", managed_surface.backup_dir.as_path()),
        ("live session dir", managed_surface.session_dir.as_path()),
    ] {
        validate_disjoint_from_managed_path(session_dir, path, "session dir", label)?;
    }
    Ok(())
}

fn validate_disjoint_from_managed_path(
    candidate: &Path,
    managed_path: &Path,
    candidate_label: &str,
    managed_label: &str,
) -> Result<()> {
    let candidate_anchor = find_existing_anchor(candidate, candidate_label)?;
    reject_anchor_symlink(&candidate_anchor, candidate_label)?;
    reject_descendant_symlinks(candidate, &candidate_anchor, candidate_label)?;
    if candidate.starts_with(managed_path) || managed_path.starts_with(candidate) {
        bail!(
            "{candidate_label} {} must stay disjoint from {managed_label} {}",
            candidate.display(),
            managed_path.display()
        );
    }
    let resolved_candidate = resolved_non_symlink_path_identity(candidate, candidate_label)?;
    let resolved_managed = resolved_host_path_identity(managed_path, managed_label)?;
    if resolved_candidate.starts_with(&resolved_managed)
        || resolved_managed.starts_with(&resolved_candidate)
    {
        bail!(
            "{candidate_label} {} must stay disjoint from {managed_label} {} after resolving path identity",
            candidate.display(),
            managed_path.display()
        );
    }
    Ok(())
}

fn reject_anchor_symlink(anchor: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(anchor).with_context(|| {
        format!(
            "failed reading metadata for {label} anchor {}",
            anchor.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        bail!("{label} anchor {} must not be a symlink", anchor.display());
    }
    Ok(())
}

fn resolved_non_symlink_path_identity(path: &Path, label: &str) -> Result<PathBuf> {
    let anchor = find_existing_anchor(path, label)?;
    reject_anchor_symlink(&anchor, label)?;
    reject_descendant_symlinks(path, &anchor, label)?;
    build_resolved_path_identity(path, &anchor, label)
}

fn resolved_host_path_identity(path: &Path, label: &str) -> Result<PathBuf> {
    let anchor = find_existing_anchor(path, label)?;
    build_resolved_path_identity(path, &anchor, label)
}

fn build_resolved_path_identity(path: &Path, anchor: &Path, label: &str) -> Result<PathBuf> {
    let resolved_anchor = fs::canonicalize(anchor)
        .with_context(|| format!("failed canonicalizing {label} anchor {}", anchor.display()))?;
    let relative = path
        .strip_prefix(anchor)
        .with_context(|| format!("failed stripping {label} anchor {}", anchor.display()))?;
    if relative.as_os_str().is_empty() {
        Ok(resolved_anchor)
    } else {
        Ok(resolved_anchor.join(relative))
    }
}

fn find_existing_anchor(path: &Path, label: &str) -> Result<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Ok(current);
        }
        if !current.pop() {
            bail!("{label} {} has no existing parent anchor", path.display());
        }
    }
}

fn reject_descendant_symlinks(path: &Path, anchor: &Path, label: &str) -> Result<()> {
    let relative = path
        .strip_prefix(anchor)
        .with_context(|| format!("failed stripping {label} anchor {}", anchor.display()))?;
    let mut current = anchor.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        if current.exists() {
            let metadata = fs::symlink_metadata(&current)
                .with_context(|| format!("failed reading metadata for {}", current.display()))?;
            if metadata.file_type().is_symlink() {
                bail!(
                    "{label} {} must not traverse symlinked path component {}",
                    path.display(),
                    current.display()
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn disjoint_root_session_dir_is_allowed() {
        let base = temp_dir("managed_surface_root_ok");
        let session_dir = base.join("turn-green-session");
        validate_turn_green_session_dir(Path::new("/"), &session_dir).unwrap();
    }

    #[test]
    fn overlap_with_managed_session_dir_is_rejected() {
        let managed = derive_install_target_managed_surface_paths(Path::new("/")).unwrap();
        let session_dir = managed.session_dir.join("turn-green-session");
        let error = validate_turn_green_session_dir(Path::new("/"), &session_dir).unwrap_err();
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
