use crate::execution_canary_quote_pnl::Cli;
use anyhow::{anyhow, Context, Result};
use copybot_config::{load_from_path, AppConfig};
use std::path::{Path, PathBuf};

pub(crate) struct ReportContext {
    pub(crate) config: Option<AppConfig>,
    pub(crate) db_path: PathBuf,
}

pub(crate) fn resolve_report_context(cli: &Cli) -> Result<ReportContext> {
    if let Some(config_path) = &cli.config_path {
        let config = load_from_path(config_path)
            .with_context(|| format!("failed to load config: {}", config_path.display()))?;
        let db_path = cli
            .db_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(config.sqlite.path.clone()));
        return Ok(ReportContext {
            config: Some(config),
            db_path,
        });
    }

    let db_path = cli
        .db_path
        .clone()
        .ok_or_else(|| anyhow!("either --config or --db-path is required"))?;
    Ok(ReportContext {
        config: None,
        db_path,
    })
}

pub(crate) fn runtime_root_from_db_path(db_path: &Path) -> Option<PathBuf> {
    let parent = db_path.parent()?;
    if parent.file_name().and_then(|name| name.to_str()) == Some("state") {
        return parent.parent().map(Path::to_path_buf);
    }
    Some(parent.to_path_buf())
}
