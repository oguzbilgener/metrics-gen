use anyhow::Context;
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub(crate) type Metrics = MetricsExposition<PrometheusType, PrometheusValue>;

#[derive(Debug, Clone)]
pub(crate) struct MetricFile {
    pub(crate) path: PathBuf,
    pub(crate) exposition: Metrics,
}

impl MetricFile {
    pub(crate) async fn load_from_path(path: PathBuf) -> anyhow::Result<MetricFile> {
        let file_contents = tokio::fs::read_to_string(path.as_path())
            .await
            .with_context(|| format!("Could not read file at path {}", path.display()))?;
        match openmetrics_parser::prometheus::parse_prometheus(&file_contents) {
            Ok(exposition) => Ok(MetricFile { path, exposition }),
            Err(err) => anyhow::bail!(
                "Could not parse Prometheus exposition from file at path {}: {:?}",
                path.display(),
                err
            ),
        }
    }
}

pub(crate) async fn load_metric_files(
    root: &Path,
    ignore: &[String],
) -> anyhow::Result<Vec<MetricFile>> {
    let mut metric_files = Vec::new();
    for entry in WalkDir::new(root) {
        let entry = entry.context("could not get dir entry")?;
        let path = entry.path();
        if !path.is_file() || ignore.iter().any(|i| path.ends_with(i)) {
            continue;
        }
        let metric_file = MetricFile::load_from_path(path.to_path_buf()).await?;
        metric_files.push(metric_file);
    }

    Ok(metric_files)
}

pub(crate) fn validate_metrics_path(path: &str) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(path);
    if !path.exists() {
        anyhow::bail!("Path {} does not exist", path.display());
    }
    if !path.is_dir() {
        anyhow::bail!("Path {} is not a directory", path.display());
    }

    Ok(path)
}
