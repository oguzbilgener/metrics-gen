use std::sync::Arc;

use anyhow::Context;
use futures_util::{future::BoxFuture, FutureExt as _};
use indicatif::ProgressStyle;
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tracing::{info, info_span, trace, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt as _;

use crate::{
    config::AppConfig, generate::SampleGenerator, metric_file::MetricFile, upload::upload_metrics,
    Args,
};

struct Execution {
    /// Keys for the predefined labels in the config
    label_keys: Arc<Vec<String>>,
    /// Generated values for the predefined labels
    label_values: Vec<String>,
    config: Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    _permit: OwnedSemaphorePermit,
}

pub(crate) async fn run(
    args: Args,
    config: AppConfig,
    metric_files: Vec<MetricFile>,
) -> anyhow::Result<()> {
    let total_execution_count: u64 = config.labels.iter().map(|label| label.count).sum();

    let header_span = info_span!("run");
    header_span.pb_set_style(&ProgressStyle::default_bar());
    header_span.pb_set_length(total_execution_count);

    let (progress_tx, progress_rx) = mpsc::channel(1);
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    let total_execution_count: usize = total_execution_count
        .try_into()
        .context("Too many executions")?;
    let collector_task = tokio::spawn(run_collector(total_execution_count, progress_rx));

    // Generate UUIDs, create Executions, schedule them with limited concurrency
    let label_keys = Arc::new(
        config
            .labels
            .iter()
            .map(|label| label.name.clone())
            .collect::<Vec<String>>(),
    );

    let label_counts = config
        .labels
        .iter()
        .map(|label| label.count)
        .collect::<Vec<u64>>();

    let mut values = Vec::with_capacity(label_keys.len());

    let config = Arc::new(config);

    visit(
        &mut values,
        0,
        &label_keys,
        &label_counts,
        &config,
        metric_files,
        semaphore,
        progress_tx,
    )
    .await?;

    collector_task.await??;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn visit<'v>(
    values: &'v mut Vec<String>,
    level: usize,
    label_keys: &'v Arc<Vec<String>>,
    label_counts: &'v Vec<u64>,
    config: &'v Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskResult>,
) -> BoxFuture<'v, anyhow::Result<()>> {
    async move {
        if level == label_keys.len() {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .with_context(|| "could not acquire a permit")?;
            let execution = Execution {
                label_keys: label_keys.clone(),
                label_values: values.clone(),
                config: config.clone(),
                metric_files,
                _permit: permit,
            };
            drop(tokio::spawn(execution.execute(sender)));
            return Ok(());
        }
        for _ in 0..label_counts[level] {
            let id = uuid::Uuid::new_v4().to_string();
            values.push(id);
            visit(
                values,
                level + 1,
                label_keys,
                label_counts,
                config,
                metric_files.clone(),
                semaphore.clone(),
                sender.clone(),
            )
            .await?;
            let _ = values.pop();
        }
        Ok(())
    }
    .boxed::<'v>()
}

impl Execution {
    async fn execute(self, sender: mpsc::Sender<TaskResult>) -> anyhow::Result<()> {
        let start = Instant::now();
        let res = self.execute_inner().await;
        let elapsed = start.elapsed();
        let error = res.err();
        sender.send(TaskResult { elapsed, error }).await?;
        Ok(())
    }

    async fn execute_inner(self) -> anyhow::Result<()> {
        let start = self.config.start_date;
        let mut current_date = start;
        let mut last_upload = start;
        let upload_interval = chrono::Duration::from_std(self.config.upload_interval)?;
        let mut generator =
            SampleGenerator::new(self.config.randomization.clone(), self.metric_files);

        let client = reqwest::Client::new();

        while current_date < self.config.end_date {
            if current_date - last_upload >= upload_interval {
                // Sleep for the upload cooldown period to avoid overwhelming the server
                if self.config.upload_cooldown > std::time::Duration::ZERO {
                    tokio::time::sleep(self.config.upload_cooldown).await;
                }
                trace!(
                    file_count = generator.file_count(),
                    sample_count = generator.sample_count(),
                    current_date = %current_date.to_rfc3339(),
                    last_upload = %last_upload.to_rfc3339(),
                    "Uploading metrics"
                );
                upload_metrics(
                    &self.config,
                    &client,
                    &self.label_keys,
                    &self.label_values,
                    generator.iter_metrics_groups(),
                )
                .await?;
                last_upload = current_date;
                generator.reset();
            }
            generator.generate(current_date)?;
            current_date += self.config.generation_period;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct TaskResult {
    elapsed: std::time::Duration,
    error: Option<anyhow::Error>,
}

async fn run_collector(num_tasks: usize, mut rx: mpsc::Receiver<TaskResult>) -> anyhow::Result<()> {
    tracing::trace!("Running collector");
    let mut collected = 0;
    while collected < num_tasks {
        let res = rx
            .recv()
            .await
            .with_context(|| "Collector channel closed")?;
        if let Some(err) = res.error {
            tracing::error!(%err, "Task failed");
        } else {
            tracing::trace!(elapsed = ?res.elapsed, "Task completed");
        }

        // TODO: report the errors, elapsed times, render a histogram even.

        collected += 1;
        Span::current().pb_inc(1);
    }

    info!("All tasks completed");
    Ok(())
}
