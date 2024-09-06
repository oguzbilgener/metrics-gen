use anyhow::Context;
use indicatif::ProgressStyle;
use tokio::sync::mpsc;
use tracing::{info, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt as _;

use crate::{
    config::{AppConfig, Mode},
    execution::TaskResult,
    metric_file::MetricFile,
    visitor::{Horizontal as HorizontalVisitor, Vertical as VerticalVisitor},
    Args,
};

pub(crate) async fn run(
    args: Args,
    config: AppConfig,
    metric_files: Vec<MetricFile>,
) -> anyhow::Result<()> {
    let total_execution_count: u64 = config
        .labels
        .iter()
        .map(|label| label.count)
        .reduce(|acc, count| {
            let acc: u64 = acc;
            acc * count
        })
        .context("No labels defined")?;

    let header_span = info_span!("run");
    header_span.pb_set_style(&ProgressStyle::default_bar());
    header_span.pb_set_length(total_execution_count);

    let (progress_tx, progress_rx) = mpsc::channel(1);

    let total_execution_count: usize = total_execution_count
        .try_into()
        .context("Too many executions")?;
    let collector_task = tokio::spawn(run_collector(total_execution_count, progress_rx));

    // Generate UUIDs, create Executions, schedule them with limited concurrency
    let label_keys = config.label_keys();
    let label_counts = config.label_counts();

    if config.mode == Mode::Vertical {
        VerticalVisitor::new(
            args.concurrency,
            config,
            metric_files,
            progress_tx,
            label_keys,
            label_counts,
        )
        .visit_all()
        .await?;
    } else {
        HorizontalVisitor::new(
            args.concurrency,
            config,
            metric_files,
            progress_tx,
            label_keys,
            &label_counts,
        )
        .visit_all(total_execution_count)
        .await?;
    }

    collector_task.await??;

    Ok(())
}

async fn run_collector(num_tasks: usize, mut rx: mpsc::Receiver<TaskResult>) -> anyhow::Result<()> {
    tracing::trace!(count = num_tasks, "Running collector");
    let mut collected = 0;
    while collected < num_tasks {
        let res = rx
            .recv()
            .await
            .with_context(|| "Collector channel unexpectedly closed")?;
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
