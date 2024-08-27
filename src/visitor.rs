use std::{sync::Arc, time::Instant};

use anyhow::Context as _;
use futures_util::{future::BoxFuture, FutureExt as _};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::{
    config::AppConfig,
    execution::{Execution, RangeIter, RangeIteration, TaskResult},
    metric_file::MetricFile,
};

pub(crate) struct Vertical {
    config: Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskResult>,
    label_keys: Arc<Vec<String>>,
    label_counts: Vec<u64>,
}

impl Vertical {
    pub(crate) fn new(
        concurrency: usize,
        config: AppConfig,
        metric_files: Vec<MetricFile>,
        sender: mpsc::Sender<TaskResult>,
        label_keys: Vec<String>,
        label_counts: Vec<u64>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            metric_files,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender,
            label_keys: Arc::new(label_keys),
            label_counts,
        }
    }

    pub(crate) async fn visit_all(&self) -> anyhow::Result<()> {
        let mut values = Vec::with_capacity(self.label_keys.len());
        self.visit(&mut values, 0).await
    }

    fn visit<'v>(
        &'v self,
        values: &'v mut Vec<String>,
        level: usize,
    ) -> BoxFuture<'v, anyhow::Result<()>> {
        async move {
            if level == self.label_keys.len() {
                let permit = acquire_permit(self.semaphore.clone()).await?;
                let execution = Execution::new(
                    self.config.clone(),
                    self.label_keys.clone(),
                    values.clone(),
                    self.metric_files.clone(),
                    None,
                );
                drop(tokio::spawn(
                    execution.execute_all(self.sender.clone(), permit),
                ));
                return Ok(());
            }
            for _ in 0..self.label_counts[level] {
                values.push(generate_id());
                self.visit(values, level + 1)
                    .await
                    .context("while visiting vertically")?;
                let _ = values.pop();
            }
            Ok(())
        }
        .boxed::<'v>()
    }
}

pub(crate) struct Horizontal {
    config: Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskResult>,
    label_keys: Arc<Vec<String>>,
    all_ids: Vec<Vec<String>>,
}

impl Horizontal {
    pub(crate) fn new(
        concurrency: usize,
        config: AppConfig,
        metric_files: Vec<MetricFile>,
        sender: mpsc::Sender<TaskResult>,
        label_keys: Vec<String>,
        label_counts: &[u64],
    ) -> Self {
        Self {
            config: Arc::new(config),
            metric_files,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender,
            label_keys: Arc::new(label_keys),
            all_ids: Self::generate_all_ids(label_counts),
        }
    }

    pub(crate) async fn visit_all(&self) -> anyhow::Result<()> {
        let start = Instant::now();
        let range_iter = RangeIter::new(
            self.config.start_date,
            self.config.end_date,
            self.config.upload_interval,
        );

        let mut executions = LabelValuesIter::new(&self.all_ids)
            .map(|label_values| {
                Execution::new(
                    self.config.clone(),
                    self.label_keys.clone(),
                    label_values,
                    self.metric_files.clone(),
                    Some(self.semaphore.clone()),
                )
            })
            .collect::<Vec<Execution>>();

        for iteration in range_iter {
            match iteration {
                RangeIteration::Generate(current_date) => {
                    executions.iter_mut().try_for_each(|execution| {
                        execution
                            .generate(current_date)
                            .context("when generating data")
                    })?;
                }
                RangeIteration::Upload(current_date) => {
                    for execution in &mut executions {
                        let res = execution
                            .upload(current_date)
                            .await
                            .context("while uploading");
                        if let Err(e) = res {
                            execution.failed = true;
                            self.sender
                                .send(TaskResult {
                                    elapsed: start.elapsed(),
                                    error: Some(e),
                                })
                                .await?;
                        }
                    }
                }
            }
        }

        for _execution in executions.iter().filter(|ex| !ex.failed) {
            self.sender
                .send(TaskResult {
                    elapsed: start.elapsed(),
                    error: None,
                })
                .await?;
        }

        Ok(())
    }

    fn generate_all_ids(label_counts: &[u64]) -> Vec<Vec<String>> {
        let mut all_ids = vec![];
        for _ in 0..label_counts.len() {
            all_ids.push(vec![]);
        }
        for (i, &count) in label_counts.iter().enumerate() {
            for _ in 0..count {
                all_ids[i].push(generate_id());
            }
        }
        all_ids
    }
}

fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

async fn acquire_permit(semaphore: Arc<Semaphore>) -> anyhow::Result<OwnedSemaphorePermit> {
    semaphore
        .acquire_owned()
        .await
        .with_context(|| "could not acquire a permit")
}

struct LabelValuesIter<'v> {
    all_ids: &'v [Vec<String>],
    current: Vec<usize>,
}

impl<'v> LabelValuesIter<'v> {
    fn new(all_ids: &'v [Vec<String>]) -> Self {
        Self {
            all_ids,
            current: vec![0; all_ids.len()],
        }
    }

    fn is_done(&self) -> bool {
        self.current.iter().all(|&i| i == self.all_ids.len())
    }
}

impl<'v> Iterator for LabelValuesIter<'v> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done() {
            return None;
        }
        Some(
            self.current
                .iter()
                .enumerate()
                .map(|(i, &j)| self.all_ids[i][j].clone())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_all_ids() {
        let all_ids = vec![2, 3, 4];
        let result = Horizontal::generate_all_ids(&all_ids);
        assert_eq!(result.len(), 3);
        for (i, ids) in result.iter().enumerate() {
            assert_eq!(ids.len() as u64, all_ids[i]);
        }
    }
}
