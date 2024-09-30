use std::{sync::Arc, time::Instant};

use anyhow::Context as _;
use futures_util::{future::BoxFuture, FutureExt as _};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::{
    config::AppConfig,
    execution::{Execution, RangeIter, RangeIteration, RealtimeIter, TaskResult},
    metric_file::MetricFile,
    Command,
};

pub(crate) struct Vertical {
    command: Command,
    config: Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskResult>,
    label_keys: Arc<Vec<String>>,
    label_counts: Vec<u64>,
}

impl Vertical {
    pub(crate) fn new(
        command: Command,
        concurrency: usize,
        config: AppConfig,
        metric_files: Vec<MetricFile>,
        sender: mpsc::Sender<TaskResult>,
        label_keys: Vec<String>,
        label_counts: Vec<u64>,
    ) -> Self {
        Self {
            command,
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
                    self.command,
                    0,
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
    command: Command,
    config: Arc<AppConfig>,
    metric_files: Vec<MetricFile>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskResult>,
    label_keys: Arc<Vec<String>>,
    all_ids: Vec<Vec<String>>,
}

impl Horizontal {
    pub(crate) fn new(
        command: Command,
        concurrency: usize,
        config: AppConfig,
        metric_files: Vec<MetricFile>,
        sender: mpsc::Sender<TaskResult>,
        label_keys: Vec<String>,
        label_counts: &[u64],
    ) -> Self {
        Self {
            command,
            metric_files,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender,
            label_keys: Arc::new(label_keys),
            all_ids: Self::get_ids(&config, label_counts),
            config: Arc::new(config),
        }
    }

    pub(crate) async fn visit_all(&self, expected_execution_count: usize) -> anyhow::Result<()> {
        let start = Instant::now();
        let range_iter: Box<dyn Iterator<Item = RangeIteration>> = match self.command {
            Command::Backfill => Box::new(RealtimeIter::new(
                self.config.generation_period,
                self.config.upload_interval,
            )),
            Command::Realtime => Box::new(RangeIter::new(
                self.config.start_date,
                self.config.end_date,
                self.config.generation_period,
                self.config.upload_interval,
            )),
        };

        let mut executions = LabelValuesIter::new(&self.all_ids)
            .enumerate()
            .map(|(order, label_values)| {
                Execution::new(
                    self.command,
                    order,
                    self.config.clone(),
                    self.label_keys.clone(),
                    label_values,
                    self.metric_files.clone(),
                    Some(self.semaphore.clone()),
                )
            })
            .collect::<Vec<Execution>>();

        assert!(executions.len() == expected_execution_count);

        for iteration in range_iter {
            match iteration {
                RangeIteration::SleepUntil(until) => {
                    tokio::time::sleep_until(until).await;
                }
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

    fn get_ids(config: &AppConfig, label_counts: &[u64]) -> Vec<Vec<String>> {
        config
            .provided_labels()
            .unwrap_or_else(|| Self::generate_all_ids(label_counts))
    }

    fn generate_all_ids(label_counts: &[u64]) -> Vec<Vec<String>> {
        let mut all_ids = vec![];
        for _ in 0..label_counts.len() {
            all_ids.push(vec![]);
        }
        let mut total_count = 1;
        for (i, &count) in label_counts.iter().enumerate() {
            total_count *= count;
            for _ in 0..total_count {
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
        assert!(!all_ids.is_empty(), "all_ids must not be empty");
        Self {
            all_ids,
            current: vec![0; all_ids.len()],
        }
    }
}

impl<'v> Iterator for LabelValuesIter<'v> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current[0] >= self.all_ids[0].len() {
            return None;
        }
        let answer = Some(
            self.current
                .iter()
                .enumerate()
                .map(|(i, &c)| self.all_ids[i][c].clone())
                .collect(),
        );
        let last = self.current.len() - 1;
        self.current[last] += 1;
        let total_executions = self.all_ids.last().unwrap().len();
        for i in (0..self.current.len()).rev() {
            if i > 0 && self.current[last] % (total_executions / self.all_ids[i - 1].len()) == 0 {
                self.current[i - 1] += 1;
            }
        }
        answer
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
        let mut count = 1;
        for (i, ids) in result.iter().enumerate() {
            count *= all_ids[i];
            assert_eq!(ids.len() as u64, count);
        }
    }

    #[test]
    fn test_generate_all_ids_less() {
        let all_ids = vec![1, 2, 2];
        let result = Horizontal::generate_all_ids(&all_ids);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[1].len(), 2);
        assert_eq!(result[2].len(), 4);

        let iter = LabelValuesIter::new(&result);
        assert_eq!(iter.count(), 4);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_label_values_iter() {
        // 2 * 3 * 4 = 24
        let all_ids = [
            vec!["!", "?"],
            vec!["1", "2", "3", "4", "5", "6"],
            vec![
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s", "t", "u", "v", "w", "x", "y",
            ],
        ];
        let all_ids: Vec<Vec<String>> = all_ids
            .iter()
            .map(|ids| ids.iter().map(ToString::to_string).collect())
            .collect();

        let mut iter = LabelValuesIter::new(&all_ids);
        assert_eq!(iter.next(), Some(vec!["!".into(), "1".into(), "a".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "1".into(), "b".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "1".into(), "c".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "1".into(), "d".into()]));

        assert_eq!(iter.next(), Some(vec!["!".into(), "2".into(), "e".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "2".into(), "f".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "2".into(), "g".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "2".into(), "h".into()]));

        assert_eq!(iter.next(), Some(vec!["!".into(), "3".into(), "i".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "3".into(), "j".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "3".into(), "k".into()]));
        assert_eq!(iter.next(), Some(vec!["!".into(), "3".into(), "l".into()]));

        assert_eq!(iter.next(), Some(vec!["?".into(), "4".into(), "m".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "4".into(), "n".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "4".into(), "o".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "4".into(), "p".into()]));

        assert_eq!(iter.next(), Some(vec!["?".into(), "5".into(), "q".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "5".into(), "r".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "5".into(), "s".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "5".into(), "t".into()]));

        assert_eq!(iter.next(), Some(vec!["?".into(), "6".into(), "u".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "6".into(), "v".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "6".into(), "w".into()]));
        assert_eq!(iter.next(), Some(vec!["?".into(), "6".into(), "x".into()]));
        assert_eq!(iter.next(), None);
    }
}
