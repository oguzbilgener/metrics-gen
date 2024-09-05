use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context as _;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tracing::trace;

use crate::{
    config::AppConfig, generate::SampleGenerator, metric_file::MetricFile, upload::upload_metrics,
};

pub(crate) struct RangeIter {
    end_date: DateTime<Utc>,
    generate_interval: TimeDelta,
    upload_interval: TimeDelta,

    last_upload: DateTime<Utc>,
    current_date: DateTime<Utc>,

    time_to_upload: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RangeIteration {
    Upload(DateTime<Utc>),
    Generate(DateTime<Utc>),
}

impl RangeIter {
    pub(crate) fn new(
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        generate_interval: Duration,
        upload_interval: Duration,
    ) -> Self {
        Self {
            end_date,
            generate_interval: TimeDelta::from_std(generate_interval).unwrap(),
            upload_interval: TimeDelta::from_std(upload_interval).unwrap(),

            last_upload: start_date,
            current_date: start_date,

            time_to_upload: false,
        }
    }
}

impl Iterator for RangeIter {
    type Item = RangeIteration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.time_to_upload {
            self.time_to_upload = false;
            self.last_upload = self.current_date;
            return Some(RangeIteration::Upload(self.current_date));
        }
        if self.current_date < self.end_date {
            self.current_date += self.generate_interval;
            if self.current_date - self.last_upload >= self.upload_interval {
                self.time_to_upload = true;
            }
            Some(RangeIteration::Generate(self.current_date))
        } else {
            None
        }
    }
}

pub(crate) struct Execution {
    order: usize,
    /// Keys for the predefined labels in the config
    label_keys: Arc<Vec<String>>,
    /// Generated values for the predefined labels
    label_values: Vec<String>,
    config: Arc<AppConfig>,
    generator: SampleGenerator,
    semaphore: Option<Arc<Semaphore>>,

    client: reqwest::Client,
    last_upload: DateTime<Utc>,

    pub failed: bool,
}

#[derive(Debug)]
pub(crate) struct TaskResult {
    pub elapsed: Duration,
    pub error: Option<anyhow::Error>,
}

impl Execution {
    pub(crate) fn new(
        order:usize,
        config: Arc<AppConfig>,
        label_keys: Arc<Vec<String>>,
        label_values: Vec<String>,
        metric_files: Vec<MetricFile>,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        Self {
            order,
            generator: SampleGenerator::new(config.randomization.clone(), metric_files),
            last_upload: config.start_date,
            config,
            label_keys,
            label_values,
            client: reqwest::Client::new(),
            semaphore,
            failed: false,
        }
    }
    pub(crate) fn generate(&mut self, current_date: DateTime<Utc>) -> anyhow::Result<()> {
        self.generator.generate(current_date)
    }

    pub(crate) async fn upload(&mut self, current_date: DateTime<Utc>) -> anyhow::Result<()> {
        let _permit = acquire_permit(self.semaphore.clone()).await?;
        // Sleep for the upload cooldown period to avoid overwhelming the server
        if self.config.upload_cooldown > Duration::ZERO {
            tokio::time::sleep(self.config.upload_cooldown).await;
        }
        trace!(
            order = %self.order,
            file_count = self.generator.file_count(),
            sample_count = self.generator.sample_count(),
            current_date = %current_date.to_rfc3339(),
            last_upload = %self.last_upload.to_rfc3339(),
            "Uploading metrics"
        );
        self.last_upload = current_date;
        upload_metrics(
            &self.config,
            &self.client,
            &self.label_keys,
            &self.label_values,
            self.generator.iter_metrics_groups(),
        )
        .await?;
        self.generator.reset();
        Ok(())
    }

    pub(crate) async fn execute_all(
        self,
        sender: mpsc::Sender<TaskResult>,
        _permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let res = self.execute_all_inner().await;
        let elapsed = start.elapsed();
        let error = res.err();
        sender.send(TaskResult { elapsed, error }).await?;
        Ok(())
    }

    async fn execute_all_inner(mut self) -> anyhow::Result<()> {
        let start = self.config.start_date;
        let range_iter = RangeIter::new(start, self.config.end_date, self.config.generation_period, self.config.upload_interval);

        for iteration in range_iter {
            match iteration {
                RangeIteration::Upload(current_date) => self.upload(current_date).await?,
                RangeIteration::Generate(current_date) => self.generate(current_date)?,
            }
        }
        Ok(())
    }
}

async fn acquire_permit(
    semaphore: Option<Arc<Semaphore>>,
) -> anyhow::Result<Option<OwnedSemaphorePermit>> {
    if let Some(semaphore) = semaphore {
        semaphore
            .acquire_owned()
            .await
            .map(Some)
            .with_context(|| "could not acquire a permit")
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_iter() {
        let mut iter = RangeIter::new(
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap().to_utc(),
            DateTime::parse_from_rfc3339("2024-01-01T00:02:00Z").unwrap().to_utc(),
            Duration::from_secs(15),
            Duration::from_secs(60),
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:00:15Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:00:30Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:00:45Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:01:00Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Upload(
                DateTime::parse_from_rfc3339("2024-01-01T00:01:00Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:01:15Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:01:30Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:01:45Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Generate(
                DateTime::parse_from_rfc3339("2024-01-01T00:02:00Z").unwrap().to_utc()
            ))
        );
        assert_eq!(
            iter.next(),
            Some(RangeIteration::Upload(
                DateTime::parse_from_rfc3339("2024-01-01T00:02:00Z").unwrap().to_utc()
            ))
        );
        assert_eq!(iter.next(), None);

    }
}