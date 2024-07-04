use std::{collections::HashMap, path::PathBuf};

use openmetrics_parser::{
    MetricFamily, MetricNumber, PrometheusCounterValue, PrometheusType, PrometheusValue, Sample,
};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

use crate::{
    config::Randomization,
    metric_file::{MetricFile, Metrics},
};

pub(crate) type Family = MetricFamily<PrometheusType, PrometheusValue>;

pub(crate) struct SampleGenerator {
    rng: StdRng,
    randomization: Option<Randomization>,
    metrics_files: Vec<MetricFile>,
    generated_expositions: Vec<Vec<Metrics>>,
    family_generators: HashMap<PathBuf, Vec<FamilySampleGenerator>>,
}

impl SampleGenerator {
    pub(crate) fn new(
        randomization: Option<Randomization>,
        metrics_files: Vec<MetricFile>,
    ) -> Self {
        let count = metrics_files.len();
        let family_generators = metrics_files
            .iter()
            .map(|metric_file| {
                let family_generators = metric_file
                    .exposition
                    .families
                    .iter()
                    .map(|(key, family)| {
                        let original_samples = family.iter_samples().cloned().collect();
                        FamilySampleGenerator::new(key.clone(), original_samples)
                    })
                    .collect();
                (metric_file.path.clone(), family_generators)
            })
            .collect();
        Self {
            rng: StdRng::from_entropy(),
            randomization,
            metrics_files,
            generated_expositions: vec![Vec::new(); count],
            family_generators,
        }
    }

    pub(crate) fn file_count(&self) -> usize {
        self.metrics_files.len()
    }

    pub(crate) fn sample_count(&self) -> usize {
        self.generated_expositions.first().map_or(0, Vec::len)
    }

    pub(crate) fn iter_metrics_groups(&self) -> impl Iterator<Item = &Metrics> {
        self.generated_expositions
            .iter()
            .flat_map(|expositions| expositions.iter())
    }

    pub(crate) fn generate(&mut self, now: chrono::DateTime<chrono::Utc>) -> anyhow::Result<()> {
        self.metrics_files
            .iter_mut()
            .enumerate()
            .try_for_each(|(file_index, metric_file)| {
                let family_generators = self.family_generators.get_mut(&metric_file.path).unwrap();
                let generated_families = family_generators.iter_mut().try_fold(
                    Vec::new(),
                    |mut acc, family_generator| {
                        let reference_family = metric_file
                            .exposition
                            .families
                            .get(&family_generator.key)
                            .unwrap();
                        let mut new_family = MetricFamily::new(
                            reference_family.family_name.clone(),
                            reference_family.get_label_names().to_vec(),
                            reference_family.family_type.clone(),
                            reference_family.help.clone(),
                            reference_family.unit.clone(),
                        );

                        family_generator.generate(
                            &mut self.rng,
                            &self.randomization,
                            &mut new_family,
                            now,
                        )?;
                        acc.push(new_family.clone());
                        Ok::<_, anyhow::Error>(acc)
                    },
                )?;
                let generated_exposition = Metrics {
                    families: generated_families
                        .into_iter()
                        .map(|family| (family.family_name.clone(), family))
                        .collect(),
                };
                self.generated_expositions
                    .get_mut(file_index)
                    .unwrap()
                    .push(generated_exposition);

                Ok::<(), anyhow::Error>(())
            })
    }

    pub(crate) fn reset(&mut self) {
        self.metrics_files.iter_mut().for_each(|metric_file| {
            let family_generators = self.family_generators.get_mut(&metric_file.path).unwrap();
            for family_generator in family_generators.iter_mut() {
                let family = metric_file
                    .exposition
                    .families
                    .get_mut(&family_generator.key)
                    .unwrap();
                family.reset_samples();
            }
        });
        self.generated_expositions = vec![Vec::new(); self.metrics_files.len()];
    }

    // fn next_value(&mut self, family: &mut Family)
}

pub(crate) struct FamilySampleGenerator {
    key: String,
    original_samples: Vec<Sample<PrometheusValue>>,
    last_samples: Option<Vec<Sample<PrometheusValue>>>,
}

impl FamilySampleGenerator {
    fn new(key: String, original_samples: Vec<Sample<PrometheusValue>>) -> Self {
        Self {
            key,
            original_samples,
            last_samples: None,
        }
    }

    fn generate(
        &mut self,
        rng: &mut StdRng,
        randomization: &Option<Randomization>,
        family: &mut Family,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        if let Some(randomization) = randomization {
            self.generate_randomized(rng, randomization, family, now)?;
        } else {
            self.generate_nonrandomized(family, now)?;
        }
        Ok(())
    }

    fn generate_randomized(
        &mut self,
        rng: &mut StdRng,
        randomization: &Randomization,
        family: &mut Family,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let factor = randomization.factor;
        let samples = self.last_samples.as_ref().unwrap_or(&self.original_samples);
        #[allow(clippy::cast_precision_loss)]
        let now_timestamp = now.timestamp_millis() as f64;
        samples
            .iter()
            .map(|sample| {
                let mut new_sample = sample.clone();
                new_sample.timestamp = Some(now_timestamp);
                new_sample.value = match sample.value {
                    PrometheusValue::Gauge(ref value) => {
                        if rng.gen::<f64>() > factor {
                            PrometheusValue::Gauge(MetricNumber::Float(
                                value.as_f64() - factor * value.as_f64(),
                            ))
                        } else {
                            PrometheusValue::Gauge(MetricNumber::Float(
                                value.as_f64() + factor * value.as_f64(),
                            ))
                        }
                    }
                    PrometheusValue::Counter(ref value) => {
                        if rng.gen::<f64>() > factor {
                            new_sample.value
                        } else {
                            PrometheusValue::Counter(PrometheusCounterValue {
                                value: MetricNumber::Int(
                                    value.value.as_i64().unwrap_or_default() + 1,
                                ),
                                exemplar: None,
                            })
                        }
                    }
                    PrometheusValue::Summary(ref value) => {
                        if rng.gen::<f64>() > factor {
                            new_sample.value
                        } else {
                            let mut new_value = value.clone();
                            new_value.count = Some(new_value.count.unwrap_or_default() + 1);
                            new_value.sum = Some(MetricNumber::Int(
                                new_value.sum.and_then(|s| s.as_i64()).unwrap_or_default() + 1,
                            ));
                            PrometheusValue::Summary(new_value)
                        }
                    }
                    PrometheusValue::Histogram(ref value) => {
                        if rng.gen::<f64>() > factor {
                            new_sample.value
                        } else {
                            let mut new_value = value.clone();
                            let count = value.count.unwrap_or_default() + 1;
                            #[allow(clippy::cast_possible_truncation)]
                            let bucket_to_modify = new_value.buckets.len() % count as usize;
                            if !new_value.buckets.is_empty() {
                                new_value.buckets[bucket_to_modify].count = MetricNumber::Int(
                                    new_value.buckets[bucket_to_modify]
                                        .count
                                        .as_i64()
                                        .unwrap_or_default()
                                        + 1,
                                );
                            }
                            new_value.count = Some(count);
                            PrometheusValue::Histogram(new_value)
                        }
                    }
                    PrometheusValue::Unknown(ref value) => PrometheusValue::Unknown(*value),
                };
                new_sample
            })
            .try_for_each(|sample| family.add_sample(sample))
            .map_err(|e| anyhow::anyhow!("Could not add sample to family: {}", e))?;

        Ok(())
    }

    fn generate_nonrandomized(
        &mut self,
        family: &mut Family,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        #[allow(clippy::cast_precision_loss)]
        let now_timestamp = now.timestamp_millis() as f64;
        // No randomization. Increment the counters
        self.original_samples
            .iter()
            .map(|sample| {
                let mut new_sample = sample.clone();
                new_sample.timestamp = Some(now_timestamp);
                new_sample.value = match sample.value {
                    PrometheusValue::Gauge(ref value) => {
                        PrometheusValue::Gauge(MetricNumber::Float(value.as_f64() + 1.0))
                    }
                    PrometheusValue::Counter(ref value) => {
                        PrometheusValue::Counter(PrometheusCounterValue {
                            value: MetricNumber::Int(value.value.as_i64().unwrap_or_default() + 1),
                            exemplar: None,
                        })
                    }
                    PrometheusValue::Summary(ref value) => {
                        let mut new_value = value.clone();
                        new_value.count = Some(new_value.count.unwrap_or_default() + 1);
                        new_value.sum = Some(MetricNumber::Int(
                            new_value.sum.and_then(|s| s.as_i64()).unwrap_or_default() + 1,
                        ));
                        PrometheusValue::Summary(new_value)
                    }
                    PrometheusValue::Histogram(ref value) => {
                        let mut new_value = value.clone();
                        let count = value.count.unwrap_or_default() + 1;
                        #[allow(clippy::cast_possible_truncation)]
                        let bucket_to_modify = new_value.buckets.len() % count as usize;
                        if !new_value.buckets.is_empty() {
                            new_value.buckets[bucket_to_modify].count = MetricNumber::Int(
                                new_value.buckets[bucket_to_modify]
                                    .count
                                    .as_i64()
                                    .unwrap_or_default()
                                    + 1,
                            );
                        }
                        new_value.count = Some(count);
                        PrometheusValue::Histogram(new_value)
                    }
                    PrometheusValue::Unknown(ref value) => PrometheusValue::Unknown(*value),
                };
                new_sample
            })
            .try_for_each(|sample| family.add_sample(sample))
            .map_err(|e| anyhow::anyhow!("Could not add sample to family: {}", e))?;

        Ok(())
    }
}
