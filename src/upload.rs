use std::{collections::HashMap, sync::Arc};

use openmetrics_parser::{PrometheusValue, Sample};
use reqwest::RequestBuilder;
use tracing::{error, trace};
use url::Url;

use crate::{
    config::{AppConfig, Destination},
    generate::Family,
    metric_file::Metrics,
    upload::remote_write::{Label, TimeSeries, WriteRequest},
};

const LABEL_NAME: &str = "__name__";
// const LABEL_TENANT_ID: &str = "tenant_id";
const CONTENT_TYPE: &str = "application/x-protobuf";
const HEADER_NAME_REMOTE_WRITE_VERSION: &str = "X-Prometheus-Remote-Write-Version";
const REMOTE_WRITE_VERSION_01: &str = "0.1.0";

#[allow(unreachable_pub)]
pub(crate) mod remote_write {
    include!(concat!(env!("OUT_DIR"), "/remote_write.rs"));
}

pub(crate) async fn upload_metrics(
    config: &AppConfig,
    client: &reqwest::Client,
    label_keys: &Arc<Vec<String>>,
    label_values: &[String],
    metrics_iter: impl Iterator<Item = &Metrics>,
) -> anyhow::Result<()> {
    let write_request: WriteRequest =
        WriteRequest::from_exposition(metrics_iter, label_keys, label_values);

    match &config.destination {
        Destination::RemoteWrite {
            url,
            headers,
            user_agent,
        } => upload_remote_write(client, write_request, url, headers, user_agent).await?,
        Destination::Stdout => write_to_stdout(&write_request),
    }
    Ok(())
}

pub(crate) async fn upload_remote_write(
    client: &reqwest::Client,
    write_request: WriteRequest,
    url: &str,
    headers: &[(String, String)],
    user_agent: &Option<String>,
) -> anyhow::Result<()> {
    let user_agent = user_agent
        .to_owned()
        .unwrap_or_else(|| format!("metrics-gen/{}", env!("CARGO_PKG_VERSION")));
    let url: Url = url.parse()?;
    let req = write_request.build_http_request(client, &url, headers, &user_agent)?;
    let response = req.send().await?;
    match response.status().as_u16() {
        200 => (),
        201..=399 => {
            let body = response.text().await?;
            trace!("Uploaded metrics: {}", body);
        }
        code => {
            let body = response.text().await?;
            error!(%code, %body, "Error uploading metrics");
        }
    }

    Ok(())
}

pub(crate) fn write_to_stdout(write_request: &WriteRequest) {
    trace!("Write request to stdout: {:?}", write_request);
}

impl WriteRequest {
    /// Prepare the write request for sending.
    ///
    /// Ensures that the request conforms to the specification.
    /// See <https://prometheus.io/docs/concepts/remote_write_spec>.
    pub(crate) fn sort(&mut self) {
        for series in &mut self.timeseries {
            series.sort_labels_and_samples();
        }
    }

    pub(crate) fn sorted(mut self) -> Self {
        self.sort();
        self
    }

    pub(crate) fn encode_proto3(self) -> Vec<u8> {
        prost::Message::encode_to_vec(&self.sorted())
    }

    pub(crate) fn encode_compressed(self) -> Result<Vec<u8>, snap::Error> {
        snap::raw::Encoder::new().compress_vec(&self.encode_proto3())
    }

    pub(crate) fn from_exposition<'m>(
        input: impl Iterator<Item = &'m Metrics>,
        label_keys: &Arc<Vec<String>>,
        label_values: &[String],
    ) -> Self {
        fn sample_to_timeseries(
            sample: &Sample<PrometheusValue>,
            family: &Family,
            all_series: &mut HashMap<String, TimeSeries>,
            label_keys: &Arc<Vec<String>>,
            label_values: &[String],
        ) -> anyhow::Result<()> {
            let mut ident = family.family_name.clone();
            ident.push_str("_$$_");

            // label names must be sorted
            let mut labelset: Vec<(String, String)> = sample
                .get_labelset()
                .map_err(|e| anyhow::anyhow!(e))?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            labelset.push((LABEL_NAME.into(), family.family_name.clone()));
            // The generated labels according to the config are added as well
            for (k, v) in label_keys.iter().zip(label_values.iter()) {
                labelset.push((k.clone(), v.clone()));
            }

            labelset.retain(|(k, v)| !k.is_empty() && !v.is_empty());

            labelset.sort_by(|a, b| a.0.cmp(&b.0));

            for (k, v) in &labelset {
                ident.push_str(k);
                ident.push('=');
                ident.push_str(v);
            }

            let series = all_series.entry(ident).or_insert_with(|| {
                let labels = labelset
                    .iter()
                    .map(|(k, v)| Label {
                        name: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect::<Vec<_>>();

                TimeSeries {
                    labels,
                    samples: vec![],
                }
            });

            let value = match &sample.value {
                PrometheusValue::Counter(v) => v.value.as_f64(),
                PrometheusValue::Gauge(v) | PrometheusValue::Unknown(v) => v.as_f64(),
                // TODO: fix the histogram to send all the field values
                #[allow(clippy::cast_precision_loss)]
                PrometheusValue::Histogram(v) => v.count.unwrap_or_default() as f64,
                // TODO: fix the summary too
                #[allow(clippy::cast_precision_loss)]
                PrometheusValue::Summary(v) => v.count.unwrap_or_default() as f64,
            };

            series.samples.push(remote_write::Sample {
                value,
                #[allow(clippy::cast_possible_truncation)]
                timestamp: sample.timestamp.unwrap_or_default() as i64,
            });

            Ok(())
        }

        // This is all very inefficient.
        let mut series = input.fold(Vec::new(), |acc: Vec<TimeSeries>, metrics| {
            metrics
                .families
                .iter()
                .fold(acc, |mut acc, (_key, family)| {
                    let all_series = family.iter_samples().fold(
                        HashMap::<String, TimeSeries>::new(),
                        |mut all_series, sample| {
                            if let Err(err) = sample_to_timeseries(
                                sample,
                                family,
                                &mut all_series,
                                label_keys,
                                label_values,
                            ) {
                                error!("Error converting sample to timeseries: {}", err);
                            }
                            all_series
                        },
                    );
                    let series: Vec<TimeSeries> = all_series.into_values().collect();
                    acc.extend(series);
                    acc
                })
        });

        let slen = series.len();
        series.retain(|s| !s.samples.is_empty() && !s.labels.is_empty());
        if series.len() < slen {
            error!("Removed {} empty series", slen - series.len());
        }

        series.sort_by(|a, b| {
            let name_a = a.labels.iter().find(|x| x.name == LABEL_NAME).unwrap();
            let name_b = b.labels.iter().find(|x| x.name == LABEL_NAME).unwrap();
            name_a.value.cmp(&name_b.value)
        });

        let s = Self { timeseries: series };

        s.sorted()
    }

    pub(crate) fn build_http_request(
        self,
        client: &reqwest::Client,
        endpoint: &Url,
        headers: &[(String, String)],
        user_agent: &str,
    ) -> anyhow::Result<RequestBuilder> {
        let req = client
            .post(endpoint.as_str())
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE)
            .header(HEADER_NAME_REMOTE_WRITE_VERSION, REMOTE_WRITE_VERSION_01)
            .header(http::header::CONTENT_ENCODING, "snappy")
            .header(http::header::USER_AGENT, user_agent);

        let req = headers.iter().fold(req, |req, (k, v)| req.header(k, v));

        let req = req.body(self.encode_compressed()?); // Assuming encode_compressed() returns Vec<u8>

        Ok(req)
    }
}

impl TimeSeries {
    /// Sort labels by name, and the samples by timestamp.
    ///
    /// Required by the specification.
    pub(crate) fn sort_labels_and_samples(&mut self) {
        self.labels.sort_by(|a, b| a.name.cmp(&b.name));
        self.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }
}
