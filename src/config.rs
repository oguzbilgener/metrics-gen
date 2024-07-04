use std::time::Duration;

use derivative::Derivative;

use serde::Deserialize;

use crate::config::validator::Validatable as _;

#[derive(Debug, Clone, Derivative, Deserialize)]
#[derivative(Default)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AppConfig {
    pub(crate) destination: Destination,

    #[derivative(Default(value = "String::from(\"./metrics\")"))]
    pub(crate) metrics_dir: String,

    #[serde(with = "humantime_serde")]
    #[derivative(Default(value = "Duration::from_secs(0)"))]
    pub(crate) upload_cooldown: Duration,

    #[serde(with = "humantime_serde")]
    #[derivative(Default(value = "Duration::from_secs(15)"))]
    pub(crate) generation_period: Duration,

    #[serde(with = "humantime_serde")]
    #[derivative(Default(value = "Duration::from_secs(600)"))]
    pub(crate) upload_interval: Duration,

    #[derivative(Default(value = "vec![Label{name: \"id\".to_string(), count: 1}]"))]
    pub(crate) labels: Vec<Label>,

    #[derivative(Default(value = "\"2024-01-01T00:00:00Z\".parse().unwrap()"))]
    pub(crate) start_date: chrono::DateTime<chrono::Utc>,

    #[derivative(Default(value = "\"2024-01-02T00:00:00Z\".parse().unwrap()"))]
    pub(crate) end_date: chrono::DateTime<chrono::Utc>,

    #[serde(default)]
    #[derivative(Default(value = "Some(Randomization::default())"))]
    pub(crate) randomization: Option<Randomization>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all_fields = "camelCase")]
pub(crate) enum Destination {
    #[serde(alias = "remoteWrite")]
    RemoteWrite {
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,

        #[serde(default)]
        user_agent: Option<String>,
    },
    #[default]
    #[serde(alias = "stdout")]
    Stdout,
}

#[derive(Debug, Clone, Derivative, Deserialize)]
#[derivative(Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Randomization {
    #[serde(default)]
    #[derivative(Default(value = "0.1"))]
    pub(crate) factor: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Label {
    pub(crate) name: String,
    pub(crate) count: u64,
}

impl AppConfig {
    pub(crate) async fn from_file(path: &str) -> anyhow::Result<Self> {
        let config = tokio::fs::read_to_string(path).await?;
        let config: Self = serde_yaml::from_str(&config)?;
        config.validate()?;
        Ok(config)
    }
}

pub(crate) mod validator {
    use std::{error::Error, fmt::Display};

    use thiserror::Error;

    use super::{AppConfig, Randomization};

    pub(crate) trait Validatable<E: Error> {
        fn validate(&self) -> Result<(), ValidationErrors<E>>
        where
            Self: ValidateInner<E>,
        {
            let errors = self.do_validate();
            if errors.is_empty() {
                Ok(())
            } else {
                Err(ValidationErrors(errors))
            }
        }
    }

    pub(crate) trait ValidateInner<E: Error> {
        fn do_validate(&self) -> Vec<E>;
    }

    impl<C, E: Error> Validatable<E> for C where C: ValidateInner<E> {}

    /// This exists to implement `Display` and `Error` for `Vec<ValidationError>`
    /// to be able to use it in `#[error]` attributes.
    /// Some validation is implicitly done in the `serde` deserialization step as well.
    #[derive(Debug)]
    pub(crate) struct ValidationErrors<E: Error>(Vec<E>);

    #[derive(Debug, Error)]
    pub(crate) enum AppConfigValidationError {
        #[error("randomization config is invalid: {0}")]
        InvalidRandomization(RandomizationValidationError),
        #[error("labels must not be empty")]
        EmptyLabels,
        #[error("label error: {0}")]
        Label(LabelError),
        #[error("start date must be before end date")]
        EndDateBeforeStartDate,
    }

    #[derive(Debug, Error)]
    pub(crate) enum DestinationError {
        #[error("remote write error: {0}")]
        RemoteWrite(RemoteWriteError),
    }

    #[derive(Debug, Error)]
    pub(crate) enum RemoteWriteError {
        #[error("url must not be empty")]
        EmptyUrl,
        #[error("url is invalid")]
        InvalidUrl,
    }

    #[derive(Debug, Error)]
    pub(crate) enum RandomizationValidationError {
        #[error("factor must be between 0 and 1")]
        InvalidFactor,
    }

    #[derive(Debug, Error)]
    pub(crate) enum LabelError {
        #[error("label name must not be empty")]
        EmptyName,
        #[error("label count must be greater than 0")]
        InvalidCount,
    }

    impl ValidateInner<AppConfigValidationError> for AppConfig {
        fn do_validate(&self) -> Vec<AppConfigValidationError> {
            let mut errors: Vec<AppConfigValidationError> = Vec::new();

            if self.labels.is_empty() {
                errors.push(AppConfigValidationError::EmptyLabels);
            }

            errors.extend(
                self.labels
                    .iter()
                    .flat_map(ValidateInner::do_validate)
                    .map(AppConfigValidationError::Label),
            );

            if self.start_date >= self.end_date {
                errors.push(AppConfigValidationError::EndDateBeforeStartDate);
            }

            if let Some(randomization) = &self.randomization {
                errors.extend(
                    randomization
                        .do_validate()
                        .into_iter()
                        .map(AppConfigValidationError::InvalidRandomization),
                );
            }

            errors
        }
    }

    impl ValidateInner<RandomizationValidationError> for Randomization {
        fn do_validate(&self) -> Vec<RandomizationValidationError> {
            let mut errors = Vec::new();

            if self.factor < 0.0 || self.factor > 1.0 {
                errors.push(RandomizationValidationError::InvalidFactor);
            }

            errors
        }
    }

    impl ValidateInner<LabelError> for super::Label {
        fn do_validate(&self) -> Vec<LabelError> {
            let mut errors = Vec::new();

            if self.name.is_empty() {
                errors.push(LabelError::EmptyName);
            }

            if self.count == 0 {
                errors.push(LabelError::InvalidCount);
            }

            errors
        }
    }

    impl ValidateInner<DestinationError> for super::Destination {
        fn do_validate(&self) -> Vec<DestinationError> {
            match self {
                super::Destination::RemoteWrite { url, .. } => {
                    let mut errors = Vec::new();

                    if url.is_empty() {
                        errors.push(DestinationError::RemoteWrite(RemoteWriteError::EmptyUrl));
                    }

                    if url.parse::<url::Url>().is_err() {
                        errors.push(DestinationError::RemoteWrite(RemoteWriteError::InvalidUrl));
                    }

                    errors
                }
                super::Destination::Stdout => Vec::new(),
            }
        }
    }

    impl<E: Error> Display for ValidationErrors<E> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for error in &self.0 {
                writeln!(f, "{error}")?;
            }
            Ok(())
        }
    }

    impl<E: Error + 'static> Error for ValidationErrors<E> {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            if self.0.is_empty() {
                None
            } else {
                Some(self)
            }
        }
    }

    #[cfg(test)]
    impl<E: Error> ValidationErrors<E> {
        pub(crate) fn inner(&self) -> &[E] {
            &self.0
        }

        pub(crate) fn len(&self) -> usize {
            self.0.len()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validator::*;
    use super::*;

    #[test]
    fn test_validation_empty_labels() {
        let config = AppConfig {
            labels: vec![],
            ..Default::default()
        };

        let errors = config.validate().unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors.inner()[0],
            AppConfigValidationError::EmptyLabels
        ));
    }

    #[test]
    fn test_empty_yaml() {
        let config = "";

        let config: AppConfig = serde_yaml::from_str(config).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_valid_yaml() {
        let config = r#"
        metricsDir: "./metrics"
        destination:
          type: remoteWrite
          url: "http://localhost:8080/api/v1/write"
          headers:
            - ["Authorization", "Bearer token"]
        generationPeriod: 30s
        uploadInterval: 300s
        labels:
          - name: "label1"
            count: 1
          - name: "label2"
            count: 2
        startDate: "2021-01-01T00:00:00Z"
        endDate: "2022-01-01T00:00:00Z"
        randomization:
          factor: 0.1
        "#;

        let config: AppConfig = serde_yaml::from_str(config).unwrap();
        assert!(config.validate().is_ok());
    }
}
