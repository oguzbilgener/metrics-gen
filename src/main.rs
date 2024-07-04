#![forbid(unsafe_code)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    arithmetic_overflow,
    unreachable_pub,
    unused_results
)]
#![allow(clippy::multiple_crate_versions)]

//! # metrics-gen
#[doc = include_str!("../README.md")]
mod config;
mod generate;
mod metric_file;
mod runner;
mod upload;

use std::{env, str::FromStr as _};

use crate::config::AppConfig;
use crate::metric_file::{load_metric_files, validate_metrics_path};
use anyhow::Context;
use clap::Parser;
use tracing::{error, info, trace};
// use tracing_indicatif::IndicatifLayer;
// use tracing_subscriber::layer::SubscriberExt;
// use tracing_subscriber::util::SubscriberInitExt;
// use tracing_subscriber::Layer as _;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    /// Path to the configuration file
    #[arg(short, long)]
    pub(crate) config: String,

    /// Log level
    #[arg(short, long, default_value = "info")]
    pub(crate) log_level: String,

    /// Concurrency
    #[arg(long, default_value = "8", short = 'C')]
    pub(crate) concurrency: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    setup_logging(&args.log_level)?;

    let config_file = AppConfig::from_file(&args.config).await?;
    let metric_files =
        load_metric_files(validate_metrics_path(&config_file.metrics_dir)?.as_ref()).await?;

    tokio::select! {
        res = runner::run(args, config_file, metric_files) => {
            if let Err(err) = res {
                error!(?err, "Runner failed");
            } else {
                trace!("Runner finished");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, shutting down");
        }
    }
    Ok(())
}

fn setup_logging(log_level: &str) -> anyhow::Result<()> {
    let log_level = tracing::Level::from_str(log_level)
        .with_context(|| format!("Invalid log level {log_level}"))?;
    // Apply additional filters to libraries that are too verbose.
    let env_filter = tracing_subscriber::EnvFilter::new(
        env::var("LOG_FILTER")
            .unwrap_or_else(|_| format!("{log_level},tower_http=warn,hyper=warn,tokio_util=warn")),
    );

    // let indicatif_layer = IndicatifLayer::new();

    // let env_filter = tracing_subscriber::fmt::layer().with_filter(env_filter);
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_env_filter(env_filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // tracing_subscriber::registry()
    //     // .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
    //     // .with(indicatif_layer)
    //     .with(env_filter)
    //     .init();

    Ok(())
}
