use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::LoggingConfig;

pub fn init_tracing(config: &LoggingConfig, service_name: &'static str) -> opentelemetry_sdk::trace::SdkTracerProvider {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(config.level.as_str()));

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(
            config
                .otlp_endpoint
                .clone()
                .unwrap_or_else(|| "http://tempo:4317".to_string()),
        )
        .build()
        .expect("Failed to create OTLP exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attribute(KeyValue::new("service.name", service_name))
                .build(),
        )
        .build();

    let otlp_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty())
        .with(otlp_layer)
        .init();

    provider
}
pub fn init_metrics(port: u16) {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to install Prometheus recorder");
}
