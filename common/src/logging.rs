use std::time::Duration;

use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::SdkTracerProvider,
};
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::LoggingConfig;

fn resource(service_name: &'static str) -> Resource {
    Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build()
}

fn get_endpoint(config: &LoggingConfig) -> String {
    config
        .otlp_endpoint
        .clone()
        .unwrap_or_else(|| "http://alloy:4317".to_string())
}

fn init_tracer_provider(endpoint: &str, service_name: &'static str) -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to create OTLP span exporter");

    SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource(service_name))
        .build()
}

fn init_meter_provider(endpoint: &str, service_name: &'static str) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::LowMemory)
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to create OTLP metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(10))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource(service_name))
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    meter_provider
}

fn init_logger_provider(endpoint: &str, service_name: &'static str) -> SdkLoggerProvider {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to create OTLP log exporter");

    SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource(service_name))
        .build()
}

fn init_metrics_exporter(port: u16) {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to install Prometheus recorder");
}

pub struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(ref tracer_provider) = self.tracer_provider
            && let Err(e) = tracer_provider.shutdown()
        {
            eprintln!("Failed to shutdown tracer provider: {:?}", e);
        }
        if let Some(ref meter_provider) = self.meter_provider
            && let Err(e) = meter_provider.shutdown()
        {
            eprintln!("Failed to shutdown meter provider: {:?}", e);
        }
        if let Some(ref logger_provider) = self.logger_provider
            && let Err(e) = logger_provider.shutdown()
        {
            eprintln!("Failed to shutdown logger provider: {:?}", e);
        }
    }
}

pub fn init_tracing(config: &LoggingConfig, service_name: &'static str) -> OtelGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(config.level.as_str()));

    let fmt_layer = tracing_subscriber::fmt::layer().pretty();

    init_metrics_exporter(9100);

    if !config.enabled {
        tracing_subscriber::registry().with(filter).with(fmt_layer).init();

        return OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        };
    }

    let endpoint = get_endpoint(config);

    let tracer_provider = init_tracer_provider(&endpoint, service_name);
    let meter_provider = init_meter_provider(&endpoint, service_name);
    let logger_provider = init_logger_provider(&endpoint, service_name);

    let tracer = tracer_provider.tracer(service_name);
    let otel_layer = OpenTelemetryLayer::new(tracer);
    let metrics_layer = MetricsLayer::new(meter_provider.clone());
    let logger_layer = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .with(metrics_layer)
        .with(logger_layer)
        .init();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    OtelGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
        logger_provider: Some(logger_provider),
    }
}
