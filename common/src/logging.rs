use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::SdkTracerProvider,
    Resource,
};
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::LoggingConfig;

fn resource(service_name: &'static str) -> Resource {
    Resource::builder()
        .with_service_name(service_name)
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build()
}
fn init_tracer_provider(config: &LoggingConfig, service_name: &'static str) -> SdkTracerProvider {
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
        .with_resource(resource(service_name))
        .build();

    provider
}

fn init_meter_provider(config: &LoggingConfig, service_name: &'static str) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::LowMemory) // todo:
        .with_endpoint(
            config
                .otlp_endpoint
                .clone()
                .unwrap_or_else(|| "http://tempo:4317".to_string()),
        )
        .build()
        .expect("Failed to create OTLP exporter");

    let reader = PeriodicReader::builder(exporter).build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource(service_name))
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    meter_provider
}

pub fn init_tracing(config: &LoggingConfig, service_name: &'static str) -> OtelGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(config.level.as_str()));

    let tracer_provider = init_tracer_provider(config, service_name);
    let meter_provider = init_meter_provider(config, service_name);

    let tracer = tracer_provider.tracer(service_name);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().pretty())
        .with(OpenTelemetryLayer::new(tracer))
        .with(MetricsLayer::new(meter_provider.clone()))
        .init();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    init_metrics_exporter(9100);

    OtelGuard {
        tracer_provider,
        meter_provider,
    }
}

pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shutdown tracer provider: {:?}", e);
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("Failed to shutdown meter provider: {:?}", e);
        }
    }
}

fn init_metrics_exporter(port: u16) {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to install Prometheus recorder");
}
