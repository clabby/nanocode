use std::{
    fs::{DirBuilder, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::{
    tonic::collector::{
        metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest,
    },
    tonic::trace::v1::ResourceSpans,
    transform::{
        common::tonic::ResourceAttributesWithSchema,
        trace::tonic::group_spans_by_resource_and_scope,
    },
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    metrics::{Temporality, data::ResourceMetrics, exporter::PushMetricExporter},
    trace::{SpanData, SpanExporter},
};
use serde::Serialize;

pub struct Exporters {
    pub traces: TraceExporter,
    pub metrics: MetricExporter,
}

impl Exporters {
    pub fn create(directory: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let directory = directory.join(format!("{timestamp}-{}", std::process::id()));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        Ok(Self {
            traces: TraceExporter {
                output: Jsonl::create(&directory.join("traces.jsonl"))?,
                resource: ResourceAttributesWithSchema::default(),
            },
            metrics: MetricExporter {
                output: Jsonl::create(&directory.join("metrics.jsonl"))?,
            },
        })
    }
}

#[derive(Debug)]
struct Jsonl {
    file: Mutex<Option<File>>,
}

impl Jsonl {
    fn create(path: &Path) -> io::Result<Self> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        Ok(Self {
            file: Mutex::new(Some(options.open(path)?)),
        })
    }

    fn write(&self, value: &impl Serialize) -> OTelSdkResult {
        let result = (|| {
            let mut encoded = serde_json::to_vec(value).map_err(io::Error::other)?;
            encoded.push(b'\n');
            let mut file = self
                .file
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let file = file
                .as_mut()
                .ok_or_else(|| io::Error::other("OTLP file exporter is shut down"))?;
            file.write_all(&encoded)
        })();
        result.map_err(|error| {
            eprintln!("nanocode: failed to write local OTLP telemetry: {error}");
            OTelSdkError::InternalFailure(error.to_string())
        })
    }

    fn shutdown(&self) -> OTelSdkResult {
        let mut file = self
            .file
            .lock()
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))?;
        drop(file.take());
        Ok(())
    }
}

#[derive(Debug)]
pub struct TraceExporter {
    output: Jsonl,
    resource: ResourceAttributesWithSchema,
}

impl SpanExporter for TraceExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let resource_spans = match <[SpanData; 1]>::try_from(batch) {
            Ok([span]) => vec![ResourceSpans::new(span, &self.resource)],
            Err(batch) => group_spans_by_resource_and_scope(batch, &self.resource),
        };
        self.output
            .write(&ExportTraceServiceRequest { resource_spans })
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.output.shutdown()
    }
}

pub struct MetricExporter {
    output: Jsonl,
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut request = serde_json::to_value(ExportMetricsServiceRequest::from(metrics))
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))?;
        metric_integers(&mut request);
        self.output.write(&request)
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.output.shutdown()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn metric_integers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if key == "asInt" && value.is_number() {
                    *value = serde_json::Value::String(value.to_string());
                } else {
                    metric_integers(value);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(metric_integers),
        _ => {}
    }
}
