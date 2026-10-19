use std::time::Duration;

use opentelemetry::Value;
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
};

pub fn restore_integers(span: &mut SpanData) {
    for attribute in &mut span.attributes {
        if let Value::String(text) = &attribute.value
            && let Some(integer) = unsigned_integer(text.as_str())
        {
            attribute.value = Value::I64(integer);
        }
    }
}

fn unsigned_integer(text: &str) -> Option<i64> {
    let digits = text.as_bytes();
    let canonical = match digits {
        [] => false,
        [b'0'] => true,
        [first, ..] => *first != b'0' && digits.iter().all(u8::is_ascii_digit),
    };
    canonical.then(|| text.parse().ok()).flatten()
}

#[derive(Debug)]
pub struct IntegerAttributes<E>(pub E);

impl<E: SpanExporter> SpanExporter for IntegerAttributes<E> {
    fn export(
        &self,
        mut batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        batch.iter_mut().for_each(restore_integers);
        self.0.export(batch)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
}
