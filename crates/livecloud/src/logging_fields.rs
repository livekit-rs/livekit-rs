//! Typed native tracing event projection into the fixed zap field encoder.
//! Visit defaults call record_debug; using those defaults would turn every
//! numeric and boolean producer into a quoted string in logging.json output.
use crate::zap::ZapValue;
use tracing::Event;
use tracing_core::field::{Field, Visit};

#[derive(Default)]
pub(crate) struct EventFields {
    pub(crate) message: String,
    pub(crate) fields: Vec<(String, ZapValue)>,
}
impl EventFields {
    pub(crate) fn collect(event: &Event<'_>) -> Self {
        let mut value = Self::default();
        event.record(&mut value);
        value
    }
    fn push(&mut self, field: &Field, value: ZapValue, message: impl FnOnce() -> String) {
        if field.name() == "message" {
            self.message = message();
        } else {
            self.fields.push((field.name().to_string(), value));
        }
    }
}
impl Visit for EventFields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, ZapValue::Num(value), || value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, ZapValue::Uint(value), || value.to_string());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, ZapValue::Float(value), || value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, ZapValue::Bool(value), || value.to_string());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, ZapValue::Str(value.into()), || value.to_string());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        // zap.Error/Errorw uses Error() text; Debug can expose a Rust struct
        // wrapper instead of the user-facing message and error chain.
        let text = value.to_string();
        self.push(field, ZapValue::Str(text.clone()), || text);
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        self.push(field, ZapValue::Str(text.clone()), || text);
    }
}
