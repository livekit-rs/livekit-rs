//! Fixed `psrpc/pkg/middleware/otelpsrpc`: unary client attempts and accepted
//! server handlers, including multi-server handlers (not multi clients/streams).
//! Context is attached for each future poll, never across an await on a thread.
use super::rpc::{MethodSpec, RpcError};
use opentelemetry::{
    Context, InstrumentationScope, KeyValue, global,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::{FutureExt, SpanKind, Status, TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::{collections::BTreeMap, future::Future, sync::Arc, time::Instant};

/// Provider captured at client/server construction, as Go's OTel options do.
/// A scoped override permits isolated exporters without mutating the global
/// provider in parallel tests, and corresponds to ClientParams.TracerProvider.
#[derive(Clone)]
pub struct RpcTracing {
    tracer: Option<Arc<global::BoxedTracer>>,
    client_layers: usize,
    excluded_services: std::collections::BTreeSet<String>,
}
impl RpcTracing {
    pub fn from_provider<P>(provider: &P) -> Self
    where
        P: TracerProvider,
        P::Tracer: Send + Sync + 'static,
        <P::Tracer as Tracer>::Span: Send + Sync + 'static,
    {
        let scope = InstrumentationScope::builder("github.com/livekit/psrpc")
            .with_version("v0.8.0")
            .build();
        Self {
            tracer: Some(Arc::new(global::BoxedTracer::new(Box::new(
                provider.tracer_with_scope(scope),
            )))),
            client_layers: 1,
            excluded_services: Default::default(),
        }
    }
    /// Explicit original otelpsrpc option. Plain generated constructors have none.
    pub fn global_options() -> Self {
        Self::from_provider(&global::tracer_provider())
    }
    /// Fixed server wire ClientParams.Options adds its own OTel interceptor,
    /// then wire_gen's explicit ClientOptions adds a second nested interceptor.
    pub fn production_client_params() -> Self {
        let mut options = Self::factory_options();
        options.client_layers = 2;
        options
    }
    /// Explicit factory option, with isolated TracerProvider injection for
    /// fixtures corresponding to Go Config.TracerProvider (never a global reset).
    pub(crate) fn factory_options() -> Self {
        Context::current()
            .get::<Self>()
            .cloned()
            .unwrap_or_else(Self::global_options)
    }
    /// A shared host façade may contain a separately constructed uninstrumented
    /// Go client (RoomManager); excluding it is constructor policy, not name inference.
    pub fn excluding_service(mut self, service: &str) -> Self {
        self.excluded_services.insert(service.into());
        self
    }
    fn disabled() -> Self {
        Self {
            tracer: None,
            client_layers: 0,
            excluded_services: Default::default(),
        }
    }
    pub fn scoped_context(&self) -> Context {
        Context::current().with_value(self.clone())
    }
    pub(crate) fn capture() -> Self {
        // Only explicit constructor options/scoped middleware fixtures opt in.
        // Configuring a global provider does not install middleware in Go.
        Context::current()
            .get::<Self>()
            .cloned()
            .unwrap_or_else(Self::disabled)
    }
    pub(crate) fn client(
        &self,
        spec: &MethodSpec,
        metadata: &mut BTreeMap<String, String>,
        deadline: Option<Instant>,
    ) -> RpcSpan {
        if self.tracer.is_none() || self.excluded_services.contains(&spec.service) {
            return RpcSpan::disabled();
        }
        let mut parent = Context::current();
        let mut contexts = Vec::new();
        for _ in 0..self.client_layers {
            parent = self.start_context(spec, "Sent", SpanKind::Client, parent);
            contexts.push(parent.clone());
        }
        TraceContextPropagator::new().inject_context(&parent, &mut CarrierMut(metadata));
        RpcSpan {
            context: parent,
            contexts,
            completed: false,
            deadline,
        }
    }
    pub(crate) fn server(
        &self,
        spec: &MethodSpec,
        metadata: &BTreeMap<String, String>,
        deadline: Instant,
    ) -> RpcSpan {
        if self.tracer.is_none() {
            return RpcSpan::disabled();
        }
        let parent =
            TraceContextPropagator::new().extract_with_context(&Context::new(), &Carrier(metadata));
        let context = self.start_context(spec, "Recv", SpanKind::Server, parent);
        RpcSpan {
            context: context.clone(),
            contexts: vec![context],
            completed: false,
            deadline: Some(deadline),
        }
    }
    fn start_context(
        &self,
        spec: &MethodSpec,
        prefix: &str,
        kind: SpanKind,
        parent: Context,
    ) -> Context {
        let tracer = self
            .tracer
            .as_ref()
            .expect("explicit OTel constructor options");
        let span = tracer.build_with_context(
            tracer
                .span_builder(format!("{prefix}.{}.{}", spec.service, spec.method))
                .with_kind(kind),
            &parent,
        );
        let context = parent.with_span(span);
        context.span().add_event("Outbound message", Vec::new());
        context
    }
}

struct Carrier<'a>(&'a BTreeMap<String, String>);
impl Extractor for Carrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}
struct CarrierMut<'a>(&'a mut BTreeMap<String, String>);
impl Injector for CarrierMut<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.into(), value);
    }
}

pub(crate) struct RpcSpan {
    context: Context,
    contexts: Vec<Context>,
    completed: bool,
    deadline: Option<Instant>,
}
impl RpcSpan {
    fn disabled() -> Self {
        Self {
            context: Context::new(),
            contexts: Vec::new(),
            completed: false,
            deadline: None,
        }
    }
    pub(crate) async fn run<T>(
        mut self,
        future: impl Future<Output = Result<T, RpcError>>,
    ) -> Result<T, RpcError> {
        if self.contexts.is_empty() {
            self.completed = true;
            return future.await;
        }
        let result = future.with_context(self.context.clone()).await;
        self.finish(result.as_ref().err(), true);
        result
    }
    fn finish(&mut self, error: Option<&RpcError>, inbound: bool) {
        for context in self.contexts.iter().rev() {
            if inbound {
                context.span().add_event("Inbound message", Vec::new());
            }
            let span = context.span();
            match error {
                None => {
                    span.set_status(Status::Ok);
                    span.set_attribute(KeyValue::new("rpc.grpc.status_code", 0_i64));
                }
                Some(error) => {
                    // psrpcError embeds the underlying error: Go Error() is the
                    // message alone, not Rust's diagnostic `code: message` Display.
                    span.add_event(
                        "exception",
                        vec![
                            KeyValue::new("exception.type", "github.com/livekit/psrpc.psrpcError"),
                            KeyValue::new("exception.message", error.message.clone()),
                        ],
                    );
                    span.set_status(Status::error(error.message.clone()));
                    span.set_attribute(KeyValue::new("rpc.psrpc.status_code", error.code.clone()));
                    span.set_attribute(KeyValue::new(
                        "rpc.grpc.status_code",
                        grpc_code(&error.code),
                    ));
                }
            }
            span.end();
        }
        self.completed = true;
    }
}
impl Drop for RpcSpan {
    fn drop(&mut self) {
        if !self.completed && !self.contexts.is_empty() {
            // Rust forcibly retires accepted futures on shutdown/expiry. End
            // exactly once, without inventing an Inbound event or a success.
            let expired = self.deadline.is_some_and(|d| Instant::now() >= d);
            let error = RpcError::new(
                if expired {
                    "deadline_exceeded"
                } else {
                    "canceled"
                },
                if expired {
                    "request timed out"
                } else {
                    "request canceled"
                },
            );
            self.finish(Some(&error), false);
        }
    }
}
fn grpc_code(code: &str) -> i64 {
    match code {
        "" | "ok" => 0,
        "canceled" => 1,
        "invalid_argument" | "malformed_request" | "upstream_client_error" => 3,
        "deadline_exceeded" => 4,
        "not_found" => 5,
        "already_exists" => 6,
        "permission_denied" => 7,
        "resource_exhausted" => 8,
        "failed_precondition" => 9,
        "aborted" => 10,
        "out_of_range" => 11,
        "unimplemented" => 12,
        "malformed_result" | "malformed_response" | "internal" | "upstream_server_error" => 13,
        "unavailable" => 14,
        "data_loss" => 15,
        "unauthenticated" => 16,
        _ => 2,
    }
}

#[cfg(test)]
mod tests;
