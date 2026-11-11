//! Actual delete lifecycle event after store deletion. Fixed telemetry/events.go
//! 514-518 is analytics-only, not an ingress_ended/deleted public webhook.
use crate::workers::io_service::{IoEvent, IoEvents};
use std::sync::Arc;
pub struct IngressDeletionEvents(pub Arc<dyn IoEvents>);
impl super::gateway::IngressDeleted for IngressDeletionEvents {
    fn deleted(&self, info: lk_protocol::livekit::IngressInfo) {
        self.0.notify(IoEvent::IngressDeleted(Box::new(info)));
    }
}
