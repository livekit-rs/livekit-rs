// Copyright 2023 LiveKit, Inc. SPDX-License-Identifier: Apache-2.0
//! Actual IOInfo event → public webhook mapping from telemetry/events.go.
//! Ingress Created/Updated are analytics-only in the fixed source, not public
//! webhook events. External worker media processing remains externally owned.
use crate::{
    webhook::{WebhookError, WebhookNotifier},
    workers::io_service::{IoEvent, IoEvents},
};
use lk_protocol::livekit as pb;

#[derive(Clone)]
pub struct WorkerNotifications {
    notifier: WebhookNotifier,
}
impl WorkerNotifications {
    pub fn new(notifier: WebhookNotifier) -> Self {
        Self { notifier }
    }
    /// Return an actual enqueue/key error to callers that need delivery setup
    /// feedback. As upstream, this does not claim a queued HTTP POST succeeded.
    pub fn notify_event(&self, event: IoEvent) -> Result<(), WebhookError> {
        let Some((event, extra)) = to_webhook(event) else {
            return Ok(());
        };
        self.notifier.notify_webhook_with_extras(event, &extra)
    }
}
impl IoEvents for WorkerNotifications {
    fn notify(&self, event: IoEvent) {
        if let Err(error) = self.notify_event(event) {
            tracing::warn!(%error, "failed to notify worker webhook");
        }
    }
}
/// Egress GetEgressNotifyOptions reads the actual selected request arm only.
/// The fixed notifier does not consume each WebhookConfig.filter_params field.
pub fn egress_webhooks(info: &pb::EgressInfo) -> &[pb::WebhookConfig] {
    match info.request.as_ref() {
        Some(pb::egress_info::Request::Egress(req)) => &req.webhooks,
        Some(pb::egress_info::Request::Replay(req)) => &req.webhooks,
        Some(pb::egress_info::Request::RoomComposite(req)) => &req.webhooks,
        Some(pb::egress_info::Request::Web(req)) => &req.webhooks,
        Some(pb::egress_info::Request::Participant(req)) => &req.webhooks,
        Some(pb::egress_info::Request::TrackComposite(req)) => &req.webhooks,
        Some(pb::egress_info::Request::Track(req)) => &req.webhooks,
        None => &[],
    }
}
fn egress(event: &str, info: pb::EgressInfo) -> (pb::WebhookEvent, Vec<pb::WebhookConfig>) {
    let extra = egress_webhooks(&info).to_vec();
    (
        pb::WebhookEvent {
            event: event.into(),
            room: Some(pb::Room {
                sid: info.room_id.clone(),
                name: info.room_name.clone(),
                ..Default::default()
            }),
            egress_info: Some(info),
            ..Default::default()
        },
        extra,
    )
}
fn ingress(event: &str, info: pb::IngressInfo) -> (pb::WebhookEvent, Vec<pb::WebhookConfig>) {
    (
        pb::WebhookEvent {
            event: event.into(),
            room: Some(pb::Room {
                name: info.room_name.clone(),
                ..Default::default()
            }),
            ingress_info: Some(info),
            ..Default::default()
        },
        Vec::new(),
    )
}
pub fn to_webhook(event: IoEvent) -> Option<(pb::WebhookEvent, Vec<pb::WebhookConfig>)> {
    match event {
        IoEvent::EgressStarted(info) => Some(egress("egress_started", *info)),
        IoEvent::EgressUpdated(info) => Some(egress("egress_updated", *info)),
        IoEvent::EgressEnded(info) => Some(egress("egress_ended", *info)),
        IoEvent::IngressStarted(info) => Some(ingress("ingress_started", *info)),
        IoEvent::IngressEnded(info) => Some(ingress("ingress_ended", *info)),
        IoEvent::IngressCreated(_) | IoEvent::IngressUpdated(_) | IoEvent::IngressDeleted(_) => {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_worker_events_have_minimal_rooms_and_analytics_do_not_invent_public_events() {
        let e = pb::EgressInfo {
            egress_id: "EG_test".into(),
            room_id: "RM_test".into(),
            room_name: "room".into(),
            ..Default::default()
        };
        for (event, name) in [
            (
                IoEvent::EgressStarted(Box::new(e.clone())),
                "egress_started",
            ),
            (
                IoEvent::EgressUpdated(Box::new(e.clone())),
                "egress_updated",
            ),
            (IoEvent::EgressEnded(Box::new(e.clone())), "egress_ended"),
        ] {
            let (actual, _) = to_webhook(event).unwrap();
            assert_eq!(actual.event, name);
            assert_eq!(actual.egress_info, Some(e.clone()));
            assert_eq!(
                actual.room,
                Some(pb::Room {
                    sid: "RM_test".into(),
                    name: "room".into(),
                    ..Default::default()
                })
            );
            assert!(actual.id.is_empty());
            assert_eq!(actual.created_at, 0);
        }
        let i = pb::IngressInfo {
            ingress_id: "IN_test".into(),
            room_name: "room".into(),
            ..Default::default()
        };
        assert!(to_webhook(IoEvent::IngressCreated(Box::new(i.clone()))).is_none());
        assert!(to_webhook(IoEvent::IngressUpdated(Box::new(i.clone()))).is_none());
        for (event, name) in [
            (
                IoEvent::IngressStarted(Box::new(i.clone())),
                "ingress_started",
            ),
            (IoEvent::IngressEnded(Box::new(i.clone())), "ingress_ended"),
        ] {
            let (actual, extra) = to_webhook(event).unwrap();
            assert_eq!(actual.event, name);
            assert!(extra.is_empty());
            assert_eq!(
                actual.room,
                Some(pb::Room {
                    name: "room".into(),
                    ..Default::default()
                })
            );
            assert_eq!(actual.ingress_info, Some(i.clone()));
        }
    }
    #[test]
    fn all_seven_egress_request_arms_supply_only_their_own_extra_webhooks() {
        let webhooks = vec![pb::WebhookConfig {
            url: "https://fixture.example".into(),
            signing_key: "fixture-key".into(),
            ..Default::default()
        }];
        let arms = [
            pb::egress_info::Request::Egress(pb::StartEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::Replay(pb::ExportReplayRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::RoomComposite(pb::RoomCompositeEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::Web(pb::WebEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::Participant(pb::ParticipantEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::TrackComposite(pb::TrackCompositeEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
            pb::egress_info::Request::Track(pb::TrackEgressRequest {
                webhooks: webhooks.clone(),
                ..Default::default()
            }),
        ];
        for arm in arms {
            let info = pb::EgressInfo {
                request: Some(arm),
                ..Default::default()
            };
            assert_eq!(egress_webhooks(&info), webhooks.as_slice());
        }
        assert!(egress_webhooks(&pb::EgressInfo::default()).is_empty());
    }
}
