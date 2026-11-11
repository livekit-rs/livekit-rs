//! Receiver identity and sticky codec-regression decisions for one logical track.
//! Fixed mediatrack.go evaluates the current room size only when a codec first
//! acquires an actual receiver. A later participant-count change must not invent
//! another AddReceiver event or reverse an already committed regression.
use rtc_core::model::BackupCodecPolicy;

#[derive(Clone, Debug)]
pub(crate) struct Receiver {
    pub mime: String,
    pub up: usize,
    pub started: bool,
    pub invalid: bool,
    pub regress_to: Option<usize>,
}

#[derive(Default)]
pub(crate) struct CodecReceivers {
    pub receivers: Vec<Receiver>,
    pub regressed_target: Option<usize>,
    pub target_received: bool,
}

impl CodecReceivers {
    pub fn find(&self, mime: &str) -> Option<usize> {
        self.receivers
            .iter()
            .find(|r| r.mime.eq_ignore_ascii_case(mime))
            .map(|r| r.up)
    }
    pub fn bind(&mut self, mime: &str, up: usize) {
        self.receivers
            .retain(|receiver| receiver.up != up || receiver.mime.eq_ignore_ascii_case(mime));
        if let Some(existing) = self
            .receivers
            .iter_mut()
            .find(|r| r.mime.eq_ignore_ascii_case(mime))
        {
            existing.up = up;
        } else {
            self.receivers.push(Receiver {
                mime: mime.to_ascii_lowercase(),
                up,
                started: false,
                invalid: false,
                regress_to: None,
            });
        }
    }
    /// Returns the real backup receiver only on a first-codec-receiver event
    /// that enables regression; a sticky target also applies to later codecs.
    pub fn started(
        &mut self,
        up: usize,
        policy: BackupCodecPolicy,
        target_mime: Option<&str>,
        threshold: i64,
        participants: usize,
    ) -> Option<usize> {
        let receiver = self.receivers.iter_mut().find(|r| r.up == up)?;
        if receiver.started {
            return None;
        }
        receiver.started = true;
        let enabled = policy == BackupCodecPolicy::Regression
            || (policy == BackupCodecPolicy::PreferRegression
                && (threshold == 0 || (participants as i128) < threshold as i128));
        if !enabled {
            return None;
        }
        if target_mime.is_some_and(|target| receiver.mime.eq_ignore_ascii_case(target)) {
            self.regressed_target = Some(up);
            self.target_received = true;
        }
        self.regressed_target
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_zero_negative_and_strict_room_threshold_only_on_new_receiver() {
        for (threshold, count, regresses) in [
            (0, 100, true),
            (-1, 0, false),
            (5, 4, true),
            (5, 5, false),
            (5, 6, false),
        ] {
            let mut group = CodecReceivers::default();
            group.bind("video/vp9", 1);
            group.bind("video/vp8", 2);
            assert_eq!(
                group.started(
                    1,
                    BackupCodecPolicy::PreferRegression,
                    Some("video/vp8"),
                    threshold,
                    count
                ),
                None
            );
            assert_eq!(
                group.started(
                    2,
                    BackupCodecPolicy::PreferRegression,
                    Some("video/vp8"),
                    threshold,
                    count
                ),
                regresses.then_some(2)
            );
            // Changing room size is not a new codec receiver event.
            assert_eq!(
                group.started(
                    2,
                    BackupCodecPolicy::PreferRegression,
                    Some("video/vp8"),
                    0,
                    0
                ),
                None
            );
        }
    }
    #[test]
    fn unconditional_regression_simulcast_and_late_primary_use_source_policy() {
        for policy in [BackupCodecPolicy::Regression, BackupCodecPolicy::Simulcast] {
            let mut group = CodecReceivers::default();
            group.bind("video/vp8", 2);
            let target = (policy == BackupCodecPolicy::Regression).then_some(2);
            assert_eq!(group.started(2, policy, Some("video/vp8"), -1, 100), target);
            group.bind("video/vp9", 1);
            assert_eq!(group.started(1, policy, Some("video/vp8"), -1, 100), target);
            assert_eq!(group.find("VIDEO/VP8"), Some(2));
        }
    }
}

use crate::engine::MediaEngine;
use rtc_core::{
    ids::TrackSid,
    model::{SubscribedCodec, VideoQuality},
};
use std::time::Instant;

impl MediaEngine {
    pub(crate) fn codec_quality_snapshot(&self, sid: &TrackSid) -> Option<Vec<SubscribedCodec>> {
        let group = self.codec_receivers.get(sid)?;
        let spec = self.published.get(sid).or_else(|| {
            group
                .receivers
                .first()
                .and_then(|r| self.ups.get(r.up))
                .map(|up| &up.spec)
        })?;
        if spec.codecs.len() < 2 {
            return None;
        }
        let mut result = Vec::new();
        for codec in &spec.codecs {
            let message = group
                .find(&codec.mime_type)
                .and_then(|ui| {
                    if group
                        .receivers
                        .iter()
                        .any(|receiver| receiver.up == ui && receiver.invalid)
                        || group.regressed_target.is_some_and(|target| target != ui)
                    {
                        return None;
                    }
                    self.ups.get(ui)?.dynacast.as_ref()?.committed_message()
                })
                .unwrap_or_else(|| {
                    // A negotiated subscriber that cannot receive the primary
                    // codec still requests its declared backup before media exists.
                    let demand = if group.regressed_target.is_some()
                        || group.receivers.iter().any(|receiver| {
                            receiver.mime.eq_ignore_ascii_case(&codec.mime_type) && receiver.invalid
                        }) {
                        VideoQuality::Off
                    } else {
                        crate::dynacast::aggregate(self.downs.iter().filter_map(|(di, down)| {
                            if &down.spec.track_sid != sid || !down.negotiated {
                                return None;
                            }
                            let supported = self.subscriber_codec_supported(di, &codec.mime_type);
                            let current = self.ups.get(down.up)?;
                            (supported && !self.receiver_codec_supported(di, down.up))
                                .then(|| current.quality_for_spatial(down.demand_spatial()))
                        }))
                    };
                    crate::dynacast::message(
                        &crate::codecs::dynacast_codec_name(&codec.mime_type),
                        demand,
                    )
                });
            result.extend(message);
        }
        (!result.is_empty()).then_some(result)
    }

    pub(crate) fn codec_receiver_started(&mut self, ui: usize, observed_mime: &str, now: Instant) {
        let Some(up) = self.ups.get(ui) else {
            return;
        };
        let sid = up.sid.clone();
        let policy = up.spec.backup_codec_policy;
        let target = up.spec.codecs.get(1).map(|codec| codec.mime_type.clone());
        let new_receiver = self.codec_receivers.get(&sid).is_some_and(|group| {
            group
                .receivers
                .iter()
                .any(|receiver| receiver.up == ui && !receiver.started)
        });
        if new_receiver && !observed_mime.is_empty() {
            self.ups.get_mut(ui).unwrap().set_mime(observed_mime, now);
        }
        let regressed = self.codec_receivers.get_mut(&sid).and_then(|group| {
            // SDP/CID selects a potential receiver; only authenticated actual
            // payload codec can prove arrival of the regression target.
            if let Some(receiver) = group
                .receivers
                .iter_mut()
                .find(|receiver| receiver.up == ui)
            {
                if !receiver.started && !observed_mime.is_empty() {
                    receiver.mime = observed_mime.to_ascii_lowercase();
                }
            }
            group.started(
                ui,
                policy,
                target.as_deref(),
                self.cfg.video.codec_regression_threshold,
                self.room_participant_count,
            )
        });
        if let Some(target) = regressed {
            self.up_by_sid.insert(sid.clone(), target);
            let sources = self.codec_receivers[&sid]
                .receivers
                .iter()
                .map(|receiver| receiver.up)
                .filter(|source| *source != target)
                .collect::<Vec<_>>();
            for source in sources {
                let subscribers = if let Some(up) = self.ups.get_mut(source) {
                    // A regressed codec is stopped, not merely debounced OFF.
                    up.dynacast = None;
                    std::mem::take(&mut up.subscribers)
                } else {
                    Vec::new()
                };
                for down in subscribers {
                    self.move_codec_subscription(down, target, now);
                }
            }
            self.refresh_down_layers(target, now);
            self.update_dynacast(target, now);
        }
        self.activate_invalid_receivers(&sid, now);
    }

    pub(crate) fn invalidate_codec_receiver(&mut self, ui: usize, now: Instant) {
        let Some(sid) = self.ups.get(ui).map(|up| up.sid.clone()) else {
            return;
        };
        let Some(receiver) = self.codec_receivers.get_mut(&sid).and_then(|group| {
            group
                .receivers
                .iter_mut()
                .find(|receiver| receiver.up == ui)
        }) else {
            return;
        };
        if receiver.invalid {
            return;
        }
        receiver.invalid = true;
        // Source treats Invalid as terminal, independent of whether the old PT
        // later returns. Only a distinct available backup can receive its users.
        if let Some(up) = self.ups.get_mut(ui) {
            up.dynacast = None;
        }
        self.activate_invalid_receivers(&sid, now);
    }
    fn activate_invalid_receivers(&mut self, sid: &TrackSid, now: Instant) {
        let Some(spec) = self.published.get(sid).or_else(|| {
            self.up_by_sid
                .get(sid)
                .and_then(|ui| self.ups.get(*ui))
                .map(|up| &up.spec)
        }) else {
            return;
        };
        if spec.backup_codec_policy == BackupCodecPolicy::Simulcast {
            return;
        }
        let Some(target_mime) = spec.codecs.get(1).map(|codec| codec.mime_type.clone()) else {
            return;
        };
        let Some(group) = self.codec_receivers.get(sid) else {
            return;
        };
        let Some(target) = group.find(&target_mime) else {
            return;
        };
        if group
            .receivers
            .iter()
            .any(|receiver| receiver.up == target && receiver.invalid)
        {
            return;
        }
        let old = group
            .receivers
            .iter()
            .filter(|receiver| {
                receiver.invalid && receiver.up != target && receiver.regress_to.is_none()
            })
            .map(|receiver| receiver.up)
            .collect::<Vec<_>>();
        for old in old {
            if let Some(receiver) = self
                .codec_receivers
                .get_mut(sid)
                .unwrap()
                .receivers
                .iter_mut()
                .find(|receiver| receiver.up == old)
            {
                receiver.regress_to = Some(target);
            }
            let subscribers = self
                .ups
                .get_mut(old)
                .map(|up| std::mem::take(&mut up.subscribers))
                .unwrap_or_default();
            for down in subscribers {
                let renegotiate = self
                    .downs
                    .get(down)
                    .filter(|d| d.negotiated)
                    .and_then(|d| self.transports.get(&d.transport?))
                    .is_some_and(|t| t.spec.codec_switch_requires_renegotiation);
                if renegotiate {
                    self.retire_receiver_report_source(down);
                    self.detach_down(down, now);
                    let target_up = self.ups.get(target).expect("active backup receiver");
                    let d = self.downs.get_mut(down).expect("negotiated downtrack");
                    d.replace_sender(target, target_up, now);
                    self.ups.get_mut(target).unwrap().subscribers.push(down);
                    self.attach_down(down, now);
                } else {
                    self.move_codec_subscription(down, target, now);
                }
            }
            if self.up_by_sid.get(sid) == Some(&old) {
                self.up_by_sid.insert(sid.clone(), target);
            }
            self.refresh_down_layers(target, now);
            self.update_dynacast(target, now);
        }
    }
    pub(crate) fn codec_receiver_unavailable(&self, ui: usize) -> bool {
        self.ups
            .get(ui)
            .and_then(|up| self.codec_receivers.get(&up.sid))
            .is_some_and(|group| {
                group
                    .receivers
                    .iter()
                    .any(|receiver| receiver.up == ui && receiver.invalid)
                    || group.regressed_target.is_some_and(|target| target != ui)
            })
    }

    pub(crate) fn receiver_codec_supported(&self, down: usize, up: usize) -> bool {
        self.ups
            .get(up)
            .is_some_and(|up| self.subscriber_codec_supported(down, &up.mime))
    }

    fn subscriber_codec_supported(&self, down: usize, mime: &str) -> bool {
        let Some(down) = self.downs.get(down) else {
            return false;
        };
        let (Some(tid), Some(mid)) = (down.transport, down.mid) else {
            return false;
        };
        let Some(transport) = self.transports.get(&tid) else {
            return false;
        };
        let Some(media) = transport.rtc.media(mid) else {
            return false;
        };
        // Same negotiated m-line filter as Writer.payload_params, without
        // taking a mutable media writer just to inspect its negotiated codecs.
        media.remote_pts().iter().any(|pt| {
            transport.rtc.codec_config().params().iter().any(|param| {
                param.pt() == *pt
                    && crate::codecs::mime_of(param.spec().codec).eq_ignore_ascii_case(mime)
            })
        })
    }

    pub(crate) fn select_codec_receivers(&mut self, ui: usize, now: Instant) {
        let Some(up) = self.ups.get(ui) else {
            return;
        };
        let sid = up.sid.clone();
        let Some(group) = self.codec_receivers.get(&sid) else {
            return;
        };
        if group.regressed_target.is_some() {
            return;
        }
        let candidates = group
            .receivers
            .iter()
            .filter(|r| r.up != ui)
            .flat_map(|r| {
                self.ups
                    .get(r.up)
                    .into_iter()
                    .flat_map(|u| u.subscribers.iter().copied())
            })
            .collect::<Vec<_>>();
        for di in candidates {
            let old = self.downs.get(di).unwrap().up;
            if !self.receiver_codec_supported(di, old) && self.receiver_codec_supported(di, ui) {
                self.move_codec_subscription(di, ui, now);
                self.update_dynacast(old, now);
                self.update_dynacast(ui, now);
            }
        }
    }

    fn move_codec_subscription(&mut self, di: usize, target: usize, now: Instant) {
        let Some(up) = self.ups.get(target) else {
            return;
        };
        let layers = up.layers.clone();
        let mime = up.mime.clone();
        let muted = up.muted;
        let Some(down) = self.downs.get_mut(di) else {
            return;
        };
        let previous = down.up;
        down.up = target;
        down.last_pt = None;
        down.codec_failed = false;
        down.av1 = Default::default();
        down.av1_packet_selector = Default::default();
        down.av1_packet_sequences = Default::default();
        down.one_dd = Default::default();
        down.pub_muted = muted;
        down.scorer.set_mime(&mime);
        // Keep SDP, SSRC, RTP clock continuity and bound lifecycle intact; the
        // new codec must pass its own keyframe gate before any media forwarding.
        down.stop_forwarding();
        down.apply_settings_layers(&layers);
        if let Some(up) = self.ups.get_mut(previous) {
            up.subscribers.retain(|other| *other != di);
        }
        if let Some(up) = self.ups.get_mut(target) {
            if !up.subscribers.contains(&di) {
                up.subscribers.push(di);
            }
        }
        self.configure_red(di, now);
    }
}

#[cfg(test)]
mod original_go_observer {
    use super::*;
    #[test]
    #[ignore = "requires independently executed original LiveKit room/MediaTrack observer"]
    fn original_livekit_room_and_track_policy_results() {
        let text = std::fs::read_to_string(
            std::env::var("GO_CODEC_GATE_RESULTS").expect("original Go observer receipt path"),
        )
        .unwrap();
        let rows = text
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_CODEC_GATE "))
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            27,
            "21 original actual-room boundaries + 6 original MediaTrack policy branches"
        );
        for row in rows {
            let mut receivers = CodecReceivers::default();
            receivers.bind("video/vp8", 2);
            let (policy, threshold, count, expected) = if row["kind"] == "room" {
                (
                    BackupCodecPolicy::PreferRegression,
                    row["threshold"].as_i64().unwrap(),
                    row["count"].as_u64().unwrap() as usize,
                    row["allows"].as_bool().unwrap(),
                )
            } else {
                let policy = match row["policy"].as_i64().unwrap() {
                    0 => BackupCodecPolicy::PreferRegression,
                    1 => BackupCodecPolicy::Simulcast,
                    2 => BackupCodecPolicy::Regression,
                    value => panic!("original source unexpected policy {value}"),
                };
                (
                    policy,
                    if row["allows"].as_bool().unwrap() {
                        0
                    } else {
                        -1
                    },
                    0,
                    row["enabled"].as_bool().unwrap(),
                )
            };
            assert_eq!(
                receivers
                    .started(2, policy, Some("video/vp8"), threshold, count)
                    .is_some(),
                expected,
                "{row}"
            );
        }
    }
}
