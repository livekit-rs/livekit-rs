//! Original packet forwarding for independent ONE/RID encodings.
use crate::{
    av1::{OutPacketDescriptor, PacketParsed, PacketReceiver, PacketSlot},
    dd_fallback::{Input, Source},
    engine::MediaEngine,
    forward_timing::ForwardingAdmissions,
};
use rtc_core::{ids::TransportId, media_api::MediaEvent, model::StreamState};
use std::time::Instant;
use str0m::{
    format::Codec,
    media::MediaPacket,
    rtp::{RtpWrite, rtcp::SenderInfo},
};

#[derive(Default)]
struct Encoding {
    identity: u64,
    ssrc: u32,
    source: Source,
    parser: PacketReceiver,
    report: Option<SenderInfo>,
}

#[derive(Default)]
pub(crate) struct Sources([Option<Box<Encoding>>; 3]);

impl Sources {
    pub fn engaged(&self, layer: usize) -> bool {
        self.0.get(layer).is_some_and(Option::is_some)
    }

    pub fn report(&mut self, layer: usize, report: SenderInfo) {
        if let Some(encoding) = self.0.get_mut(layer).and_then(Option::as_mut) {
            if encoding.ssrc == *report.ssrc {
                encoding.report = Some(report);
            }
        }
    }

    pub(crate) fn get_report(&self, layer: i32) -> Option<SenderInfo> {
        self.0.get(usize::try_from(layer).ok()?)?.as_ref()?.report
    }

    fn observe(
        &mut self,
        layer: usize,
        data: &MediaPacket,
        codec: Codec,
        configured: bool,
        now: Instant,
        restart: bool,
    ) -> Option<(u64, Option<PacketParsed>)> {
        let slot = self.0.get_mut(layer)?;
        let ssrc = *data.packet.header.ssrc;
        if slot
            .as_ref()
            .is_none_or(|encoding| encoding.identity != data.source_id || encoding.ssrc != ssrc)
        {
            *slot = Some(Box::new(Encoding {
                identity: data.source_id,
                ssrc,
                ..Encoding::default()
            }));
        }
        let encoding = slot.as_mut()?;
        if let Some(report) = data
            .packet
            .last_sender_info
            .filter(|report| *report.ssrc == ssrc)
        {
            encoding.report = Some(report);
        }
        let packet = &data.packet;
        let sequence = encoding.source.normalize_sequence(
            data.source_id,
            *packet.seq_no,
            packet.payload.is_empty(),
            data.received_out_of_order,
        )?;
        let input = encoding.source.observe(
            data.source_id,
            codec,
            configured,
            packet
                .header
                .ext_vals
                .user_values
                .get::<crate::av1::RawDescriptor>()
                .is_some(),
            !packet.payload.is_empty(),
        );
        let parsed = if input == Input::Descriptor {
            Some(
                encoding
                    .parser
                    .parse(&packet.header.ext_vals, sequence, now, restart)?,
            )
        } else {
            None
        };
        Some((sequence, parsed))
    }
}

fn reference_timestamp(input: u32, reference: SenderInfo, target: SenderInfo) -> u32 {
    let nanos = match reference.ntp_time.duration_since(target.ntp_time) {
        Ok(duration) => duration.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    };
    let delta = nanos * i128::from(reference.rtp_time.frequency().get()) / 1_000_000_000;
    input.wrapping_add(
        (reference.rtp_time.numer() as u32)
            .wrapping_sub((target.rtp_time.numer() as u32).wrapping_add(delta as u32)),
    )
}

impl MediaEngine {
    pub(crate) fn on_one_dd_packet(
        &mut self,
        ui: usize,
        tid: TransportId,
        layer: usize,
        data: &MediaPacket,
        codec: Codec,
        configured: bool,
        now: Instant,
    ) {
        let dequeued = Instant::now();
        let packet = &data.packet;
        let blank = packet.payload.is_empty();
        let key = match codec {
            Codec::Vp9 => crate::dd_fallback::vp9_key(&packet.payload),
            Codec::Av1 => crate::dd_fallback::av1_key(&packet.payload),
            _ => false,
        };
        let one_shot = self
            .transports
            .get(&tid)
            .is_some_and(|transport| transport.spec.one_shot);
        // The arrival counter already includes this packet; observe it before
        // availability is used, as Go's receiver does per packet.
        let arrivals = self.transports.get_mut(&tid).and_then(|t| {
            t.rtc
                .direct_api()
                .stream_rx_by_mid(data.mid, data.rid)
                .map(|stream| stream.media_packets_received())
        });
        let Some(up) = self.ups.get_mut(ui) else {
            return;
        };
        if let Some(count) = arrivals.filter(|_| up.packet_tracking_eligible()) {
            let spatial = up.tracker_spatial(data.rid);
            up.observe_layer_count(spatial, count, now);
        }
        let Some((sequence, parsed)) = up.one_dd.observe(
            layer,
            data,
            codec,
            configured,
            now,
            self.cfg.rtc.enable_rtp_stream_restart_detection,
        ) else {
            return;
        };
        // An invalid nonempty VP9 descriptor is rejected by the source Buffer.
        // After real DD absence, that Buffer parses the payload before forwarding.
        if !blank
            && parsed.is_none()
            && codec == Codec::Vp9
            && !crate::dd_fallback::vp9_valid(&packet.payload)
        {
            return;
        }
        let spatial = layer as i32;
        let temporal = parsed.as_ref().map_or(0, PacketParsed::temporal);
        if !blank {
            if !up.started {
                up.started = true;
                self.events.push(MediaEvent::TrackMediaStarted {
                    track_sid: up.sid.clone(),
                    mime_type: up.mime.clone(),
                    layers: up.layers.clone(),
                });
            }
            if one_shot {
                if let Some(sizes) = parsed.as_ref().and_then(PacketParsed::resolutions) {
                    if let Some(sizes) = up.media_sizes.observe(&up.mime, spatial, false, sizes) {
                        self.events.push(MediaEvent::TrackVideoSizeChanged {
                            track_sid: up.sid.clone(),
                            mime_type: up.mime.clone(),
                            sizes,
                        });
                    }
                }
            }
            // The independent Go tracker charges the complete logical RTP packet.
            up.on_frame(spatial, data.rid, data.logical_rtp_bytes, now);
        }
        if up.muted {
            return;
        }
        let available = up.available(now);
        let configured_layers = up.configured_layers();
        let incomplete = up.one_dd_incomplete_sr();
        let observation = packet.forwarding_observation().map(|mut sample| {
            sample.spatial = Some(layer as u8);
            sample.temporal = Some(temporal);
            sample
        });
        let mut admissions = ForwardingAdmissions::new(observation.as_slice());
        let mut changed = false;
        let mut want_pli = [false; 3];
        for &di in &up.subscribers {
            let Some(down) = self.downs.get_mut(di) else {
                continue;
            };
            if !down.negotiated || down.codec_failed {
                continue;
            }
            let (Some(target), Some(mid)) = (down.transport, down.mid) else {
                continue;
            };
            let Some(transport) = self.transports.get_mut(&target) else {
                continue;
            };
            if !transport.usable() || !transport.can_send(mid) {
                continue;
            }
            down.refresh_target(available, configured_layers);
            // A ONE/RID selector retires DD on the current nonempty missing
            // packet and still evaluates that packet using outer Simulcast.
            if !configured || (!blank && parsed.is_none()) {
                down.one_dd.disabled = true;
            }
            let chosen = down.one_dd_spatial(spatial, key, now);
            if chosen != Some(spatial) || !configured_layers[spatial.clamp(0, 2) as usize] {
                if down.wants_keyframe() {
                    want_pli[down.target.clamp(0, 2) as usize] = true;
                }
                continue;
            }
            let source_changed = down.one_dd.source != Some(data.source_id);
            let resumed = down.current < 0;
            if source_changed
                && (data.received_out_of_order || (down.one_dd.source.is_some() && !key))
            {
                continue;
            }
            let out_pt = {
                let Some(writer) = transport.rtc.writer(mid) else {
                    continue;
                };
                if blank {
                    down.last_pt.map(|(_, pt)| pt).filter(|pt| {
                        writer
                            .payload_params()
                            .any(|params| params.pt() == *pt && params.spec().codec == codec)
                    })
                } else {
                    writer
                        .match_params(data.params)
                        .filter(|pt| writer.payload_params().any(|params| params.pt() == *pt))
                }
            };
            let Some(out_pt) = out_pt else {
                continue;
            };
            let mut direct = transport.rtc.direct_api();
            let Some(stream) = direct.stream_tx_by_mid(mid, None) else {
                continue;
            };
            if source_changed && down.one_dd.source.is_some() {
                let mapped = if incomplete {
                    None
                } else {
                    let reference = down.one_dd.reference.unwrap_or(spatial);
                    if reference == spatial {
                        Some(packet.header.timestamp)
                    } else {
                        let (Some(reference), Some(target)) = (
                            up.one_dd.get_report(reference),
                            up.one_dd.get_report(spatial),
                        ) else {
                            continue;
                        };
                        Some(reference_timestamp(
                            packet.header.timestamp,
                            reference,
                            target,
                        ))
                    }
                };
                if !down.rebase_one_dd_clock(packet.time, mapped) {
                    continue;
                }
            }
            if source_changed || resumed || !down.av1_packet_sequences.is_initialized() {
                down.av1_packet_sequences
                    .reset(sequence, *stream.next_seq_no());
                down.one_dd.start_source(data.source_id, spatial);
            }
            let cap = down.subscriber_temporal_limit(up, spatial);
            let selected = if !down.one_dd.disabled {
                match &parsed {
                    Some(parsed) => down.one_dd.select(
                        parsed,
                        cap,
                        key,
                        data.received_out_of_order,
                        packet.header.marker,
                    ),
                    None => Some(0),
                }
            } else {
                Some(0)
            };
            let late = down.av1_packet_sequences.is_late(sequence);
            let Some(slot) = down
                .av1_packet_sequences
                .translate(sequence, selected.is_some())
            else {
                continue;
            };
            let time = down.out_original_packet_time(packet.time, packet.timestamp, late);
            match slot {
                PacketSlot::Media(sequence) if !blank => {
                    let Some(output_temporal) = selected else {
                        continue;
                    };
                    let mut extensions = packet.header.ext_vals.clone();
                    extensions.mid = None;
                    extensions.rid = None;
                    extensions.rid_repair = None;
                    extensions.transport_cc = None;
                    extensions.abs_send_time = None;
                    extensions.user_values.remove::<crate::av1::RawDescriptor>();
                    extensions.user_values.remove::<OutPacketDescriptor>();
                    if let Some(descriptor) = parsed
                        .as_ref()
                        .filter(|_| !down.one_dd.disabled)
                        .and_then(|parsed| {
                            down.one_dd
                                .descriptor(data.source_id, parsed, output_temporal)
                        })
                    {
                        extensions.user_values.set(descriptor);
                    }
                    extensions.play_delay_min = None;
                    extensions.play_delay_max = None;
                    if let Some((min, max)) = transport
                        .spec
                        .effective_playout_delay(self.cfg.playout_delay)
                    {
                        extensions.play_delay_min =
                            Some(str0m::media::MediaTime::from_millis(u64::from(min)));
                        extensions.play_delay_max =
                            Some(str0m::media::MediaTime::from_millis(u64::from(max)));
                    }
                    down.last_pt = Some((data.params.pt(), out_pt));
                    stream.write_rtp(
                        RtpWrite::new(
                            out_pt,
                            sequence.into(),
                            time.numer() as u32,
                            packet.timestamp,
                            packet.payload.clone(),
                        )
                        .marker(packet.header.marker)
                        .ext_vals(extensions)
                        .csrc(&packet.header.csrc[..packet.header.csrc_count])
                        .nackable(true),
                    );
                    if key {
                        // Go DownTrack.WriteRTP: a forwarded keyframe packet ends
                        // the FlagStopRTXOnPLI throttle.
                        if stream.nack_throttled() {
                            tracing::debug!(
                                path = "one_dd_forward.rs",
                                "keyframe packet forwarded: NACK throttle cleared"
                            );
                        }
                        stream.set_nack_throttled(false);
                    }
                    let previous = (down.current, down.forwarded_temporal_cap());
                    down.commit_av1_packet_selection(spatial, output_temporal, cap);
                    changed |= previous != (down.current, down.forwarded_temporal_cap());
                    admissions.accepted(spatial, temporal, spatial);
                    if resumed {
                        self.events.push(MediaEvent::StreamState {
                            subscriber: down.spec.subscriber.clone(),
                            publisher: down.spec.publisher.clone(),
                            track_sid: down.spec.track_sid.clone(),
                            state: StreamState::Active,
                        });
                    }
                }
                PacketSlot::Media(sequence) | PacketSlot::Padding(sequence) => {
                    stream.write_rtp(RtpWrite::padding_only(
                        out_pt,
                        sequence.into(),
                        time.numer() as u32,
                        packet.timestamp,
                    ))
                }
            }
            if !self.dirty.contains(&target) {
                self.dirty.push(target);
            }
        }
        for (layer, wanted) in want_pli.into_iter().enumerate() {
            if wanted {
                crate::forward::request_pli(
                    &mut self.transports,
                    up,
                    layer as i32,
                    &self.cfg.rtc.pli_throttle,
                    &mut self.io,
                    &mut self.dirty,
                    now,
                );
            }
        }
        self.record_forwarding_cohort(&admissions, Instant::now(), dequeued);
        if changed {
            self.update_dynacast(ui, now);
        }
    }
}
