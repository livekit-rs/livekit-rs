//! Authenticated AV1 / DD-enabled SVC VP9 original-packet forwarding. DD frame IDs, dependency edges,
//! RTP boundaries and recoverable SN holes survive independently of assembly.
use crate::{
    av1::{OutPacketDescriptor, PacketSlot},
    codecs,
    engine::MediaEngine,
    forward_timing::ForwardingAdmissions,
};
use rtc_core::{ids::TransportId, media_api::MediaEvent, model::StreamState};
use std::time::Instant;
use str0m::{
    format::Codec,
    media::{MediaPacket, Mid},
    rtp::RtpWrite,
};
impl MediaEngine {
    pub(crate) fn dependency_descriptor_negotiated(&self, tid: TransportId, mid: Mid) -> bool {
        self.transports
            .get(&tid)
            .and_then(|transport| transport.rtc.media(mid))
            .is_some_and(|media| {
                media.remote_extmap().iter().any(|(_, ext)| {
                matches!(ext, str0m::rtp::Extension::UnknownUri(uri, _) if uri == crate::av1::URI)
            })
            })
    }

    pub(crate) fn on_av1_packet(&mut self, tid: TransportId, mut data: MediaPacket, now: Instant) {
        let dequeued = Instant::now();
        let Some(&ui) = self.up_by_mid.get(&(tid, data.mid)) else {
            return;
        };
        let blank = data.packet.payload.is_empty();
        let codec = if blank {
            self.ups
                .get(ui)
                .and_then(|up| {
                    if up.mime.eq_ignore_ascii_case("video/VP9") {
                        Some(Codec::Vp9)
                    } else if up.mime.eq_ignore_ascii_case("video/AV1") {
                        Some(Codec::Av1)
                    } else {
                        None
                    }
                })
                .unwrap_or(data.params.spec().codec)
        } else {
            data.params.spec().codec
        };
        if !matches!(codec, Codec::Av1 | Codec::Vp9) {
            return;
        }
        let configured = self.dependency_descriptor_negotiated(tid, data.mid);
        let mime = codecs::mime_of(codec);
        // Non-DD-negotiated VP9 retains its established frame consumer. A
        // matching source which already entered DD processing owns its fallback.
        if codec == Codec::Vp9
            && !configured
            && !self.ups.get(ui).is_some_and(|up| {
                up.mime.eq_ignore_ascii_case(mime)
                    && (up.dd_source.engaged()
                        || up
                            .one_dd_layer(mime, data.rid)
                            .is_some_and(|layer| up.one_dd.engaged(layer)))
            })
        {
            // Ordinary VP9 continues to forward complete frames, while its
            // bitrate follows authenticated packet arrival independently.
            if !blank
                && (data.rid.is_some() || data.packet.payload[0] & 0x20 == 0)
                && crate::dd_fallback::vp9_valid(&data.packet.payload)
                && !self.codec_receiver_unavailable(ui)
                && let Some(up) = self.ups.get_mut(ui)
                && let Some(layer) = up.ordinary_vp9_bitrate_layer(data.rid)
            {
                up.record_bitrate_bytes(layer, data.logical_rtp_bytes, data.packet.timestamp);
            }
            return;
        }
        if !blank {
            self.codec_receiver_started(ui, mime, now);
        }
        if self.codec_receiver_unavailable(ui) {
            return;
        }
        if !blank {
            self.select_codec_receivers(ui, now);
        }
        if let Some(layer) = self.ups.get(ui).and_then(|up| {
            up.one_dd_layer(mime, data.rid).filter(|&layer| {
                configured || (up.mime.eq_ignore_ascii_case(mime) && up.one_dd.engaged(layer))
            })
        }) {
            if let Some(up) = self.ups.get_mut(ui) {
                if !blank && !up.mime.eq_ignore_ascii_case(mime) {
                    up.set_mime(mime, now);
                }
            }
            self.on_one_dd_packet(ui, tid, layer, &data, codec, configured, now);
            return;
        }
        // Buffer source-SN normalization occurs before DD parsing, independently
        // of each subscriber RTPMunger and without touching native SRTP/ROC state.
        {
            let Some(up) = self.ups.get_mut(ui) else {
                return;
            };
            if !blank && !up.mime.eq_ignore_ascii_case(mime) {
                up.set_mime(mime, now);
            }
            if !up.uses_av1_original_packets(data.rid) {
                return;
            }
            let Some(sequence) = up.dd_source.normalize_sequence(
                data.source_id,
                *data.packet.seq_no,
                data.packet.payload.is_empty(),
                data.received_out_of_order,
            ) else {
                return;
            };
            data.packet.seq_no = sequence.into();
            data.packet.header.sequence_number = sequence as u16;
        }
        let packet = &data.packet;
        let (parsed, input) = {
            let Some(up) = self.ups.get_mut(ui) else {
                return;
            };
            if !blank && !up.mime.eq_ignore_ascii_case(mime) {
                up.set_mime(mime, now);
            }
            if !up.uses_av1_original_packets(data.rid) {
                return;
            }
            let input = up.dd_source.observe(
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
            let parsed = match input {
                crate::dd_fallback::Input::Descriptor => {
                    let Some(value) = up.av1_packets.parse(
                        &packet.header.ext_vals,
                        *packet.seq_no,
                        now,
                        self.cfg.rtc.enable_rtp_stream_restart_detection,
                    ) else {
                        return;
                    };
                    Some(value)
                }
                _ => None,
            };
            (parsed, input)
        };
        if input == crate::dd_fallback::Input::Padding {
            // Only a genuine out-of-order, nonduplicate initialized padding
            // packet survives Buffer's queue boundary. It has no DD but must
            // not disable the VP9 Buffer parser itself.
            if data.received_out_of_order {
                self.on_dd_payload_packet(tid, &data, now, configured, true, None, codec);
            }
            return;
        }
        let Some(parsed) = parsed else {
            self.on_dd_payload_packet(tid, &data, now, configured, true, None, codec);
            return;
        };
        let observation = packet.forwarding_observation().map(|mut p| {
            p.spatial = Some(parsed.spatial());
            p.temporal = Some(parsed.temporal());
            p
        });
        let mut admissions = ForwardingAdmissions::new(observation.as_slice());
        let mut changed = false;
        let vp9_dd_layers_changed;
        {
            let Self {
                ups,
                downs,
                transports,
                events,
                dirty,
                cfg,
                io,
                ..
            } = self;
            let Some(up) = ups.get_mut(ui) else {
                return;
            };
            if !up.started {
                up.started = true;
                events.push(MediaEvent::TrackMediaStarted {
                    track_sid: up.sid.clone(),
                    mime_type: up.mime.clone(),
                    layers: up.layers.clone(),
                });
            }
            if transports.get(&tid).is_some_and(|t| t.spec.one_shot) {
                if let Some(sizes) = parsed.resolutions() {
                    let multiple =
                        crate::video_dimensions::multiple_spatial_layers(&up.spec.codecs, &up.mime);
                    if let Some(sizes) = up.media_sizes.observe(
                        &up.mime,
                        i32::from(parsed.spatial()),
                        multiple,
                        sizes,
                    ) {
                        events.push(MediaEvent::TrackVideoSizeChanged {
                            track_sid: up.sid.clone(),
                            mime_type: up.mime.clone(),
                            sizes,
                        });
                    }
                }
            }
            let before_active_dd = up.active_vp9_dd_layers();
            let before_vp9_packet = before_active_dd
                .or_else(|| up.unrecognized_vp9_layer_mode().then(|| up.available(now)));
            up.on_av1_packet(
                &parsed,
                packet.time,
                packet.payload.len(),
                data.logical_rtp_bytes,
                now,
            );
            if up.muted {
                return;
            }
            vp9_dd_layers_changed =
                before_active_dd.is_some_and(|before| Some(before) != up.active_vp9_dd_layers());
            let available = up.available(now);
            let configured_layers = up.configured_layers();
            let mut want_pli = false;
            for &di in &up.subscribers {
                let Some(d) = downs.get_mut(di) else {
                    continue;
                };
                if !d.negotiated || d.codec_failed {
                    continue;
                }
                d.dd_fallback.sync(data.source_id, codec, configured);
                if d.dd_fallback.disabled {
                    continue;
                }

                let (Some(target), Some(mid)) = (d.transport, d.mid) else {
                    continue;
                };
                // Go broadcasts before the per-layer tracker sees this packet.
                // A live subscription uses the earlier availability, then may
                // target the newly observed layer for subsequent packets.
                let selection_availability = before_vp9_packet
                    .filter(|previous| d.current >= 0 && previous[d.current as usize])
                    .unwrap_or(available);
                d.refresh_target(selection_availability, configured_layers);
                if d.target < 0 {
                    continue;
                }
                let max_temporal = d.subscriber_temporal_limit(up, d.max_spatial);
                // Before a target has received its first frame, do not turn a
                // high subscription into S0/S1 merely because those independent
                // packets arrived earlier in this same SVC picture. Original DD
                // selection uses its requested target, then checks this packet's
                // DTI; actual availability still controls an already live target.
                let target_spatial = if up.svc {
                    if d.current < 0 {
                        d.effective_max()
                    } else {
                        d.target
                    }
                } else {
                    2
                };
                let Some(t) = transports.get_mut(&target) else {
                    continue;
                };
                if !t.usable() || !t.can_send(mid) {
                    continue;
                }
                let out_pt = {
                    let Some(writer) = t.rtc.writer(mid) else {
                        continue;
                    };
                    writer
                        .match_params(data.params)
                        .filter(|pt| writer.payload_params().any(|p| p.pt() == *pt))
                };
                let Some(out_pt) = out_pt else {
                    continue;
                };
                let selection = d.av1_packet_selector.select(
                    &parsed,
                    target_spatial,
                    max_temporal,
                    packet.header.marker,
                );
                if selection_availability != available {
                    d.refresh_target(available, configured_layers);
                }
                let resuming = d.current < 0;
                if selection.is_none() && resuming {
                    want_pli = true;
                    continue;
                }
                let mut direct = t.rtc.direct_api();
                let Some(stream) = direct.stream_tx_by_mid(mid, None) else {
                    continue;
                };
                if !d.av1_packet_sequences.is_initialized() || (selection.is_some() && resuming) {
                    d.av1_packet_sequences
                        .reset(*packet.seq_no, *stream.next_seq_no());
                }
                let late = d.av1_packet_sequences.is_late(*packet.seq_no);
                let Some(slot) = d
                    .av1_packet_sequences
                    .translate(*packet.seq_no, selection.is_some())
                else {
                    continue;
                };
                let rtp_time = d.out_original_packet_time(packet.time, packet.timestamp, late);
                match (slot, selection) {
                    (PacketSlot::Media(sequence), Some(selection)) => {
                        let mut exts = packet.header.ext_vals.clone();
                        exts.mid = None;
                        exts.rid = None;
                        exts.rid_repair = None;
                        exts.transport_cc = None;
                        exts.abs_send_time = None;
                        exts.user_values.remove::<crate::av1::RawDescriptor>();
                        exts.user_values
                            .set(OutPacketDescriptor(selection.descriptor));
                        exts.play_delay_min = None;
                        exts.play_delay_max = None;
                        if let Some((min, max)) = t.spec.effective_playout_delay(cfg.playout_delay)
                        {
                            exts.play_delay_min =
                                Some(str0m::media::MediaTime::from_millis(u64::from(min)));
                            exts.play_delay_max =
                                Some(str0m::media::MediaTime::from_millis(u64::from(max)));
                        }
                        d.last_pt = Some((data.params.pt(), out_pt));
                        stream.write_rtp(
                            RtpWrite::new(
                                out_pt,
                                sequence.into(),
                                rtp_time.numer() as u32,
                                packet.timestamp,
                                packet.payload.clone(),
                            )
                            .marker(selection.marker)
                            .ext_vals(exts)
                            .csrc(&packet.header.csrc[..packet.header.csrc_count])
                            .nackable(true),
                        );
                        let keyframe = match codec {
                            Codec::Vp9 => crate::dd_fallback::vp9_key(&packet.payload),
                            Codec::Av1 => crate::dd_fallback::av1_key(&packet.payload),
                            _ => false,
                        };
                        if keyframe {
                            // Go DownTrack.WriteRTP: a forwarded keyframe packet ends
                            // the FlagStopRTXOnPLI throttle.
                            if stream.nack_throttled() {
                                tracing::debug!(
                                    path = "av1_packet_forward.rs",
                                    "keyframe packet forwarded: NACK throttle cleared"
                                );
                            }
                            stream.set_nack_throttled(false);
                        }
                        let before = (d.current, d.forwarded_temporal_cap());
                        d.commit_av1_packet_selection(
                            if up.svc {
                                i32::from(selection.spatial)
                            } else {
                                0
                            },
                            selection.temporal,
                            max_temporal,
                        );
                        changed |= before != (d.current, d.forwarded_temporal_cap());
                        admissions.accepted(
                            i32::from(parsed.spatial()),
                            parsed.temporal(),
                            i32::from(parsed.spatial()),
                        );
                        if resuming {
                            events.push(MediaEvent::StreamState {
                                subscriber: d.spec.subscriber.clone(),
                                publisher: d.spec.publisher.clone(),
                                track_sid: d.spec.track_sid.clone(),
                                state: StreamState::Active,
                            });
                        }
                    }
                    (PacketSlot::Padding(sequence), _) => {
                        stream.write_rtp(RtpWrite::padding_only(
                            out_pt,
                            sequence.into(),
                            rtp_time.numer() as u32,
                            packet.timestamp,
                        ));
                    }
                    _ => {}
                }
                if !dirty.contains(&target) {
                    dirty.push(target);
                }
            }
            if want_pli {
                crate::forward::request_pli(
                    transports,
                    up,
                    0,
                    &cfg.rtc.pli_throttle,
                    io,
                    dirty,
                    now,
                );
            }
        }
        self.record_forwarding_cohort(&admissions, Instant::now(), dequeued);
        if changed {
            self.update_dynacast(ui, now);
        }
        // AV1's Buffer may resume parsing valid descriptors while an existing
        // downtrack intentionally remains on its unfiltered payload selector.
        self.on_dd_payload_packet(
            tid,
            &data,
            now,
            configured,
            false,
            Some((i32::from(parsed.spatial()), parsed.temporal())),
            codec,
        );
        if vp9_dd_layers_changed {
            // The Go receiver broadcasts the boundary packet before observing
            // its layer change. Apply the allocation to subsequent packets.
            let subscribers: std::collections::HashSet<_> = self
                .ups
                .get(ui)
                .into_iter()
                .flat_map(|up| &up.subscribers)
                .filter_map(|index| self.downs.get(*index))
                .filter(|down| down.negotiated)
                .filter_map(|down| down.transport)
                .collect();
            for transport in subscribers {
                self.allocate_transport(transport, now);
            }
        }
    }
}
