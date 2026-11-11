//! Authenticated same-TCB restart, fixed Pion SCTP v1.12.0
//! association.go handleRestartInit/handleCookieEcho/restartAssociation.
//! Cookies are opaque on the wire. Their timestamp uses this TCB's monotonic
//! epoch rather than a wall clock, retaining Pion's one-minute age/tie-tag checks.
use super::*;
use crate::chunk::{
    COOKIE_RECEIVED_WHILE_SHUTTING_DOWN, ErrorCause, RESTART_OF_AN_ASSOCIATION_WITH_NEW_ADDRESSES,
    STALE_COOKIE_ERROR,
};
use crate::param::param_type::ParamType;
use bytes::{BufMut, BytesMut};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const COOKIE_HEADER: usize = 24;
const MAC_LENGTH: usize = 32;
const COOKIE_LIFETIME: Duration = Duration::from_secs(60);

impl Association {
    pub(super) fn valid_restart_packet(&self, packet: &Packet, chunk: &dyn Chunk) -> bool {
        if packet.common_header.source_port != self.destination_port
            || packet.common_header.destination_port != self.source_port
        {
            return false;
        }
        if chunk
            .as_any()
            .downcast_ref::<ChunkInit>()
            .is_some_and(|init| !init.is_ack)
            || chunk.as_any().is::<ChunkCookieEcho>()
        {
            return true;
        }
        packet.common_header.verification_tag == self.my_verification_tag
    }

    pub(super) fn handle_restart_init(
        &mut self,
        packet: &Packet,
        init: &ChunkInit,
        now: Instant,
    ) -> Result<Vec<Packet>> {
        if !matches!(
            self.state,
            AssociationState::Established
                | AssociationState::ShutdownPending
                | AssociationState::ShutdownSent
                | AssociationState::ShutdownReceived
        ) || packet.common_header.destination_port != self.source_port
            || packet.common_header.source_port != self.destination_port
        {
            // A rejected restart must not tear down the still-live TCB.
            return Ok(vec![]);
        }
        if init.params.iter().any(|param| {
            matches!(
                param.header().typ,
                ParamType::Ipv4Addr | ParamType::Ipv6Addr | ParamType::HostNameAddr
            )
        }) {
            let mut response = self.create_packet(vec![Box::new(ChunkAbort {
                error_causes: vec![ErrorCause {
                    code: RESTART_OF_AN_ASSOCIATION_WITH_NEW_ADDRESSES,
                    raw: Bytes::new(),
                }],
            })]);
            response.common_header.verification_tag = init.initiate_tag;
            return Ok(vec![response]);
        }
        let key = *self
            .restart_cookie_key
            .get_or_insert_with(random::<[u8; 32]>);
        let epoch = *self.restart_cookie_epoch.get_or_insert(now);
        let issued = now
            .saturating_duration_since(epoch)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let mut local_tag = random::<u32>();
        while local_tag == 0 || local_tag == self.my_verification_tag {
            local_tag = random::<u32>();
        }
        let mut raw = BytesMut::new();
        raw.put_u64(issued);
        raw.put_u32(self.my_verification_tag);
        raw.put_u32(self.peer_verification_tag);
        raw.put_u32(local_tag);
        raw.put_u32(self.my_next_tsn);
        // Retain the peer's complete INIT, including extension negotiation.
        raw.extend_from_slice(&init.marshal()?);
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).expect("HMAC accepts 32-byte key");
        mac.update(&raw);
        raw.extend_from_slice(&mac.finalize().into_bytes());
        let cookie = ParamStateCookie {
            cookie: raw.freeze(),
        };
        let mut ack = ChunkInit {
            is_ack: true,
            initiate_tag: local_tag,
            initial_tsn: self.my_next_tsn,
            advertised_receiver_window_credit: self.max_receive_buffer_size,
            num_inbound_streams: self
                .my_max_num_inbound_streams
                .min(init.num_outbound_streams),
            num_outbound_streams: self
                .my_max_num_outbound_streams
                .min(init.num_inbound_streams),
            params: vec![Box::new(cookie)],
        };
        ack.set_supported_extensions();
        let mut response = self.create_packet(vec![Box::new(ack)]);
        response.common_header.verification_tag = init.initiate_tag;
        // Deliberately no TCB mutation until a valid COOKIE-ECHO arrives.
        Ok(vec![response])
    }

    pub(super) fn handle_restart_cookie(
        &mut self,
        packet: &Packet,
        echo: &ChunkCookieEcho,
        now: Instant,
    ) -> Result<Vec<Packet>> {
        if !matches!(
            self.state,
            AssociationState::Established
                | AssociationState::ShutdownPending
                | AssociationState::ShutdownSent
                | AssociationState::ShutdownReceived
                | AssociationState::ShutdownAckSent
        ) || echo.cookie.len() < COOKIE_HEADER + 20 + MAC_LENGTH
        {
            return Ok(vec![]);
        }
        let (Some(key), Some(epoch)) = (self.restart_cookie_key, self.restart_cookie_epoch) else {
            return Ok(vec![]);
        };
        let (raw, signature) = echo.cookie.split_at(echo.cookie.len() - MAC_LENGTH);
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).expect("HMAC accepts 32-byte key");
        mac.update(raw);
        if mac.verify_slice(signature).is_err() {
            return Ok(vec![]);
        }
        let u32_at = |offset| u32::from_be_bytes(raw[offset..offset + 4].try_into().unwrap());
        let issued = u64::from_be_bytes(raw[..8].try_into().unwrap());
        let (local_tie, peer_tie, local_tag, local_tsn) =
            (u32_at(8), u32_at(12), u32_at(16), u32_at(20));
        let Ok(init) = ChunkInit::unmarshal(&Bytes::copy_from_slice(&raw[COOKIE_HEADER..])) else {
            return Ok(vec![]);
        };
        if init.is_ack
            || packet.common_header.verification_tag != local_tag
            || packet.common_header.destination_port != self.source_port
            || packet.common_header.source_port != self.destination_port
            || local_tie != self.my_verification_tag
            || peer_tie != self.peer_verification_tag
            || local_tag == self.my_verification_tag
            || init.initiate_tag == self.peer_verification_tag
        {
            return Ok(vec![]);
        }
        let age = now
            .checked_duration_since(epoch)
            .and_then(|elapsed| elapsed.checked_sub(Duration::from_nanos(issued)));
        if age.is_none_or(|age| age > COOKIE_LIFETIME) {
            let stale = age
                .unwrap_or_default()
                .saturating_sub(COOKIE_LIFETIME)
                .as_micros()
                .min(u32::MAX as u128) as u32;
            let mut response = self.create_packet(vec![Box::new(ChunkError {
                error_causes: vec![ErrorCause {
                    code: STALE_COOKIE_ERROR,
                    raw: Bytes::copy_from_slice(&stale.to_be_bytes()),
                }],
            })]);
            response.common_header.verification_tag = init.initiate_tag;
            return Ok(vec![response]);
        }
        if self.state == AssociationState::ShutdownAckSent {
            self.retransmit_shutdown_ack();
            let mut response = self.create_packet(vec![Box::new(ChunkError {
                error_causes: vec![ErrorCause {
                    code: COOKIE_RECEIVED_WHILE_SHUTTING_DOWN,
                    raw: Bytes::new(),
                }],
            })]);
            response.common_header.verification_tag = init.initiate_tag;
            return Ok(vec![response]);
        }
        self.restart_protocol_state(local_tag, local_tsn, &init, echo.cookie.clone());
        Ok(vec![self.create_packet(vec![Box::new(ChunkCookieAck {})])])
    }

    fn restart_protocol_state(
        &mut self,
        local_tag: u32,
        local_tsn: u32,
        init: &ChunkInit,
        cookie: Bytes,
    ) {
        for timer in Timer::VALUES {
            self.timers.stop(timer);
        }
        self.restart_generation = self.restart_generation.wrapping_add(1);
        // Old per-stream application events refer to discarded protocol buffers.
        self.events
            .retain(|event| matches!(event, Event::DataChannelStatsFinal(_)));
        let mut closing = vec![];
        for (&id, stream) in &mut self.streams {
            if stream.state != RecvSendState::ReadWritable {
                closing.push(id);
                continue;
            }
            let was_high = stream.buffered_amount > stream.buffered_amount_low;
            stream.reassembly_queue = ReassemblyQueue::new(id, self.max_receive_message_size);
            stream.sequence_number = 0;
            stream.first_message_id = 0;
            stream.buffered_amount = 0;
            if was_high {
                self.events
                    .push_back(Event::Stream(StreamEvent::BufferedAmountLow { id }));
            }
            self.events
                .push_back(Event::Stream(StreamEvent::Writable { id }));
        }
        for id in closing {
            if let Some(stream) = self.streams.remove(&id) {
                self.events
                    .push_back(Event::DataChannelStatsFinal(stream.stats));
                self.events
                    .push_back(Event::Stream(StreamEvent::Finished { id }));
                self.events
                    .push_back(Event::Stream(StreamEvent::ResetComplete { id }));
            }
        }
        self.payload_queue = PayloadQueue::default();
        self.outbound_queue = OutboundQueue::default();
        self.inflight_queue_length = 0;
        self.control_queue.clear();
        self.stream_queue.retain(|id| self.streams.contains_key(id));
        self.reconfigs.clear();
        self.reconfig_reset_streams.clear();
        self.active_reconfig = None;
        self.pending_reset_streams.clear();
        self.reconfig_requests.clear();
        self.deferred_reset_data.clear();
        self.max_completed_reconfig_rsn = None;
        self.pending_reset_completions = PendingResetCompletions::default();
        self.failed_reset_streams.clear();
        self.retiring_streams.clear();
        self.ready_stream_finishes = PendingResetCompletions::default();
        self.delivered_stream_finishes = PendingResetCompletions::default();
        self.deferred_forward_tsns.clear();
        self.my_verification_tag = local_tag;
        self.peer_verification_tag = init.initiate_tag;
        self.my_next_tsn = local_tsn;
        self.my_next_rsn = local_tsn;
        self.min_tsn2measure_rtt = local_tsn;
        self.cumulative_tsn_ack_point = local_tsn.wrapping_sub(1);
        self.advanced_peer_tsn_ack_point = local_tsn.wrapping_sub(1);
        self.my_max_num_inbound_streams = self
            .my_max_num_inbound_streams
            .min(init.num_outbound_streams);
        self.my_max_num_outbound_streams = self
            .my_max_num_outbound_streams
            .min(init.num_inbound_streams);
        self.my_cookie = Some(ParamStateCookie { cookie });
        self.will_send_forward_tsn = false;
        self.will_retransmit_fast = false;
        self.will_retransmit_reconfig = false;
        self.will_send_shutdown = false;
        self.will_send_shutdown_ack = false;
        self.will_send_shutdown_complete = false;
        self.source_shutdown_complete_pending = false;
        self.source_writer_pending.store(false, Ordering::Relaxed);
        self.ordinary_write_pending = false;
        self.source_pr_abandoned.clear();
        self.stored_init = None;
        self.stored_cookie_echo = None;
        self.ack_state = AckState::Idle;
        self.delayed_ack_triggered = false;
        self.immediate_ack_triggered = false;
        self.use_forward_tsn = false;
        self.apply_remote_init_params(
            init.initial_tsn,
            init.advertised_receiver_window_credit,
            &init.params,
            "restart",
        );
        self.rto_mgr.reset();
        self.cwnd = (4 * self.mtu)
            .min((2 * self.mtu).max(4380))
            .max(self.min_cwnd);
        self.ssthresh = self.rwnd;
        self.partial_bytes_acked = 0;
        self.fast_recover_exit_point = 0;
        self.in_fast_recovery = false;
        self.source_rack_ack = SourceRackAck::with_min_tsn(local_tsn);
        self.source_tlr = SourceTlr::default();
        self.source_burst = None;
        self.set_state(AssociationState::Established);
        self.handshake_completed = true;
        self.endpoint_events
            .push_back(EndpointEventInner::Restarted {
                local_tag,
                peer_tag: init.initiate_tag,
            });
    }
}
