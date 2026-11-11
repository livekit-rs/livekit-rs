//! Cooperative owners of real data writes. No async task owns an Rtc; while a
//! source-blocking stream waits, the room continues authenticating packets and
//! driving ICE/DTLS/SCTP, API calls, shutdown and channel closure.
use crate::engine::MediaEngine;
use rtc_core::ids::{ParticipantSid, TransportId};
use rtc_core::media_api::{DataKind, DataWrite, MediaEvent};
use std::collections::VecDeque;
use std::time::Instant;
use str0m::channel::ChannelId;

pub(crate) struct Batch {
    completion: Option<Completion>,
    /// Forwarded packets keep the publisher's per-sender FIFO.
    source: Option<ParticipantSid>,
    waiting: VecDeque<DataWrite>,
    active: Vec<Write>,
    replay: bool,
    needs_admission: bool,
    workers: usize,
}
struct Write {
    id: u64,
    value: DataWrite,
    target: Option<(TransportId, ChannelId)>,
    begun: bool,
    replay: bool,
    required_transport: Option<TransportId>,
    admission: AdmissionState,
}
#[derive(Clone, Copy)]
enum AdmissionState {
    Request,
    Waiting(u64),
    Accepted,
}
enum Completion {
    Api(u64),
    Replay(ParticipantSid, TransportId),
}
impl Batch {
    fn new(completion: Option<Completion>, writes: Vec<DataWrite>) -> Self {
        let workers = if writes.len() < 4 {
            1
        } else {
            super::data_fanout_cpu::count().min(writes.len())
        };
        let replay = matches!(&completion, Some(Completion::Replay(..)));
        let needs_admission = matches!(&completion, Some(Completion::Api(..)));
        Self {
            completion,
            source: None,
            waiting: writes.into(),
            active: Vec::new(),
            workers,
            replay,
            needs_admission,
        }
    }
}
impl MediaEngine {
    pub(crate) fn enqueue_data_fanout(&mut self, completion: Option<u64>, writes: Vec<DataWrite>) {
        self.data_fanout
            .push(Batch::new(completion.map(Completion::Api), writes));
    }
    pub(crate) fn enqueue_reliable_replay(
        &mut self,
        participant: ParticipantSid,
        transport: TransportId,
        writes: Vec<DataWrite>,
    ) {
        self.data_replays.insert((participant.clone(), transport));
        // Source holds one participant joining lock across the entire FIFO.
        let mut batch = Batch::new(Some(Completion::Replay(participant, transport)), writes);
        batch.workers = 1;
        self.data_fanout.push(batch);
    }
    pub(crate) fn data_write_deadline(&self) -> Option<Instant> {
        self.transports
            .values()
            .flat_map(|t| t.ordinary_data_writers.values())
            .filter_map(|w| w.wake_deadline(self.data_clock))
            .min()
    }
    /// true only when some real write/outcome/completion progressed; a blocked
    /// writer is woken by native input/output or its exact50ms deadline.
    pub(crate) fn progress_data_fanout(&mut self, now: Instant) -> bool {
        let batches = std::mem::take(&mut self.data_fanout);
        let mut progress = false;
        // A source whose earlier forwarded packet is still in flight blocks its
        // later packets entirely (upstream reads/broadcasts one at a time).
        let mut busy_sources: Vec<ParticipantSid> = Vec::new();
        for mut batch in batches {
            if batch
                .source
                .as_ref()
                .is_some_and(|s| busy_sources.contains(s))
            {
                self.data_fanout.push(batch);
                continue;
            }
            loop {
                let mut completed = false;
                let active = std::mem::take(&mut batch.active);
                for mut write in active {
                    if self.poll_data_write(&mut write, now) {
                        completed = true;
                        progress = true;
                    } else {
                        batch.active.push(write);
                    }
                }
                while batch.active.len() < batch.workers {
                    let Some(value) = batch.waiting.pop_front() else {
                        break;
                    };
                    let id = self.next_data_write;
                    self.next_data_write =
                        id.checked_add(1).expect("data write identity exhausted");
                    let mut write = Write {
                        id,
                        value,
                        target: None,
                        begun: false,
                        replay: batch.replay,
                        required_transport: match &batch.completion {
                            Some(Completion::Replay(_, tid)) => Some(*tid),
                            _ => None,
                        },
                        admission: if batch.needs_admission {
                            AdmissionState::Request
                        } else {
                            AdmissionState::Accepted
                        },
                    };
                    if self.poll_data_write(&mut write, now) {
                        completed = true;
                        progress = true;
                    } else {
                        batch.active.push(write);
                    }
                }
                if batch.waiting.is_empty() && batch.active.is_empty() {
                    match batch.completion {
                        Some(Completion::Api(id)) => {
                            self.events.push(MediaEvent::DataFanoutCompleted { id })
                        }
                        Some(Completion::Replay(participant, transport)) => {
                            self.data_replays.remove(&(participant.clone(), transport));
                            self.events.push(MediaEvent::JoiningReliableReplayed {
                                participant,
                                transport,
                            });
                        }
                        None => {}
                    }
                    progress = true;
                    break;
                }
                // Source workers take another recipient only after their own
                // previous SendDataMessage outcome, not before a blocked write.
                if !completed {
                    if let Some(source) = &batch.source {
                        busy_sources.push(source.clone());
                    }
                    self.data_fanout.push(batch);
                    break;
                }
            }
        }
        progress
    }
    fn poll_data_write(&mut self, write: &mut Write, now: Instant) -> bool {
        // participant.go4213–4238: the joining lock is taken once when
        // SendDataMessage is entered. A write that already began (and may own
        // the raw stream lock) is never re-blocked by a later replay.
        if !write.begun
            && !write.replay
            && write.value.kind == DataKind::Reliable
            && self
                .data_replays
                .iter()
                .any(|(sid, _)| sid == &write.value.to)
        {
            return false;
        }
        match write.admission {
            AdmissionState::Request => {
                let request = self.next_data_admission;
                self.next_data_admission = request
                    .checked_add(1)
                    .expect("data admission identity exhausted");
                self.events.push(MediaEvent::DataWriteAdmission {
                    request,
                    to: write.value.to.clone(),
                    kind: write.value.kind,
                    payload: write.value.payload.clone(),
                    expected_transport: write.value.expected_transport,
                });
                write.admission = AdmissionState::Waiting(request);
                return false;
            }
            AdmissionState::Waiting(request) => match self.data_admissions.remove(&request) {
                None => return false,
                Some(false) => return true,
                Some(true) => write.admission = AdmissionState::Accepted,
            },
            AdmissionState::Accepted => {}
        }
        if write.target.is_none() {
            let Some(tid) = self.participants.get(&write.value.to).and_then(|list| {
                list.iter()
                    .copied()
                    .find(|tid| self.transports.get(tid).is_some_and(|t| t.spec.primary))
            }) else {
                return true;
            };
            if write
                .required_transport
                .or(write.value.expected_transport)
                .is_some_and(|required| required != tid)
            {
                return true;
            }
            let Some(t) = self.transports.get(&tid) else {
                return true;
            };
            // Source always uses the captured participant's primary PC. Do not
            // silently choose a secondary transport when the primary is absent.
            if !t.usable() {
                return true;
            }
            let Some((cid, _)) = t.channel_for(write.value.kind) else {
                if t.has_closed_data_writer(write.value.kind) {
                    self.events.push(MediaEvent::DataSendError {
                        participant: write.value.to.clone(),
                        transport: tid,
                    });
                }
                return true;
            };
            write.target = Some((tid, cid));
        }
        let (tid, cid) = write.target.expect("resolved captured writer");
        let Some(t) = self.transports.get_mut(&tid) else {
            return true;
        };
        if t.spec.participant != write.value.to || !t.usable() {
            if let Some(policy) = t.ordinary_data_writers.get_mut(&cid) {
                policy.release_raw(write.id);
            }
            return true;
        }
        // A labeled channel whose native stream is not yet established is a
        // nonfatal drop (pre-existing behavior pinned by the terminal-slot
        // regression); only a real closed-stream write reports DataSendError.
        let Some(mut ch) = t.rtc.channel(cid) else {
            if let Some(policy) = t.ordinary_data_writers.get_mut(&cid) {
                policy.release_raw(write.id);
            }
            return true;
        };
        let Some(policy) = t.ordinary_data_writers.get_mut(&cid) else {
            return true;
        };
        if !write.begun {
            if let Err(reason) = policy.begin(ch.buffered_amount()) {
                tracing::trace!(?reason, "ordinary data rejected by source writer policy");
                return true;
            }
            write.begun = true;
            policy.set_deadline(now);
        }
        // Original EnableDataChannelBlockWrite is enabled iff slowThreshold>0
        // for the association, including lossy writers. It waits for PREVIOUS
        // pending DATA, not ACK of this new message or total buffered bytes.
        let blocking = self.cfg.rtc.data_channel_slow_threshold > 0;
        if blocking {
            let Some(epoch) = policy.lock_raw(write.id) else {
                return false;
            };
            let same_generation = ch
                .sctp_association_generation()
                .is_some_and(|generation| policy.retain_generation(generation));
            if !same_generation {
                policy.release_raw(write.id);
                // Source ErrAssociationRestarted is a terminal raw-write error,
                // not a timeout and not an instruction to enqueue the old data
                // again. A later independent call may use the open channel.
                tracing::trace!("ordinary blocked write interrupted by SCTP association restart");
                return true;
            }
            if ch.has_pending_sctp_data() {
                if !policy.expired(epoch, now) {
                    return false;
                }
                policy.release_raw(write.id);
                match policy.result(0, ch.buffered_amount(), true, now) {
                    Ok(true) => {
                        policy.set_deadline(now);
                        return false;
                    }
                    Ok(false) => unreachable!("timeout has terminal reason or real retry"),
                    Err(reason) => {
                        tracing::trace!(?reason, "ordinary data blocked write completed");
                        return true;
                    }
                }
            }
        }
        // Pion nonblocking mode has no aggregate str0m one-megabyte queue cap.
        // Slow-threshold blocking is supplied above, not approximated by that cap.
        let result = ch.write_with_buffer_budget(true, &write.value.payload, usize::MAX);
        let buffered = ch.buffered_amount();
        let written = usize::from(matches!(result, Ok(true))) * write.value.payload.len();
        policy.release_raw(write.id);
        let _ = policy.result(written, buffered, false, now);
        match result {
            Ok(true) => {
                // Fixed transportmanager counts the application message once
                // after the real SCTP write succeeds. Prior blocked attempts,
                // generation retirement and timeout retries never reach here.
                self.accepted_data_out.0 = self
                    .accepted_data_out
                    .0
                    .saturating_add(write.value.payload.len() as u64);
                self.accepted_data_out.1 = self.accepted_data_out.1.saturating_add(1);
            }
            Ok(false) => unreachable!("unbounded aggregate admission cannot return false"),
            Err(ref error) => {
                if matches!(
                    error,
                    str0m::RtcError::Sctp(str0m::error::SctpError::Proto(
                        str0m::error::ProtoError::ErrStreamClosed
                    ))
                ) {
                    self.events.push(MediaEvent::DataSendError {
                        participant: write.value.to.clone(),
                        transport: tid,
                    });
                }
                tracing::trace!(error=%error,"ordinary channel actual write completed with error");
            }
        }
        self.mark_dirty(tid);
        true
    }
    /// One forwarded packet: serial below four recipients, else min(NumCPU, n)
    /// workers (protocol utils/parallel.go), FIFO per source.
    pub(crate) fn forward_data(&mut self, source: ParticipantSid, writes: Vec<DataWrite>) {
        let mut batch = Batch::new(None, writes);
        batch.source = Some(source);
        self.data_fanout.push(batch);
        self.progress_data_fanout(self.data_clock);
    }
    /// Existing untracked upstream-data/PerformRpc paths share the same actual
    /// ordinary writer policy. Their caller has no synchronous reply token.
    pub(crate) fn send_data(&mut self, to: &ParticipantSid, kind: DataKind, payload: &[u8]) {
        self.enqueue_data_fanout(
            None,
            vec![DataWrite {
                to: to.clone(),
                kind,
                payload: payload.to_vec(),
                expected_transport: None,
            }],
        );
        self.progress_data_fanout(self.data_clock);
    }
}
