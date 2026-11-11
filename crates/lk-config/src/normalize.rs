//! Official tree (post-processed) → `rtc_core::config::ServerConfig`.
//!
//! Values that Go keeps as wide integers are range-checked here; where upstream
//! would only fail later (at listen time) we fail at load with
//! `invalid configuration "<path>": <why>`.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;

use rtc_core::config::{
    AudioConfig, CongestionControl, ExtensionConfig, ExternalTurnServer, Filter, HttpConfig,
    IceServerProtocol, Limits, LoggingConfig, NodeIp, PliThrottle, PrometheusConfig, RoomDefaults,
    RtcConfig, SctpConfig, ServerConfig, TurnConfig, UdpMode, VideoConfig, WebhookConfig,
};
use rtc_core::model::CodecSpec;
use rtc_core::permission::RoomConfig as RoomPreset;

use crate::error::ConfigError;
use crate::official::{Config, RoomConfiguration};

use rtc_core::config::{PolicyDuration, SendSidePacer};

fn project_stream_allocator_config(
    c: &crate::official::StreamAllocatorConfig,
) -> rtc_core::config::StreamAllocatorConfig {
    rtc_core::config::StreamAllocatorConfig {
        min_channel_capacity: c.min_channel_capacity,
        disable_estimation_unmanaged_tracks: c.disable_etimation_unmanaged_tracks,
        probe_mode: c.probe_mode.clone(),
        probe_overage_pct: c.probe_overage_pct,
        probe_min_bps: c.probe_min_bps,
        paused_min_wait: PolicyDuration::from_nanos(c.paused_min_wait.nanos()),
    }
}

fn project_remote_bwe_config(
    c: &crate::official::RemoteBweConfig,
) -> rtc_core::config::RemoteBweConfig {
    rtc_core::config::RemoteBweConfig {
        nack_ratio_attenuator: c.nack_ratio_attenuator,
        expected_usage_threshold: c.expected_usage_threshold,
        channel_observer_probe: project_channel_observer_config(&c.channel_observer_probe),
        channel_observer_non_probe: project_channel_observer_config(&c.channel_observer_non_probe),
        probe_controller: project_probe_controller_config(&c.probe_controller),
    }
}

fn project_channel_observer_config(
    c: &crate::official::ChannelObserverConfig,
) -> rtc_core::config::ChannelObserverConfig {
    rtc_core::config::ChannelObserverConfig {
        estimate: project_trend_detector_config(&c.estimate),
        nack: project_nack_tracker_config(&c.nack),
    }
}

fn project_trend_detector_config(
    c: &crate::official::TrendDetectorConfig,
) -> rtc_core::config::TrendDetectorConfig {
    rtc_core::config::TrendDetectorConfig {
        required_samples: c.required_samples,
        required_samples_min: c.required_samples_min,
        downward_trend_threshold: c.downward_trend_threshold,
        downward_trend_max_wait: PolicyDuration::from_nanos(c.downward_trend_max_wait.nanos()),
        collapse_threshold: PolicyDuration::from_nanos(c.collapse_threshold.nanos()),
        validity_window: PolicyDuration::from_nanos(c.validity_window.nanos()),
    }
}

fn project_nack_tracker_config(
    c: &crate::official::NackTrackerConfig,
) -> rtc_core::config::NackTrackerConfig {
    rtc_core::config::NackTrackerConfig {
        window_min_duration: PolicyDuration::from_nanos(c.window_min_duration.nanos()),
        window_max_duration: PolicyDuration::from_nanos(c.window_max_duration.nanos()),
        ratio_threshold: c.ratio_threshold,
    }
}

fn project_probe_controller_config(
    c: &crate::official::ProbeControllerConfig,
) -> rtc_core::config::RemoteProbeControllerConfig {
    rtc_core::config::RemoteProbeControllerConfig {
        probe_regulator: project_probe_regulator_config(&c.probe_regulator),
        settle_wait_num_rtt: c.settle_wait_num_rtt,
        settle_wait_min: PolicyDuration::from_nanos(c.settle_wait_min.nanos()),
        settle_wait_max: PolicyDuration::from_nanos(c.settle_wait_max.nanos()),
    }
}

fn project_probe_regulator_config(
    c: &crate::official::ProbeRegulatorConfig,
) -> rtc_core::config::ProbeRegulatorConfig {
    rtc_core::config::ProbeRegulatorConfig {
        base_interval: PolicyDuration::from_nanos(c.base_interval.nanos()),
        backoff_factor: c.backoff_factor,
        max_interval: PolicyDuration::from_nanos(c.max_interval.nanos()),
        min_duration: PolicyDuration::from_nanos(c.min_duration.nanos()),
        max_duration: PolicyDuration::from_nanos(c.max_duration.nanos()),
        duration_increase_factor: c.duration_increase_factor,
    }
}

fn project_send_side_bwe_config(
    c: &crate::official::SendSideBweConfig,
) -> rtc_core::config::SendSideBweConfig {
    rtc_core::config::SendSideBweConfig {
        congestion_detector: project_congestion_detector_config(&c.congestion_detector),
    }
}

fn project_congestion_detector_config(
    c: &crate::official::CongestionDetectorConfig,
) -> rtc_core::config::CongestionDetectorConfig {
    rtc_core::config::CongestionDetectorConfig {
        packet_group: project_packet_group_config(&c.packet_group),
        packet_group_max_age: PolicyDuration::from_nanos(c.packet_group_max_age.nanos()),
        probe_packet_group: project_probe_packet_group_config(&c.probe_packet_group),
        probe_regulator: project_probe_regulator_config(&c.probe_regulator),
        probe_signal: project_probe_signal_config(&c.probe_signal),
        jqr_min_delay: PolicyDuration::from_nanos(c.jqr_min_delay.nanos()),
        jqr_min_trend_coefficient: c.jqr_min_trend_coefficient,
        dqr_max_delay: PolicyDuration::from_nanos(c.dqr_max_delay.nanos()),
        weighted_loss: project_weighted_loss_config(&c.weighted_loss),
        jqr_min_weighted_loss: c.jqr_min_weighted_loss,
        dqr_max_weighted_loss: c.dqr_max_weighted_loss,
        queuing_delay_early_warning_jqr: project_congestion_signal_config(
            &c.queuing_delay_early_warning_jqr,
        ),
        queuing_delay_early_warning_dqr: project_congestion_signal_config(
            &c.queuing_delay_early_warning_dqr,
        ),
        loss_early_warning_jqr: project_congestion_signal_config(&c.loss_early_warning_jqr),
        loss_early_warning_dqr: project_congestion_signal_config(&c.loss_early_warning_dqr),
        queuing_delay_congested_jqr: project_congestion_signal_config(
            &c.queuing_delay_congested_jqr,
        ),
        queuing_delay_congested_dqr: project_congestion_signal_config(
            &c.queuing_delay_congested_dqr,
        ),
        loss_congested_jqr: project_congestion_signal_config(&c.loss_congested_jqr),
        loss_congested_dqr: project_congestion_signal_config(&c.loss_congested_dqr),
        congested_ctr_trend: project_trend_detector_config(&c.congested_ctr_trend),
        congested_ctr_epsilon: c.congested_ctr_epsilon,
        congested_packet_group: project_packet_group_config(&c.congested_packet_group),
        estimation_window_duration: PolicyDuration::from_nanos(c.estimaton_window_duration.nanos()),
    }
}

fn project_packet_group_config(
    c: &crate::official::PacketGroupConfig,
) -> rtc_core::config::PacketGroupConfig {
    rtc_core::config::PacketGroupConfig {
        min_packets: c.min_packets,
        max_window_duration: PolicyDuration::from_nanos(c.max_window_duration.nanos()),
    }
}

fn project_probe_packet_group_config(
    c: &crate::official::ProbePacketGroupConfig,
) -> rtc_core::config::ProbePacketGroupConfig {
    rtc_core::config::ProbePacketGroupConfig {
        packet_group: project_packet_group_config(&c.packet_group),
        settle_wait_num_rtt: c.settle_wait_num_rtt,
        settle_wait_min: PolicyDuration::from_nanos(c.settle_wait_min.nanos()),
        settle_wait_max: PolicyDuration::from_nanos(c.settle_wait_max.nanos()),
    }
}

fn project_probe_signal_config(
    c: &crate::official::ProbeSignalConfig,
) -> rtc_core::config::ProbeSignalConfig {
    rtc_core::config::ProbeSignalConfig {
        min_bytes_ratio: c.min_bytes_ratio,
        min_duration_ratio: c.min_duration_ratio,
        jqr_min_delay: PolicyDuration::from_nanos(c.jqr_min_delay.nanos()),
        dqr_max_delay: PolicyDuration::from_nanos(c.dqr_max_delay.nanos()),
        weighted_loss: project_weighted_loss_config(&c.weighted_loss),
        jqr_min_weighted_loss: c.jqr_min_weighted_loss,
        dqr_max_weighted_loss: c.dqr_max_weighted_loss,
    }
}

fn project_weighted_loss_config(
    c: &crate::official::WeightedLossConfig,
) -> rtc_core::config::WeightedLossConfig {
    rtc_core::config::WeightedLossConfig {
        min_duration_for_loss_validity: PolicyDuration::from_nanos(
            c.min_duration_for_loss_validity.nanos(),
        ),
        base_duration: PolicyDuration::from_nanos(c.base_duration.nanos()),
        base_pps: c.base_pps,
        loss_penalty_factor: c.loss_penalty_factor,
    }
}

fn project_congestion_signal_config(
    c: &crate::official::CongestionSignalConfig,
) -> rtc_core::config::CongestionSignalConfig {
    rtc_core::config::CongestionSignalConfig {
        min_number_of_groups: c.min_number_of_groups,
        min_duration: PolicyDuration::from_nanos(c.min_duration.nanos()),
    }
}

fn project_congestion_control(
    c: &crate::official::CongestionControlConfig,
) -> Result<CongestionControl, ConfigError> {
    // Normally already checked by load; direct normalization still fails closed.
    let send_side_bwe_pacer = match c.send_side_bwe_pacer.as_str() {
        "" => SendSidePacer::Unspecified,
        "pass-through" => SendSidePacer::PassThrough,
        "no-queue" => SendSidePacer::NoQueue,
        "leaky-bucket" => SendSidePacer::LeakyBucket,
        other => {
            return Err(invalid(
                "rtc.congestion_control.send_side_bwe_pacer",
                format!("unknown pacer {other:?}"),
            ));
        }
    };
    Ok(CongestionControl {
        enabled: c.enabled,
        allow_pause: c.allow_pause,
        use_send_side_bwe: c.use_send_side_bwe,
        use_send_side_bwe_interceptor: c.use_send_side_bwe_interceptor,
        send_side_bwe_pacer,
        stream_allocator: project_stream_allocator_config(&c.stream_allocator),
        remote_bwe: project_remote_bwe_config(&c.remote_bwe),
        send_side_bwe: project_send_side_bwe_config(&c.send_side_bwe),
    })
}

/// TTL for external TURN credentials when `ttl` is 0 (`DefaultExternalTURNTTLSeconds`).
const DEFAULT_EXTERNAL_TURN_TTL: u64 = 14_400;
/// Fallback for negative external TURN TTLs (`DefaultTURNTTLSeconds`).
const DEFAULT_TURN_TTL: u64 = 300;

/// zap level names (`PROTO/logger/logger.go:80-86`).
const LOG_LEVELS: &[&str] = &[
    "debug", "info", "warn", "warning", "error", "dpanic", "panic", "fatal",
];

pub(crate) fn invalid(path: &str, why: impl std::fmt::Display) -> ConfigError {
    ConfigError::msg(format!("invalid configuration \"{path}\": {why}"))
}

fn port_u16(path: &str, v: i64) -> Result<u16, ConfigError> {
    u16::try_from(v).map_err(|_| invalid(path, format!("{v} is not a valid port")))
}

fn opt_port(path: &str, v: i64) -> Result<Option<u16>, ConfigError> {
    if v <= 0 {
        Ok(None)
    } else {
        port_u16(path, v).map(Some)
    }
}

fn parse_ip(path: &str, s: &str) -> Result<IpAddr, ConfigError> {
    s.trim().parse::<IpAddr>().map_err(|_| {
        invalid(
            path,
            format!("invalid IP address {s:?} (host names are not supported)"),
        )
    })
}

/// Go `net.ParseCIDR`, returning the masked network and prefix length.
pub(crate) fn parse_cidr(s: &str) -> Result<(IpAddr, u8), String> {
    let err = || format!("invalid CIDR address: {s}");
    let (ip, bits) = s.split_once('/').ok_or_else(err)?;
    let ip: IpAddr = ip.parse().map_err(|_| err())?;
    if bits.is_empty() || !bits.bytes().all(|b| b.is_ascii_digit()) || bits.len() > 3 {
        return Err(err());
    }
    let bits: u8 = bits.parse().map_err(|_| err())?;
    Ok(match ip {
        IpAddr::V4(v4) => {
            if bits > 32 {
                return Err(err());
            }
            let mask = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - bits)
            };
            (IpAddr::V4(Ipv4Addr::from(u32::from(v4) & mask)), bits)
        }
        IpAddr::V6(v6) => {
            if bits > 128 {
                return Err(err());
            }
            let mask = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - bits)
            };
            (IpAddr::V6(Ipv6Addr::from(u128::from(v6) & mask)), bits)
        }
    })
}

/// `rtc.ips.{includes,excludes}` are parsed by `IPFilterFromConf`
/// (webrtc_config.go:441-460), reached from `NewWebRTCConfig` inside
/// `NewLocalRoomManager` (service/roommanager.go:126, wire_gen.go:148) — after
/// `getConfig`/`ValidateKeys`/`LoadTURNSecrets`. `startServer` propagates the
/// bare `net.ParseCIDR` error and `main.go:191` prints `err.Error()` verbatim to
/// stdout (SPEC-0074 C30), so the path is not part of the message.
///
/// Normalization therefore skips malformed entries instead of failing config
/// load; `check_ip_filters` re-parses the raw list at the wiring boundary and
/// refuses startup, so a bad CIDR is never accepted and ignored.
fn cidrs(list: &[String]) -> Vec<(IpAddr, u8)> {
    list.iter().filter_map(|s| parse_cidr(s).ok()).collect()
}

/// Authoritative `IPFilterFromConf` parse, run at the wiring boundary.
pub(crate) fn check_ip_filters(cfg: &Config) -> Result<(), ConfigError> {
    for s in cfg.rtc.ips.includes.iter().chain(&cfg.rtc.ips.excludes) {
        parse_cidr(s).map_err(ConfigError::msg)?;
    }
    Ok(())
}

/// TURN peer CIDR lists use the upstream error text (`service/turn.go`).
fn peer_cidrs(field: &str, list: &[String]) -> Result<Vec<(IpAddr, u8)>, ConfigError> {
    list.iter()
        .map(|s| {
            parse_cidr(s)
                .map_err(|e| ConfigError::msg(format!("invalid CIDR {s:?} in {field}: {e}")))
        })
        .collect()
}

fn nonzero_usize(v: i64) -> Option<usize> {
    if v <= 0 { None } else { Some(v as usize) }
}

fn secs(v: u32) -> Duration {
    Duration::from_secs(v as u64)
}

fn preset(p: &RoomConfiguration) -> RoomPreset {
    let nz = |v: u32| if v == 0 { None } else { Some(v) };
    RoomPreset {
        tags: p.tags.clone(),
        automatic_recording: p.egress.as_ref().map(recording_preset),
        empty_timeout_s: nz(p.empty_timeout),
        departure_timeout_s: nz(p.departure_timeout),
        max_participants: nz(p.max_participants),
        metadata: if p.metadata.is_empty() {
            None
        } else {
            Some(p.metadata.clone())
        },
        min_playout_delay: nz(p.min_playout_delay),
        max_playout_delay: nz(p.max_playout_delay),
        sync_streams: if p.sync_streams { Some(true) } else { None },
        agent_dispatches: p.agents.0.as_ref().map(|agents| {
            agents
                .iter()
                .map(|agent| {
                    let agent = agent.clone().unwrap_or_default();
                    rtc_core::permission::RoomAgentDispatchOptions {
                        agent_name: agent.agent_name,
                        metadata: agent.metadata,
                        restart_policy: agent.restart_policy,
                        deployment: agent.deployment,
                        attributes: agent.attributes,
                    }
                })
                .collect()
        }),
    }
}

fn bitrate_intervals(config: &crate::official::StreamTrackerConfig) -> [Option<Duration>; 3] {
    std::array::from_fn(|layer| {
        config
            .bitrate_report_interval
            .get(&(layer as i32))
            .filter(|duration| duration.nanos() > 0)
            .map(|duration| duration.to_std())
    })
}

/// Fixed Go AddTracker chooses one tracker type and looks up its layer map.
fn stream_trackers(
    config: &crate::official::StreamTrackerConfig,
) -> [Option<rtc_core::config::StreamTrackerSpec>; 3] {
    use rtc_core::config::{PacketTrackerSpec, StreamTrackerSpec};

    std::array::from_fn(|layer| {
        let layer = layer as i32;
        match config.stream_tracker_type.as_str() {
            "packet" => config.packet_tracker.get(&layer).map(|tracker| {
                StreamTrackerSpec::Packet(PacketTrackerSpec {
                    samples_required: tracker.samples_required,
                    cycles_required: tracker.cycles_required,
                    cycle_duration: tracker.cycle_duration.to_std(),
                })
            }),
            "frame" => config
                .frame_tracker
                .get(&layer)
                .map(|tracker| StreamTrackerSpec::Frame {
                    min_fps: tracker.min_fps,
                }),
            _ => None,
        }
    })
}

/// Inputs that are not part of the official tree.
pub(crate) struct Extras {
    pub node_ip_auto_generated: bool,
    pub node_id: String,
    pub extension: Option<ExtensionConfig>,
    pub cpus: usize,
}

/// Builds the normalized config. Returns warnings alongside.
pub(crate) fn to_server_config(
    cfg: &Config,
    extras: Extras,
) -> Result<(ServerConfig, Vec<String>), ConfigError> {
    let mut warnings = Vec::new();

    // --- http
    let http_port = port_u16("port", cfg.port as i64)?;
    let http = HttpConfig {
        port: http_port,
        bind_addresses: cfg.bind_addresses.clone(),
        max_request_body: cfg.limit.max_api_request_body_size.max(0) as u64,
    };

    // --- rtc
    let r = &cfg.rtc;
    let udp = if r.port_range_start != 0 && r.port_range_end != 0 {
        UdpMode::Range {
            start: port_u16("rtc.port_range_start", r.port_range_start as i64)?,
            end: port_u16("rtc.port_range_end", r.port_range_end as i64)?,
        }
    } else if r.udp_port.valid() {
        // The server binds using the raw configured ports. Keep only the
        // representable prefix here for discovery, without wrapping a port
        // that upstream rejects later at its UDP listener.
        let ports = r
            .udp_port
            .iter()
            .take(extras.cpus.max(1))
            .map_while(|port| u16::try_from(port).ok())
            .collect();
        UdpMode::Mux { ports }
    } else {
        UdpMode::Disabled
    };
    if matches!(udp, UdpMode::Disabled) {
        warnings.push(
            "no ICE/UDP port configured (rtc.port_range_end is 0 and rtc.udp_port is unset); WebRTC over UDP is disabled"
                .to_string(),
        );
    }

    // SPEC-0074 C30: the node IP is not validated at load time upstream. The
    // base `--node-ip` flag and YAML go through `NodeIP.UnmarshalString` (a bad
    // value is a warning and the field stays empty), but a dotted generated
    // flag (`rtc.node_ip.ipv4`) writes the raw text straight into the struct
    // leaf, bypassing that check (`config.go:96-115`). `NewWebRTCConfig` then
    // forwards the string to the ICE address-rewrite rules without parsing
    // (webrtc_config.go:257-286), so an invalid literal simply never matches
    // and the server starts. Mirror that here: parse best-effort and drop a
    // value that is not an IP rather than failing the whole load; a value that
    // parses is applied as a real 1:1 NAT mapping below.
    let node_ip = NodeIp {
        v4: if r.node_ip.v4.is_empty() {
            None
        } else {
            r.node_ip.v4.parse::<IpAddr>().ok().and_then(|ip| match ip {
                IpAddr::V4(v4) => Some(v4),
                IpAddr::V6(v6) => v6.to_ipv4_mapped(),
            })
        },
        v6: if r.node_ip.v6.is_empty() {
            None
        } else {
            r.node_ip
                .v6
                .parse::<Ipv6Addr>()
                .ok()
                .filter(|v6| v6.to_ipv4_mapped().is_none())
        },
    };

    let mut turn_servers = Vec::new();
    for s in &r.turn_servers {
        let protocol = match s.protocol.as_str() {
            "udp" => IceServerProtocol::Udp,
            "tls" => IceServerProtocol::Tls,
            // Go's room manager defaults to TCP for all other spellings.
            _ => IceServerProtocol::Tcp,
        };
        let secret = s.secret.trim();
        let ttl = match s.ttl {
            0 => DEFAULT_EXTERNAL_TURN_TTL,
            t if t < 0 => DEFAULT_TURN_TTL,
            t => t as u64,
        };
        turn_servers.push(ExternalTurnServer {
            host: s.host.clone(),
            port: s.port,
            protocol,
            username: s.username.clone(),
            credential: s.credential.clone(),
            secret: if secret.is_empty() {
                None
            } else {
                Some(secret.as_bytes().to_vec())
            },
            ttl: Duration::from_secs(ttl),
        });
    }

    let packet_buffer_size = if r.packet_buffer_size == 0 {
        500
    } else {
        r.packet_buffer_size
    };
    let pbv = if r.packet_buffer_size_video == 0 {
        packet_buffer_size
    } else {
        r.packet_buffer_size_video
    };
    let pba = if r.packet_buffer_size_audio == 0 {
        packet_buffer_size
    } else {
        r.packet_buffer_size_audio
    };

    let forward_stats = &r.forward_stats;
    let forward_stats = if [
        forward_stats.summary_interval.nanos(),
        forward_stats.report_interval.nanos(),
        forward_stats.report_window.nanos(),
    ]
    .contains(&0)
    {
        None
    } else {
        Some(
            rtc_core::forward_stats::ForwardStats::new(
                forward_stats.summary_interval.nanos(),
                forward_stats.report_interval.nanos(),
                forward_stats.report_window.nanos(),
                std::time::Instant::now(),
            )
            .map_err(|e| invalid("rtc.forward_stats", e))?,
        )
    };
    let rtc = RtcConfig {
        ice_acceptance_min_waits: [
            (r.host_acceptance_min_wait, Duration::ZERO),
            (r.srflx_acceptance_min_wait, Duration::from_millis(500)),
            (r.prflx_acceptance_min_wait, Duration::from_secs(1)),
            (r.relay_acceptance_min_wait, Duration::from_secs(2)),
        ]
        .map(|(configured, inherited)| {
            if configured.0 > 0 {
                Duration::from_nanos(configured.0 as u64)
            } else {
                inherited
            }
        }),
        forward_stats,
        udp,
        // The raw nonzero port is checked by the startup TCP listener.
        tcp_port: u16::try_from(r.tcp_port).ok().filter(|port| *port != 0),
        use_node_ip_mapping: !r.node_ip.is_empty()
            && (r.use_external_ip || !extras.node_ip_auto_generated),
        node_ip,
        advertise_internal_ip: r.advertise_internal_ip,
        stun_servers: r.stun_servers.clone(),
        turn_servers,
        use_ice_lite: r.use_ice_lite,
        force_tcp: r.force_tcp,
        use_stun_port_as_ice: r.use_stun_port_as_ice,
        use_external_ip: r.use_external_ip,
        skip_external_ip_validation: r.skip_external_ip_validation,
        external_ip_only: r.external_ip_only,
        batch_write_size: r.batch_io.batch_size.max(0) as usize,
        batch_write_interval: r.batch_io.max_flush_interval.to_std(),
        interfaces: Filter {
            includes: r.interfaces.includes.clone(),
            excludes: r.interfaces.excludes.clone(),
        },
        ips: Filter {
            includes: cidrs(&r.ips.includes),
            excludes: cidrs(&r.ips.excludes),
        },
        enable_loopback_candidate: r.enable_loopback_candidate,
        use_mdns: r.use_mdns,
        enable_warp: r.enable_warp,
        enable_rtp_stream_restart_detection: r.enable_rtp_stream_restart_detection,
        data_channel_slow_threshold: r.datachannel_slow_threshold,
        data_channel_max_buffered_amount: r.data_channel_max_buffered_amount,
        data_channel_lossy_target_latency: PolicyDuration::from_nanos(
            r.datachannel_lossy_target_latency.nanos(),
        ),
        data_track_target_latency: PolicyDuration::from_nanos(
            r.datachannel_data_track_target_latency.nanos(),
        ),
        sctp: SctpConfig {
            min_cwnd: r.sctp_min_cwnd,
            fast_rtx_wnd: r.sctp_fast_rtx_wnd,
            cwnd_ca_step: r.sctp_cwnd_ca_step,
        },
        packet_buffer_size_video: pbv.max(0) as usize,
        packet_buffer_size_audio: pba.max(0) as usize,
        pli_throttle: PliThrottle {
            low: r.pli_throttle.low_quality.to_std(),
            mid: r.pli_throttle.mid_quality.to_std(),
            high: r.pli_throttle.high_quality.to_std(),
        },
        congestion_control: project_congestion_control(&r.congestion_control)?,
        allow_tcp_fallback: r.allow_tcp_fallback.unwrap_or(true),
        tcp_fallback_rtt_threshold: r.tcp_fallback_rtt_threshold,
        allow_udp_unstable_fallback: r.allow_udp_unstable_fallback,
        reconnect_on_publication_error: r.reconnect_on_publication_error.unwrap_or(false),
        reconnect_on_subscription_error: r.reconnect_on_subscription_error.unwrap_or(false),
        reconnect_on_data_channel_error: r.reconnect_on_data_channel_error.unwrap_or(false),
    };

    // --- room
    let rm = &cfg.room;
    let room = RoomDefaults {
        auto_create: rm.auto_create,
        enabled_codecs: rm
            .enabled_codecs
            .iter()
            .map(|c| CodecSpec {
                mime: c.mime.clone(),
                fmtp_line: c.fmtp_line.clone(),
            })
            .collect(),
        max_participants: rm.max_participants,
        empty_timeout: secs(rm.empty_timeout),
        departure_timeout: secs(rm.departure_timeout),
        enable_remote_unmute: rm.enable_remote_unmute,
        playout_delay: if rm.playout_delay.enabled {
            Some((rm.playout_delay.min as u32, rm.playout_delay.max as u32))
        } else {
            None
        },
        sync_streams: rm.sync_streams,
        enable_full_room_in_webhooks: rm.enable_full_room_in_webhooks,
        create_room_timeout: PolicyDuration::from_nanos(rm.create_room_timeout.nanos()),
        create_room_attempts: rm.create_room_attempts,
        update_batch_target_size: rm.update_batch_target_size,
        presets: rm
            .room_configurations
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().map(preset)))
            .collect::<BTreeMap<_, _>>(),
    };

    // --- audio / video
    let audio = AudioConfig {
        active_level: cfg.audio.active_level,
        min_percentile: cfg.audio.min_percentile,
        update_interval: Duration::from_millis(cfg.audio.update_interval as u64),
        smooth_intervals: cfg.audio.smooth_intervals,
        active_red_encoding: cfg.audio.active_red_encoding,
    };
    let video = VideoConfig {
        codec_regression_threshold: cfg.video.codec_regression_threshold,
        dynacast_pause_delay: cfg.video.dynacast_pause_delay.to_std(),
        bitrate_report: rtc_core::config::VideoBitrateReport {
            video: bitrate_intervals(&cfg.video.stream_tracker_manager.video),
            screenshare: bitrate_intervals(&cfg.video.stream_tracker_manager.screenshare),
        },
        stream_trackers: rtc_core::config::VideoStreamTrackers {
            video: stream_trackers(&cfg.video.stream_tracker_manager.video),
            screenshare: stream_trackers(&cfg.video.stream_tracker_manager.screenshare),
        },
    };

    // --- turn
    let t = &cfg.turn;
    let mut turn_bind = Vec::new();
    // Fixed NewTurnServer returns before these consumers when TURN is disabled.
    // Keep the original official config intact; inactive typed runtime fields
    // have no listener/relay consumer and must not reject the main server.
    if t.enabled {
        for a in &t.bind_addresses {
            turn_bind.push(parse_ip("turn.bind_addresses", a)?);
        }
    }
    let tls_port = if t.enabled {
        opt_port("turn.tls_port", t.tls_port)?
    } else {
        None
    };
    let udp_port = if t.enabled {
        opt_port("turn.udp_port", t.udp_port)?
    } else {
        None
    };
    // Upstream compiles PROXY trusted CIDRs inside newTURNTCPListener, so the
    // list is only parsed and required to be non-empty when a TCP (TLS-port)
    // listener exists and ProxyProtocol is set (turn.go:222-245).
    let mut proxy_trusted_cidrs = Vec::new();
    if t.enabled && t.proxy_protocol && tls_port.is_some() {
        let parsed = peer_cidrs(
            "turn.proxy_protocol_trusted_cidrs",
            &t.proxy_protocol_trusted_cidrs,
        )?;
        if parsed.is_empty() {
            return Err(ConfigError::msg(
                "turn.proxy_protocol requires at least one entry in turn.proxy_protocol_trusted_cidrs",
            ));
        }
        proxy_trusted_cidrs = parsed;
    }
    let turn = TurnConfig {
        enabled: t.enabled,
        domain: t.domain.clone(),
        cert_file: if t.cert_file.is_empty() {
            None
        } else {
            Some(PathBuf::from(&t.cert_file))
        },
        key_file: if t.key_file.is_empty() {
            None
        } else {
            Some(PathBuf::from(&t.key_file))
        },
        tls_port,
        udp_port,
        relay_range: (t.relay_range_start, t.relay_range_end),
        external_tls: t.external_tls,
        bind_addresses: turn_bind,
        per_user_allocation_limit: t.per_user_relay_allocation_limit.clamp(0, u32::MAX as i64)
            as u32,
        ttl: Duration::from_secs(t.ttl_seconds.max(0) as u64),
        allow_restricted_peer_cidrs: if t.enabled {
            peer_cidrs(
                "turn.allow_restricted_peer_cidrs",
                &t.allow_restricted_peer_cidrs,
            )?
        } else {
            Vec::new()
        },
        deny_peer_cidrs: if t.enabled {
            peer_cidrs("turn.deny_peer_cidrs", &t.deny_peer_cidrs)?
        } else {
            Vec::new()
        },
        proxy_protocol: t.proxy_protocol,
        proxy_trusted_cidrs,
    };

    // --- webhook
    let w = &cfg.webhook;
    let webhook = WebhookConfig {
        urls: w.urls.clone(),
        api_key: w.api_key.clone(),
        include_events: w.filter_params.include_events.clone(),
        exclude_events: w.filter_params.exclude_events.clone(),
        max_age: PolicyDuration::from_nanos(w.resource_url_notifier.max_age.nanos()),
        max_depth: w.resource_url_notifier.max_depth,
    };

    // --- limits
    let l = &cfg.limit;
    let limits = Limits {
        subscription_limit_video: if l.subscription_limit_video > 0 {
            Some(l.subscription_limit_video as u32)
        } else {
            None
        },
        subscription_limit_audio: if l.subscription_limit_audio > 0 {
            Some(l.subscription_limit_audio as u32)
        } else {
            None
        },
        max_metadata_size: nonzero_usize(l.max_metadata_size as i64),
        max_attributes_size: nonzero_usize(l.max_attributes_size as i64),
        max_room_name_length: l.max_room_name_length,
        max_participant_identity_length: l.max_participant_identity_length,
        max_participant_name_length: l.max_participant_name_length,
        max_data_track_custom_encoding_length: l.max_data_track_custom_encoding_length,
        max_data_blob_key_length: l.max_data_blob_key_length,
        max_data_blob_size: l.max_data_blobs_size,
        signal_message_size_limit: nonzero_usize(l.signal_message_size_limit),
    };

    // --- logging
    let mut level = cfg.logging.level.to_ascii_lowercase();
    if level.is_empty() {
        level = "info".to_string();
    } else if !LOG_LEVELS.contains(&level.as_str()) {
        // upstream silently falls back to info
        warnings.push(format!(
            "logging.level {:?} is not a valid level, using \"info\"",
            cfg.logging.level
        ));
        level = "info".to_string();
    }
    let logging = LoggingConfig {
        sample: cfg.logging.sample,
        sample_initial: cfg.logging.sample_initial,
        sample_interval: cfg.logging.sample_interval,
        item_sample_seconds: cfg.logging.item_sample_seconds,
        item_sample_initial: cfg.logging.item_sample_initial,
        item_sample_interval: cfg.logging.item_sample_interval,
        level,
        json: cfg.logging.json,
        component_levels: cfg.logging.component_levels.clone(),
    };

    // --- prometheus (`server.go:148-151`)
    let mut prom_port = cfg.prometheus.port;
    if cfg.prometheus_port > 0 {
        warnings.push(
            "prometheus_port is deprecated, please switch to prometheus.port instead".to_string(),
        );
        prom_port = cfg.prometheus_port;
    }
    let prometheus = PrometheusConfig {
        port: opt_port("prometheus.port", prom_port as i64)?,
        username: if cfg.prometheus.username.is_empty() {
            None
        } else {
            Some(cfg.prometheus.username.clone())
        },
        password: if cfg.prometheus.password.is_empty() {
            None
        } else {
            Some(cfg.prometheus.password.clone())
        },
    };

    let server = ServerConfig {
        http,
        rtc,
        room,
        audio,
        video,
        turn,
        webhook,
        limits,
        logging,
        prometheus,
        keys: cfg.keys.0.clone(),
        region: cfg.region.clone(),
        node_id: extras.node_id,
        development: cfg.development,
        enable_data_tracks: cfg.enable_data_tracks,
        enable_participant_data_blob: cfg.enable_participant_data_blob,
        enable_psrpc_for_get_list_participants: cfg.api.enable_psrpc_for_get_list_participants,
        extension: extras.extension,
    };
    Ok((server, warnings))
}

/// Embedded TURN checks done by upstream `NewTurnServer` (`service/turn.go:65-140`).
pub(crate) fn validate_turn(cfg: &Config) -> Result<(), ConfigError> {
    let t = &cfg.turn;
    if !t.enabled {
        return Ok(());
    }
    if t.tls_port <= 0 && t.udp_port <= 0 {
        return Err(ConfigError::msg("invalid TURN ports"));
    }
    if t.tls_port > 0 {
        if t.domain.is_empty() {
            return Err(ConfigError::msg("TURN domain required"));
        }
        if !is_valid_domain(&t.domain) {
            return Err(ConfigError::msg("TURN domain is not correct"));
        }
    }
    for a in &t.bind_addresses {
        let is_v4 = matches!(a.parse::<IpAddr>(), Ok(IpAddr::V4(_)));
        let node_ip = if is_v4 {
            &cfg.rtc.node_ip.v4
        } else {
            &cfg.rtc.node_ip.v6
        };
        if node_ip.is_empty() {
            return Err(ConfigError::msg("no matching node IP for relay"));
        }
    }
    Ok(())
}

/// `^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+\.?$`
pub(crate) fn is_valid_domain(d: &str) -> bool {
    let d = d.strip_suffix('.').unwrap_or(d);
    let labels: Vec<&str> = d.split('.').collect();
    labels.len() >= 2
        && labels
            .iter()
            .all(|l| !l.is_empty() && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_parsing() {
        assert_eq!(
            parse_cidr("10.1.2.3/24").unwrap(),
            ("10.1.2.0".parse().unwrap(), 24)
        );
        assert_eq!(
            parse_cidr("::1/128").unwrap(),
            ("::1".parse().unwrap(), 128)
        );
        assert_eq!(
            parse_cidr("10.0.0.1").unwrap_err(),
            "invalid CIDR address: 10.0.0.1"
        );
        assert!(parse_cidr("10.0.0.0/33").is_err());
    }

    #[test]
    fn domains() {
        assert!(is_valid_domain("turn.example.com"));
        assert!(is_valid_domain("turn.example.com."));
        assert!(!is_valid_domain("localhost"));
        assert!(!is_valid_domain("bad_domain.com"));
    }

    #[test]
    fn defaults_normalize() {
        let mut c = Config::default();
        c.rtc.port_range_start = 50000;
        c.rtc.port_range_end = 60000;
        c.rtc.node_ip.v4 = "10.0.0.1".into();
        c.turn.relay_range_start = 30000;
        c.turn.relay_range_end = 40000;
        let (s, _) = to_server_config(
            &c,
            Extras {
                node_ip_auto_generated: true,
                node_id: "ND_x".into(),
                extension: None,
                cpus: 4,
            },
        )
        .unwrap();
        assert_eq!(s.http.port, 7880);
        assert_eq!(
            s.rtc.udp,
            UdpMode::Range {
                start: 50000,
                end: 60000
            }
        );
        assert_eq!(s.rtc.tcp_port, Some(7881));
        assert!(!s.rtc.use_node_ip_mapping);
        assert!(s.rtc.allow_tcp_fallback);
        assert_eq!(s.logging.level, "info");
        assert_eq!(s.limits.max_metadata_size, Some(524_288));
        assert_eq!(s.room.empty_timeout, Duration::from_secs(300));
        assert_eq!(s.turn.ttl, Duration::from_secs(300));
    }

    #[test]
    fn video_bitrate_report_intervals_preserve_layer_overrides_and_null_map() {
        let mut config = Config::default();
        config.rtc.node_ip.v4 = "10.0.0.1".into();
        crate::official::decode_yaml(
            "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 125ms, 1: 450ms}\n    screenshare:\n      bitrate_report_interval: null\n",
            true,
            &mut config,
        )
        .unwrap();
        let (server, _) = to_server_config(
            &config,
            Extras {
                node_ip_auto_generated: true,
                node_id: "ND_window".into(),
                extension: None,
                cpus: 4,
            },
        )
        .unwrap();
        assert_eq!(
            server.video.bitrate_report.video,
            [
                Some(Duration::from_millis(125)),
                Some(Duration::from_millis(450)),
                Some(Duration::from_secs(1)),
            ]
        );
        assert_eq!(server.video.bitrate_report.screenshare, [None; 3]);
    }

    #[test]
    fn mdns_flag_reaches_the_room_config() {
        const EXTRAS: Extras = Extras {
            node_ip_auto_generated: false,
            node_id: String::new(),
            extension: None,
            cpus: 1,
        };
        let mut c = Config::default();
        c.rtc.node_ip.v4 = "10.0.0.1".into();
        let (off, _) = to_server_config(&c, Extras { ..EXTRAS }).unwrap();
        // Upstream default (mediatransportutil rtcconfig `use_mdns`), which
        // disables pion's mDNS engine.
        assert!(!off.rtc.use_mdns);
        c.rtc.use_mdns = true;
        let (on, _) = to_server_config(&c, Extras { ..EXTRAS }).unwrap();
        assert!(on.rtc.use_mdns);
    }

    #[test]
    fn rtp_restart_flag_reaches_runtime_default_false_and_explicit_true() {
        let mut c = Config::default();
        c.rtc.node_ip.v4 = "10.0.0.1".into();
        let extras = || Extras {
            node_ip_auto_generated: false,
            node_id: String::new(),
            extension: None,
            cpus: 1,
        };
        assert!(
            !to_server_config(&c, extras())
                .unwrap()
                .0
                .rtc
                .enable_rtp_stream_restart_detection
        );
        crate::official::decode_yaml(
            "rtc: {enable_rtp_stream_restart_detection: true}\n",
            true,
            &mut c,
        )
        .unwrap();
        assert!(
            to_server_config(&c, extras())
                .unwrap()
                .0
                .rtc
                .enable_rtp_stream_restart_detection
        );
    }

    #[test]
    fn warp_flag_reaches_the_media_engine_config() {
        const EXTRAS: Extras = Extras {
            node_ip_auto_generated: false,
            node_id: String::new(),
            extension: None,
            cpus: 1,
        };
        let mut c = Config::default();
        c.rtc.node_ip.v4 = "10.0.0.1".into();
        let (off, _) = to_server_config(&c, Extras { ..EXTRAS }).unwrap();
        // Upstream default (`config.go` EnableWarp), experimental and off.
        assert!(!off.rtc.enable_warp);
        c.rtc.enable_warp = true;
        // SPEC-0074 C20: no longer rejected as unsupported; the media engine
        // consumes it for SNAP on every non-one-shot peer connection.
        crate::unsupported::check(&c).unwrap();
        let (on, _) = to_server_config(&c, Extras { ..EXTRAS }).unwrap();
        assert!(on.rtc.enable_warp);
    }

    #[test]
    fn mux_takes_first_cpus_ports() {
        let mut c = Config::default();
        c.rtc.udp_port = crate::ports::PortRange {
            start: 7882,
            end: 7892,
        };
        let (s, _) = to_server_config(
            &c,
            Extras {
                node_ip_auto_generated: false,
                node_id: String::new(),
                extension: None,
                cpus: 2,
            },
        )
        .unwrap();
        assert_eq!(
            s.rtc.udp,
            UdpMode::Mux {
                ports: vec![7882, 7883]
            }
        );
    }

    #[test]
    fn typed_room_agent_preset_projects_to_neutral_options_without_losing_null_entries() {
        let mut config = Config::default();
        crate::official::decode_yaml("room:\n  room_configurations:\n    p:\n      agents:\n        - {agent_name: assistant, metadata: context, restart_policy: 1, deployment: prod, attributes: {k: v}}\n        - null\n", true, &mut config).unwrap();
        let projected = preset(config.room.room_configurations["p"].as_ref().unwrap());
        let agents = projected.agent_dispatches.unwrap();
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].agent_name, "assistant");
        assert_eq!(agents[0].metadata, "context");
        assert_eq!(agents[0].restart_policy, 1);
        assert_eq!(agents[0].deployment, "prod");
        assert_eq!(agents[0].attributes["k"], "v");
        assert_eq!(
            agents[1],
            rtc_core::permission::RoomAgentDispatchOptions::default()
        );
    }
    #[test]
    fn participant_api_view_default_and_explicit_live_flag_project_without_inversion() {
        for enabled in [false, true] {
            let mut c = Config::default();
            c.rtc.node_ip.v4 = "127.0.0.1".into();
            c.api.enable_psrpc_for_get_list_participants = enabled;
            let (normalized, _) = to_server_config(
                &c,
                Extras {
                    node_ip_auto_generated: false,
                    node_id: "ND_fixture".into(),
                    extension: None,
                    cpus: 2,
                },
            )
            .unwrap();
            assert_eq!(normalized.enable_psrpc_for_get_list_participants, enabled);
        }
        assert!(!Config::default().api.enable_psrpc_for_get_list_participants);
    }

    #[test]
    fn reconnect_flags_project_real_loader_defaults_yaml_env_and_cli_precedence() {
        use crate::{EntryAction, Env, ProgramInfo, parse_with_probe};
        use std::ffi::OsString;
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        };
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        let fields = ["publication", "subscription", "data_channel"];
        for value in [None, Some(false), Some(true)] {
            let yaml = fields.iter().fold(
                if value.is_some() {
                    String::from("rtc:\n")
                } else {
                    String::new()
                },
                |mut out, name| {
                    if let Some(value) = value {
                        out.push_str(&format!("  reconnect_on_{name}_error: {value}\n"));
                    }
                    out
                },
            );
            let argv = ["livekit-server", "--dev", "--config-body", yaml.as_str()]
                .into_iter()
                .map(OsString::from)
                .collect();
            let EntryAction::RunServer(config) =
                parse_with_probe(argv, &Env::default(), &info, &probe).unwrap()
            else {
                panic!("actual server loader")
            };
            assert_eq!(config.official.rtc.reconnect_on_publication_error, value);
            let rtc = &config.server.rtc;
            let expected = value.unwrap_or(false);
            assert_eq!(
                (
                    rtc.reconnect_on_publication_error,
                    rtc.reconnect_on_subscription_error,
                    rtc.reconnect_on_data_channel_error
                ),
                (expected, expected, expected)
            );
            let policy = rtc_core::config::RoomPolicy::from(&config.server);
            assert_eq!(
                (
                    policy.reconnect_on_publication_error,
                    policy.reconnect_on_subscription_error,
                    policy.reconnect_on_data_channel_error
                ),
                (expected, expected, expected)
            );
        }
        let env = Env::from_pairs([
            ("LIVEKIT_RTC_RECONNECT_ON_PUBLICATION_ERROR", "true"),
            ("LIVEKIT_RTC_RECONNECT_ON_SUBSCRIPTION_ERROR", "true"),
            ("LIVEKIT_RTC_RECONNECT_ON_DATA_CHANNEL_ERROR", "true"),
        ]);
        let argv = [
            "livekit-server",
            "--dev",
            "--rtc.reconnect_on_publication_error=false",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let EntryAction::RunServer(config) = parse_with_probe(argv, &env, &info, &probe).unwrap()
        else {
            panic!("actual env+CLI loader")
        };
        assert!(!config.server.rtc.reconnect_on_publication_error);
        assert!(config.server.rtc.reconnect_on_subscription_error);
        assert!(config.server.rtc.reconnect_on_data_channel_error);
    }

    #[test]
    fn reconnect_public_loader_drives_real_core_pending_publication_timeout() {
        use rtc_core::ids::*;
        use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent};
        use rtc_core::model::*;
        use rtc_core::permission::{Grants, VideoGrant};
        use rtc_core::room::{Effect, Room};
        use rtc_core::session::*;
        use std::ffi::OsString;
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        struct NoData;
        impl rtc_core::data::DataHandler for NoData {
            fn inbound(
                &self,
                _: &rtc_core::data::DataSender<'_>,
                _: DataKind,
                _: &str,
                _: &[u8],
            ) -> rtc_core::data::DataVerdict {
                rtc_core::data::DataVerdict::Drop
            }
        }
        fn policy_join(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
            JoinParams {
                room: RoomKey::new(ProjectId::local(), "r1"),
                grants: Grants {
                    identity: identity.into(),
                    video: VideoGrant {
                        room_join: true,
                        room: "r1".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                identity: identity.into(),
                name: String::new(),
                metadata: String::new(),
                attributes: BTreeMap::new(),
                client: ClientMeta {
                    sdk: SdkKind::Js,
                    protocol,
                    ..Default::default()
                },
                capabilities: ClientCapabilities {
                    video_publication_requires_rtp: false,
                    codec_switch_requires_renegotiation: false,
                    ice_tcp: true,
                    one_shot: false,
                    subscriber_primary: true,
                    speaker_changed: true,
                    connection_quality: true,
                    ice_lite: true,
                    unpublish: true,
                    fast_start: true,
                    disconnected_update: true,
                    sync_stream_id: true,
                    disable_playout_delay_for_sync: false,
                    connection_quality_lost: true,
                    identity_based_reconnection: true,
                    regions_in_leave: true,
                    non_error_signal_response: true,
                    request_response: true,
                    audio_red: true,
                },
                auto_subscribe: true,
                auto_subscribe_data_track: false,
                adaptive_stream: false,
                single_pc,
                disable_ice_lite: false,
                subscriber_allow_pause: None,
                reconnect: false,
                reconnect_reason: 0,
                participant_sid: None,
                add_tracks: vec![],
                publisher_offer: None,
            }
        }
        fn policy_add_track(cid: &str, kind: TrackKind, source: TrackSource) -> AddTrack {
            AddTrack {
                token: 1,
                cid: cid.into(),
                name: String::new(),
                kind,
                width: 0,
                height: 0,
                muted: false,
                disable_dtx: false,
                source,
                layers: vec![],
                simulcast_codecs: vec![],
                sid: None,
                stereo: false,
                disable_red: false,
                encryption: EncryptionType::None,
                stream: String::new(),
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                audio_features: vec![],
                packet_trailer_features: vec![],
            }
        }
        for enabled in [false, true] {
            let info = crate::ProgramInfo {
                binary_name: "livekit-server".into(),
                product_version: "0.1.0".into(),
                compat_version: "1.13.10",
            };
            let argv = [
                "livekit-server".to_owned(),
                "--dev".to_owned(),
                format!("--rtc.reconnect_on_publication_error={enabled}"),
            ]
            .into_iter()
            .map(OsString::from)
            .collect();
            let crate::EntryAction::RunServer(config) = crate::parse_with_probe(
                argv,
                &crate::Env::default(),
                &info,
                &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
            )
            .unwrap() else {
                panic!("full public loader")
            };
            let now = Instant::now();
            let mut room = Room::new(
                RoomKey::new(ProjectId::local(), "r1"),
                Arc::new(rtc_core::config::RoomPolicy::from(&config.server)),
                Arc::new(NoData),
                &Default::default(),
                now,
            );
            room.take_effects();
            let joined = room
                .join(policy_join("pending-publisher", 17, true), now)
                .unwrap();
            let transport = room
                .take_effects()
                .into_iter()
                .find_map(|effect| match effect {
                    Effect::Media(MediaCommand::CreateTransport(spec))
                        if spec.participant == joined.sid =>
                    {
                        Some(spec.id)
                    }
                    _ => None,
                })
                .expect("actual created transport");
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: joined.sid.clone(),
                    transport,
                },
                now,
            );
            room.take_effects();
            room.handle_request(
                &joined.sid,
                joined.generation,
                SessionRequest::AddTrack(policy_add_track(
                    "never-sends-media",
                    TrackKind::Audio,
                    TrackSource::Microphone,
                )),
                now,
            );
            room.take_effects();
            room.tick(now + Duration::from_secs(31));
            let effects = room.take_effects();
            let leave=effects.iter().any(|effect|matches!(effect,Effect::Signal{to,event:SignalEvent::Leave{reason:DisconnectReason::StateMismatch,action:LeaveAction::Reconnect}}if to==&joined.sid));
            assert_eq!(
                leave, enabled,
                "actual parsed flag must control room-owner publication error action"
            );
            assert_eq!(room.participant_generation(&joined.sid).is_none(), enabled);
        }
    }
    #[test]
    fn signed_name_public_loader_rejects_real_metadata_update_without_writes() {
        use rtc_core::ids::*;
        use rtc_core::media_api::DataKind;
        use rtc_core::permission::{Grants, VideoGrant};
        use rtc_core::room::{Effect, Room};
        use rtc_core::session::*;
        use std::ffi::OsString;
        use std::sync::Arc;
        use std::time::Instant;
        struct NoData;
        impl rtc_core::data::DataHandler for NoData {
            fn inbound(
                &self,
                _: &rtc_core::data::DataSender<'_>,
                _: DataKind,
                _: &str,
                _: &[u8],
            ) -> rtc_core::data::DataVerdict {
                rtc_core::data::DataVerdict::Drop
            }
        }
        fn policy_join(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
            JoinParams {
                room: RoomKey::new(ProjectId::local(), "r1"),
                grants: Grants {
                    identity: identity.into(),
                    video: VideoGrant {
                        room_join: true,
                        room: "r1".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                identity: identity.into(),
                name: String::new(),
                metadata: String::new(),
                attributes: BTreeMap::new(),
                client: ClientMeta {
                    sdk: SdkKind::Js,
                    protocol,
                    ..Default::default()
                },
                capabilities: ClientCapabilities {
                    video_publication_requires_rtp: false,
                    codec_switch_requires_renegotiation: false,
                    ice_tcp: true,
                    one_shot: false,
                    subscriber_primary: true,
                    speaker_changed: true,
                    connection_quality: true,
                    ice_lite: true,
                    unpublish: true,
                    fast_start: true,
                    disconnected_update: true,
                    sync_stream_id: true,
                    disable_playout_delay_for_sync: false,
                    connection_quality_lost: true,
                    identity_based_reconnection: true,
                    regions_in_leave: true,
                    non_error_signal_response: true,
                    request_response: true,
                    audio_red: true,
                },
                auto_subscribe: true,
                auto_subscribe_data_track: false,
                adaptive_stream: false,
                single_pc,
                disable_ice_lite: false,
                subscriber_allow_pause: None,
                reconnect: false,
                reconnect_reason: 0,
                participant_sid: None,
                add_tracks: vec![],
                publisher_offer: None,
            }
        }

        let info = crate::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        for limit in [-1i64, 0, 3] {
            let body = format!("limit:\n  max_participant_name_length: {limit}\n");
            let argv = [
                "livekit-server".to_owned(),
                "--dev".to_owned(),
                "--node-ip".to_owned(),
                "127.0.0.1".to_owned(),
                "--config-body".to_owned(),
                body,
            ]
            .into_iter()
            .map(OsString::from)
            .collect();
            let crate::EntryAction::RunServer(config) = crate::parse_with_probe(
                argv,
                &crate::Env::default(),
                &info,
                &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
            )
            .unwrap() else {
                panic!("actual public server loader")
            };
            let now = Instant::now();
            let mut room = Room::new(
                RoomKey::new(ProjectId::local(), "r1"),
                Arc::new(rtc_core::config::RoomPolicy::from(&config.server)),
                Arc::new(NoData),
                &Default::default(),
                now,
            );
            room.take_effects();
            let mut join = policy_join("name-owner", 17, true);
            join.name = "existing".into();
            join.grants.video.can_update_own_metadata = Some(true);
            let actor = room.join(join, now).unwrap();
            room.take_effects();
            for (request_id, name) in [(1, ""), (2, "éa"), (3, "éé")] {
                let before = room.get_participant(&"name-owner".into()).unwrap();
                room.handle_request(
                    &actor.sid,
                    actor.generation,
                    SessionRequest::UpdateMetadata {
                        token: request_id,
                        request_id,
                        name: name.into(),
                        metadata: format!("attempt-{request_id}"),
                        attributes: BTreeMap::new(),
                    },
                    now,
                );
                let effects = room.take_effects();
                let rejected = limit < 0 || (limit > 0 && name.len() > limit as usize);
                let expected = if rejected {
                    RequestOutcome::LimitExceeded
                } else {
                    RequestOutcome::Ok
                };
                assert!(effects.iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::RequestResponse{request_id:id,outcome,..}}if to==&actor.sid&&*id==request_id&&*outcome==expected)),"signed limit {limit}, real update {request_id}");
                let after = room.get_participant(&"name-owner".into()).unwrap();
                if rejected {
                    assert_eq!(after.name, before.name);
                    assert_eq!(after.metadata, before.metadata);
                } else {
                    assert_eq!(
                        after.name,
                        if name.is_empty() {
                            before.name.as_str()
                        } else {
                            name
                        }
                    );
                    assert_eq!(after.metadata, format!("attempt-{request_id}"));
                }
            }
        }
    }
}

/// Actual RoomConfiguration protoyaml fields (fixed Go 24-row oracle).
fn recording_preset(
    e: &crate::official::RecordingRoomEgress,
) -> rtc_core::auto_egress::AutomaticRecording {
    use rtc_core::auto_egress as a;
    let file = |v: &Option<crate::official::RecordingFile>| {
        let v = v.clone().unwrap_or_default();
        a::EncodedFilePolicy {
            file_type: v.file_type,
            filepath: v.filepath,
            disable_manifest: v.disable_manifest,
            destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
        }
    };
    let segments = |v: &Option<crate::official::RecordingSegments>| {
        let v = v.clone().unwrap_or_default();
        a::SegmentedFilePolicy {
            protocol: v.protocol,
            filename_prefix: v.filename_prefix,
            playlist_name: v.playlist_name,
            live_playlist_name: v.live_playlist_name,
            segment_duration: v.segment_duration,
            filename_suffix: v.filename_suffix,
            disable_manifest: v.disable_manifest,
            destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
        }
    };
    a::AutomaticRecording {
        tracks: e.tracks.as_ref().map(|v| a::TrackRecordingPolicy {
            filepath: v.filepath.clone(),
            disable_manifest: v.disable_manifest,
            destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
        }),
        participant: e
            .participant
            .as_ref()
            .map(|v| a::ParticipantRecordingPolicy {
                encoding: recording_encoding(&v.preset, &v.advanced),
                files: v.file_outputs.iter().map(file).collect(),
                segments: v.segment_outputs.iter().map(segments).collect(),
            }),
        room: e.room.as_ref().map(|v| a::RoomCompositeRecordingPolicy {
            room_name: v.room_name.clone(),
            layout: v.layout.clone(),
            audio_only: v.audio_only,
            audio_mixing: v.audio_mixing,
            video_only: v.video_only,
            custom_base_url: v.custom_base_url.clone(),
            legacy_output: v
                .file
                .as_ref()
                .map(|x| a::CompositeLegacyOutput::File(recording_file(x)))
                .or_else(|| {
                    v.stream.as_ref().map(|x| {
                        a::CompositeLegacyOutput::Stream(a::StreamRecordingPolicy {
                            protocol: x.protocol,
                            urls: x.urls.clone(),
                        })
                    })
                })
                .or_else(|| {
                    v.segments
                        .as_ref()
                        .map(|x| a::CompositeLegacyOutput::Segments(recording_segments(x)))
                }),
            encoding: recording_encoding(&v.preset, &v.advanced),
            files: v.file_outputs.iter().map(file).collect(),
            segments: v.segment_outputs.iter().map(segments).collect(),
            streams: v
                .stream_outputs
                .iter()
                .map(|v| {
                    let v = v.clone().unwrap_or_default();
                    a::StreamRecordingPolicy {
                        protocol: v.protocol,
                        urls: v.urls,
                    }
                })
                .collect(),
            images: v
                .image_outputs
                .iter()
                .map(|v| {
                    let v = v.clone().unwrap_or_default();
                    a::ImageRecordingPolicy {
                        capture_interval: v.capture_interval,
                        width: v.width,
                        height: v.height,
                        filename_prefix: v.filename_prefix,
                        filename_suffix: v.filename_suffix,
                        image_codec: v.image_codec,
                        disable_manifest: v.disable_manifest,
                        destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
                    }
                })
                .collect(),
            webhooks: v
                .webhooks
                .iter()
                .map(|v| {
                    let v = v.clone().unwrap_or_default();
                    a::RecordingWebhook {
                        url: v.url,
                        signing_key: v.signing_key,
                        filter: v.filter_params.map(|f| a::RecordingEventFilter {
                            include_events: f.include_events,
                            exclude_events: f.exclude_events,
                        }),
                    }
                })
                .collect(),
        }),
    }
}

#[cfg(test)]
mod automatic_recording_yaml_tests {
    use super::*;
    #[test]
    fn actual_protoyaml_fields_project_all_local_outputs_and_preserves_nil_empty_egress() {
        let mut config = crate::official::Config::default();
        crate::official::decode_yaml("room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          audio_only: true\n          file_outputs:\n            - filepath: room.ogg\n              file_type: 1\n              disable_manifest: true\n        participant:\n          segment_outputs:\n            - filename_prefix: participant\n              segment_duration: 4\n        tracks:\n          filepath: track.ogg\n          disable_manifest: true\n    empty:\n      egress: {}\n    absent: {}\n",true,&mut config).unwrap();
        let value = preset(config.room.room_configurations["record"].as_ref().unwrap());
        let value = value.automatic_recording.unwrap();
        assert!(value.room.unwrap().audio_only);
        assert_eq!(value.participant.unwrap().segments[0].segment_duration, 4);
        assert!(value.tracks.unwrap().disable_manifest);
        assert_eq!(
            preset(config.room.room_configurations["empty"].as_ref().unwrap()).automatic_recording,
            Some(Default::default())
        );
        assert!(
            preset(config.room.room_configurations["absent"].as_ref().unwrap())
                .automatic_recording
                .is_none()
        );
    }
    #[test]
    fn lowercase_generated_struct_spelling_is_rejected_by_protoyaml() {
        let mut config = crate::official::Config::default();
        assert!(crate::official::decode_yaml("room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          audioonly: true\n",true,&mut config).is_err());
    }
}

fn recording_destination(
    s3: &Option<crate::official::RecordingS3>,
    gcp: &Option<crate::official::RecordingGcp>,
    azure: &Option<crate::official::RecordingAzure>,
    ali: &Option<crate::official::RecordingAli>,
) -> Option<rtc_core::auto_egress::UploadDestination> {
    use rtc_core::auto_egress as a;
    let proxy = |p: &crate::official::RecordingProxy| a::UploadProxy {
        url: p.url.clone(),
        username: p.username.clone(),
        password: p.password.clone(),
    };
    if let Some(v) = s3 {
        return Some(a::UploadDestination::S3(Box::new(a::S3Destination {
            access_key: v.access_key.clone(),
            secret: v.secret.clone(),
            region: v.region.clone(),
            endpoint: v.endpoint.clone(),
            bucket: v.bucket.clone(),
            force_path_style: v.force_path_style,
            metadata: v.metadata.clone(),
            tagging: v.tagging.clone(),
            content_disposition: v.content_disposition.clone(),
            proxy: v.proxy.as_ref().map(proxy),
            session_token: v.session_token.clone(),
            assume_role_arn: v.assume_role_arn.clone(),
            assume_role_external_id: v.assume_role_external_id.clone(),
        })));
    }
    if let Some(v) = gcp {
        return Some(a::UploadDestination::Gcp(a::GcpDestination {
            credentials: v.credentials.clone(),
            bucket: v.bucket.clone(),
            proxy: v.proxy.as_ref().map(proxy),
        }));
    }
    if let Some(v) = azure {
        return Some(a::UploadDestination::Azure(a::AzureDestination {
            account_name: v.account_name.clone(),
            account_key: v.account_key.clone(),
            container_name: v.container_name.clone(),
        }));
    }
    ali.as_ref().map(|v| {
        a::UploadDestination::AliOss(a::AliOssDestination {
            access_key: v.access_key.clone(),
            secret: v.secret.clone(),
            region: v.region.clone(),
            endpoint: v.endpoint.clone(),
            bucket: v.bucket.clone(),
        })
    })
}
fn recording_encoding(
    p: &Option<i32>,
    advanced: &Option<crate::official::RecordingAdvanced>,
) -> Option<rtc_core::auto_egress::RecordingEncoding> {
    use rtc_core::auto_egress as a;
    if let Some(v) = p {
        return Some(a::RecordingEncoding::Preset(*v));
    }
    advanced.as_ref().map(|v| {
        a::RecordingEncoding::Advanced(a::AdvancedEncoding {
            width: v.width,
            height: v.height,
            depth: v.depth,
            framerate: v.framerate,
            audio_codec: v.audio_codec,
            audio_bitrate: v.audio_bitrate,
            audio_frequency: v.audio_frequency,
            video_codec: v.video_codec,
            video_bitrate: v.video_bitrate,
            key_frame_interval: a::FloatBits::from_value(v.key_frame_interval),
            audio_quality: v.audio_quality,
            video_quality: v.video_quality,
        })
    })
}
fn recording_file(v: &crate::official::RecordingFile) -> rtc_core::auto_egress::EncodedFilePolicy {
    rtc_core::auto_egress::EncodedFilePolicy {
        file_type: v.file_type,
        filepath: v.filepath.clone(),
        disable_manifest: v.disable_manifest,
        destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
    }
}
fn recording_segments(
    v: &crate::official::RecordingSegments,
) -> rtc_core::auto_egress::SegmentedFilePolicy {
    rtc_core::auto_egress::SegmentedFilePolicy {
        protocol: v.protocol,
        filename_prefix: v.filename_prefix.clone(),
        playlist_name: v.playlist_name.clone(),
        live_playlist_name: v.live_playlist_name.clone(),
        segment_duration: v.segment_duration,
        filename_suffix: v.filename_suffix,
        disable_manifest: v.disable_manifest,
        destination: recording_destination(&v.s3, &v.gcp, &v.azure, &v.aliOSS),
    }
}

#[cfg(test)]
mod source_room_tags_projection_tests {
    use super::*;
    #[test]
    fn descriptor_yaml_tags_are_admitted_and_retained_in_neutral_preset() {
        let mut config = Config::default();
        crate::official::decode_yaml("room: {room_configurations: {record: {tags: {plain: one, utf8: hé, clear: null, plain: two}}}}",true,&mut config).unwrap();
        crate::unsupported::check(&config).unwrap();
        let neutral = preset(config.room.room_configurations["record"].as_ref().unwrap());
        assert_eq!(neutral.tags.get("plain").unwrap(), "two");
        assert_eq!(neutral.tags.get("utf8").unwrap(), "hé");
        assert_eq!(neutral.tags.get("clear").unwrap(), "null");
        assert!(
            crate::official::decode_yaml(
                "room: {room_configurations: {record: {tags: null}}}",
                true,
                &mut Config::default()
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod create_room_request_policy_projection_tests {
    use super::*;
    #[test]
    fn signed_create_policy_survives_real_normalization_without_clamping() {
        for attempts in [-1, 0, 1, 3] {
            for nanos in [-1, 0, 37_000_000] {
                let mut input = Config::default();
                input.rtc.node_ip.v4 = "127.0.0.1".into();
                input.room.create_room_attempts = attempts;
                input.room.create_room_timeout = crate::duration::GoDuration(nanos);
                let (actual, _) = to_server_config(
                    &input,
                    Extras {
                        node_ip_auto_generated: true,
                        node_id: "ND_projection".into(),
                        extension: None,
                        cpus: 4,
                    },
                )
                .unwrap();
                assert_eq!(actual.room.create_room_attempts, attempts);
                assert_eq!(actual.room.create_room_timeout.as_nanos_i64(), nanos);
            }
        }
    }
    #[test]
    fn skip_external_validation_projects_real_yaml_cli_env_and_cli_false_precedence() {
        use crate::{EntryAction, Env, ProgramInfo, parse_with_probe};
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for (extra, env, expected) in [
            (vec![], Env::default(), false),
            (
                vec!["--config-body", "rtc: {skip_external_ip_validation: true}"],
                Env::default(),
                true,
            ),
            (
                vec!["--rtc.skip_external_ip_validation=true"],
                Env::default(),
                true,
            ),
            (
                vec![],
                Env::from_pairs([("LIVEKIT_RTC_SKIP_EXTERNAL_IP_VALIDATION", "true")]),
                true,
            ),
            (
                vec!["--rtc.skip_external_ip_validation=false"],
                Env::from_pairs([("LIVEKIT_RTC_SKIP_EXTERNAL_IP_VALIDATION", "true")]),
                false,
            ),
        ] {
            let mut argv = vec!["livekit-server", "--dev"];
            argv.extend(extra);
            let EntryAction::RunServer(config) = parse_with_probe(
                argv.into_iter().map(Into::into).collect(),
                &env,
                &info,
                &probe,
            )
            .unwrap() else {
                panic!("actual server loader")
            };
            assert_eq!(config.official.rtc.skip_external_ip_validation, expected);
            assert_eq!(
                config.server.rtc.skip_external_ip_validation, expected,
                "must reach runtime projection"
            );
        }
    }
    #[test]
    fn forward_stats_all_three_source_activation_and_shared_runtime_projection() {
        use crate::{EntryAction, Env, ProgramInfo, parse_with_probe};
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for (body, enabled, valid) in [
            (
                "rtc: {forward_stats: {summary_interval: 100ms, report_interval: 1s}}",
                false,
                true,
            ),
            (
                "rtc: {forward_stats: {summary_interval: 100ms, report_interval: 1s, report_window: 250ms}}",
                true,
                true,
            ),
            (
                "rtc: {forward_stats: {summary_interval: 100ms, report_interval: 1s, report_window: -1s}}",
                true,
                true,
            ),
            (
                "rtc: {forward_stats: {summary_interval: -1s, report_interval: 1s, report_window: 1s}}",
                false,
                false,
            ),
            (
                "rtc: {forward_stats: {summary_interval: -1s, report_interval: 0s, report_window: 1s}}",
                false,
                true,
            ),
        ] {
            let result = parse_with_probe(
                vec![
                    "livekit-server".into(),
                    "--dev".into(),
                    "--config-body".into(),
                    body.into(),
                ],
                &Env::default(),
                &info,
                &probe,
            );
            if !valid {
                assert!(
                    result.is_err(),
                    "source nonpositive ticker must reject startup"
                );
                continue;
            }
            let EntryAction::RunServer(config) = result.unwrap() else {
                panic!("server config")
            };
            assert_eq!(config.server.rtc.forward_stats.is_some(), enabled);
            if let Some(collector) = config.server.rtc.forward_stats.as_ref() {
                let cloned = config.server.rtc.clone();
                assert!(
                    std::sync::Arc::ptr_eq(collector, cloned.forward_stats.as_ref().unwrap()),
                    "rooms share one node collector"
                );
            }
        }
    }
    #[test]
    fn forward_stats_env_cli_and_explicit_zero_precedence_reach_real_activation() {
        use crate::{EntryAction, Env, ProgramInfo, parse_with_probe};
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        let enabled_env = Env::from_pairs([
            ("LIVEKIT_RTC_FORWARD_STATS_SUMMARY_INTERVAL", "100ms"),
            ("LIVEKIT_RTC_FORWARD_STATS_REPORT_INTERVAL", "1s"),
            ("LIVEKIT_RTC_FORWARD_STATS_REPORT_WINDOW", "250ms"),
        ]);
        for (flags, env, enabled) in [
            (vec![], enabled_env.clone(), true),
            (
                vec!["--rtc.forward_stats.summary_interval=0s"],
                enabled_env,
                false,
            ),
            (
                vec![
                    "--rtc.forward_stats.summary_interval=100ms",
                    "--rtc.forward_stats.report_interval=1s",
                    "--rtc.forward_stats.report_window=250ms",
                ],
                Env::default(),
                true,
            ),
        ] {
            let mut argv = vec!["livekit-server", "--dev"];
            argv.extend(flags);
            let EntryAction::RunServer(config) = parse_with_probe(
                argv.into_iter().map(Into::into).collect(),
                &env,
                &info,
                &probe,
            )
            .unwrap() else {
                panic!("actual server loader")
            };
            assert_eq!(config.server.rtc.forward_stats.is_some(), enabled);
        }
    }
}

#[cfg(test)]
mod ice_acceptance_projection_tests {
    use super::*;
    use crate::{EntryAction, Env, ProgramInfo, parse_with_probe};

    #[test]
    fn four_nomination_waits_reach_runtime_from_yaml_cli_and_environment() {
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        let keys = ["host", "srflx", "prflx", "relay"];
        let defaults = [
            Duration::ZERO,
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
        ];
        for (value, override_wait) in [
            ("-1ns", None),
            ("0s", None),
            ("1ns", Some(Duration::from_nanos(1))),
            ("370ms", Some(Duration::from_millis(370))),
        ] {
            let body = format!(
                "rtc: {{{}}}",
                keys.iter()
                    .map(|kind| format!("{kind}_acceptance_min_wait: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let cli = keys
                .iter()
                .map(|kind| format!("--rtc.{kind}_acceptance_min_wait={value}"))
                .collect::<Vec<_>>();
            let env_pairs = keys
                .iter()
                .map(|kind| {
                    (
                        format!("LIVEKIT_RTC_{}_ACCEPTANCE_MIN_WAIT", kind.to_uppercase()),
                        value.to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            for (arguments, env) in [
                (vec!["--config-body".to_owned(), body], Env::default()),
                (cli, Env::default()),
                (vec![], Env::from_pairs(env_pairs)),
            ] {
                let mut argv = vec!["livekit-server".to_owned(), "--dev".to_owned()];
                argv.extend(arguments);
                let EntryAction::RunServer(config) = parse_with_probe(
                    argv.into_iter().map(Into::into).collect(),
                    &env,
                    &info,
                    &probe,
                )
                .unwrap() else {
                    panic!("must actually enter the server loader")
                };
                assert_eq!(
                    config.server.rtc.ice_acceptance_min_waits,
                    defaults.map(|inherited| override_wait.unwrap_or(inherited)),
                    "nonpositive values inherit Pion defaults, not zero for every kind"
                );
            }
        }
    }
}
