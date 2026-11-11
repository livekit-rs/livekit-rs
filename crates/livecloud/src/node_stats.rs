//! Runtime node sampling corresponding to fixed routing/nodestats.go and
//! protocol/utils/hwstats. OS FFI is used only for native system statistics.
use lk_protocol::livekit as pb;
use rtc_runtime::RoomManager;
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant},
};

pub struct Sampler {
    manager: RoomManager,
    intervals: Vec<Duration>,
    capacity: usize,
    state: Mutex<State>,
}
struct State {
    cpu: CpuMonitor,
    history: VecDeque<pb::NodeStats>,
}
impl std::fmt::Debug for Sampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStatsSampler").finish_non_exhaustive()
    }
}
impl Sampler {
    pub fn new(
        manager: RoomManager,
        update: Duration,
        intervals: Vec<Duration>,
    ) -> std::io::Result<Self> {
        let max = intervals.iter().copied().max().unwrap_or_default();
        let capacity = if update.is_zero() {
            1
        } else {
            max.as_nanos()
                .div_ceil(update.as_nanos())
                .max(1)
                .min(usize::MAX as u128) as usize
        };
        Ok(Self {
            manager,
            intervals,
            capacity,
            state: Mutex::new(State {
                cpu: CpuMonitor::new()?,
                history: VecDeque::new(),
            }),
        })
    }
}
impl lk_compat::routing::NodeStatsProvider for Sampler {
    fn poll_system(&self) -> Result<(), String> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cpu
            .poll()
            .map_err(|e| e.to_string())
    }
    fn sample(&self, started: i64, now: i64) -> Result<pb::NodeStats, String> {
        let counts = self.manager.node_stats();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let load = load_average().map_err(|e| e.to_string())?;
        // Upstream deliberately ignores memory errors (e.g. missing vm_stat on macOS).
        let (memory_used, memory_total) = memory().unwrap_or_default();
        let (sys_packets_out, sys_packets_dropped) = crate::node_tc::sample().unwrap_or_default();
        let mut stats = pb::NodeStats {
            forward_latency: counts.forward_stats.latency_ns,
            started_at: started,
            updated_at: now,
            num_rooms: counts.rooms as i32,
            num_clients: counts.participants as i32,
            num_tracks_in: counts.tracks_in as i32,
            num_tracks_out: counts.tracks_out as i32,
            num_track_publish_attempts: counts.lifecycle.track_publish_attempts as i32,
            num_track_publish_success: counts.lifecycle.track_publish_success as i32,
            num_track_publish_cancels: counts.lifecycle.track_publish_cancels as i32,
            num_track_subscribe_attempts: counts.lifecycle.track_subscribe_attempts as i32,
            num_track_subscribe_success: counts.lifecycle.track_subscribe_success as i32,
            num_track_subscribe_cancels: counts.lifecycle.track_subscribe_cancels as i32,
            participant_signal_connected: counts.lifecycle.participant_signal_connected,
            participant_rtc_init: counts.lifecycle.participant_rtc_init,
            participant_rtc_connected: counts.lifecycle.participant_rtc_connected,
            participant_rtc_canceled: counts.lifecycle.participant_rtc_canceled,
            participant_rtc_active: counts.lifecycle.participant_rtc_active,
            bytes_in: counts.bytes_in,
            bytes_out: counts.bytes_out,
            packets_in: counts.packets_in,
            packets_out: counts.packets_out,
            nack_total: counts.nacks,
            sys_packets_out,
            sys_packets_dropped,
            retransmit_bytes_out: counts.retransmit_bytes_out,
            retransmit_packets_out: counts.retransmit_packets_out,
            num_cpus: state.cpu.effective_cpus() as u32,
            cpu_load: state.cpu.load as f32,
            load_avg_last1min: load[0] as f32,
            load_avg_last5min: load[1] as f32,
            load_avg_last15min: load[2] as f32,
            memory_used,
            memory_total,
            ..Default::default()
        };
        let history: Vec<_> = state.history.iter().cloned().collect();
        for interval in &self.intervals {
            if let Some(first) = history
                .iter()
                .rposition(|old| now - old.updated_at >= interval.as_secs() as i64)
            {
                let mut window = history[first..].to_vec();
                window.push(stats.clone());
                if let Some(rate) = rate(&window) {
                    stats.rates.push(rate);
                }
            }
        }
        if state.history.len() >= self.capacity {
            state.history.pop_front();
        }
        state.history.push_back(stats.clone());
        Ok(stats)
    }
}
fn rate(history: &[pb::NodeStats]) -> Option<pb::NodeStatsRate> {
    let first = history.first()?;
    let last = history.last()?;
    let elapsed = last.updated_at - first.updated_at;
    if elapsed <= 0 {
        return None;
    }
    let mut out = pb::NodeStatsRate {
        started_at: first.updated_at,
        ended_at: last.updated_at,
        duration: elapsed,
        ..Default::default()
    };
    macro_rules! per_sec { ($($dst:ident : $src:ident),+ $(,)?) => {$(out.$dst=(last.$src as u64).wrapping_sub(first.$src as u64) as f32 / elapsed as f32;)+}; }
    per_sec!(bytes_in:bytes_in, bytes_out:bytes_out, packets_in:packets_in, packets_out:packets_out, nack_total:nack_total,
        retransmit_bytes_out:retransmit_bytes_out,retransmit_packets_out:retransmit_packets_out,
        track_publish_attempts:num_track_publish_attempts,track_publish_success:num_track_publish_success,track_publish_cancels:num_track_publish_cancels,
        track_subscribe_attempts:num_track_subscribe_attempts,track_subscribe_success:num_track_subscribe_success,track_subscribe_cancels:num_track_subscribe_cancels,
        participant_signal_connected:participant_signal_connected, participant_rtc_init:participant_rtc_init,
        participant_rtc_connected:participant_rtc_connected,participant_rtc_canceled:participant_rtc_canceled,participant_rtc_active:participant_rtc_active,
        sys_packets_out:sys_packets_out,sys_packets_dropped:sys_packets_dropped);
    for pair in history.windows(2).rev() {
        let span = pair[1].updated_at - pair[0].updated_at;
        if span <= 0 {
            continue;
        }
        let s = &pair[1];
        let weight = span as f32;
        out.cpu_load += s.cpu_load * weight;
        out.memory_used += s.memory_used as f32 * weight;
        out.memory_total += s.memory_total as f32 * weight;
        if s.memory_total > 0 {
            out.memory_load += s.memory_used as f32 / s.memory_total as f32 * weight;
        }
    }
    out.cpu_load /= elapsed as f32;
    out.memory_used /= elapsed as f32;
    out.memory_total /= elapsed as f32;
    out.memory_load /= elapsed as f32;
    Some(out)
}
fn invalid(message: &str) -> std::io::Error {
    std::io::Error::other(message)
}
#[derive(Clone, Copy)]
struct CpuTicks {
    idle: u64,
    total: u64,
}
struct CpuMonitor {
    previous: CpuTicks,
    sampled_at: Instant,
    cpus: f64,
    requested: Option<f64>,
    cgroup: Option<&'static str>,
    load: f64,
}
impl CpuMonitor {
    fn new() -> std::io::Result<Self> {
        let cpus = std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(1) as f64;
        let requested = std::env::var("LIVEKIT_CPU_REQUEST")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|x| *x > 0.0);
        let (cgroup, cpus) = cgroup_cpu(cpus)?;
        let previous = read_cpu(cgroup)?;
        Ok(Self {
            previous,
            sampled_at: Instant::now(),
            cpus,
            requested,
            cgroup,
            load: 0.0,
        })
    }
    fn effective_cpus(&self) -> f64 {
        self.requested.unwrap_or(self.cpus)
    }
    fn poll(&mut self) -> std::io::Result<()> {
        let next = read_cpu(self.cgroup)?;
        let now = Instant::now();
        let idle = if self.cgroup.is_some() {
            let busy = next.total.saturating_sub(self.previous.total) as f64
                / now.duration_since(self.sampled_at).as_nanos().max(1) as f64;
            (self.cpus - busy).clamp(0.0, self.cpus)
        } else {
            let total = next.total.wrapping_sub(self.previous.total);
            if total == 0 {
                return Ok(());
            }
            self.cpus * next.idle.wrapping_sub(self.previous.idle) as f64 / total as f64
        };
        self.load = (1.0 - idle / self.effective_cpus()).clamp(0.0, 1.0);
        self.previous = next;
        self.sampled_at = now;
        Ok(())
    }
}
#[cfg(target_os = "linux")]
fn cgroup_cpu(host: f64) -> std::io::Result<(Option<&'static str>, f64)> {
    use std::{fs, path::Path};
    let v1 = "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage";
    let v2 = "/sys/fs/cgroup/cpu.stat";
    if Path::new(v1).exists() {
        let q = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?
            .trim()
            .parse::<f64>()
            .map_err(|_| invalid("invalid cpu quota"))?;
        let p = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?
            .trim()
            .parse::<f64>()
            .map_err(|_| invalid("invalid cpu period"))?;
        return Ok((Some(v1), if q < 0.0 || p <= 0.0 { host } else { q / p }));
    }
    if Path::new(v2).exists() {
        let text =
            fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_else(|_| "max 100000".into());
        let cols: Vec<_> = text.split_whitespace().collect();
        let cpus = if cols.first() == Some(&"max") {
            host
        } else {
            quota(&text)?
        };
        return Ok((Some(v2), cpus));
    }
    Ok((None, host))
}
#[cfg(not(target_os = "linux"))]
fn cgroup_cpu(host: f64) -> std::io::Result<(Option<&'static str>, f64)> {
    Ok((None, host))
}
fn quota(text: &str) -> std::io::Result<f64> {
    let mut v = text.split_whitespace();
    let q = v
        .next()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| invalid("invalid cpu quota"))?;
    let p = v
        .next()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| invalid("invalid cpu period"))?;
    if q <= 0.0 || p <= 0.0 {
        return Err(invalid("nonpositive cpu quota or period"));
    }
    Ok(q / p)
}
#[cfg(target_os = "linux")]
fn read_cpu(cgroup: Option<&str>) -> std::io::Result<CpuTicks> {
    if let Some(path) = cgroup {
        let text = std::fs::read_to_string(path)?;
        let total = if path.ends_with("cpu.stat") {
            text.lines()
                .find_map(|l| l.strip_prefix("usage_usec "))
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| invalid("invalid cpu.stat"))?
                .saturating_mul(1000)
        } else {
            text.trim()
                .parse()
                .map_err(|_| invalid("invalid cpuacct.usage"))?
        };
        return Ok(CpuTicks { idle: 0, total });
    }
    let text = std::fs::read_to_string("/proc/stat")?;
    let vals: Vec<u64> = text
        .lines()
        .next()
        .ok_or_else(|| invalid("empty /proc/stat"))?
        .split_whitespace()
        .skip(1)
        .take(8)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .map_err(|_| invalid("invalid /proc/stat"))?;
    Ok(CpuTicks {
        idle: *vals.get(3).ok_or_else(|| invalid("missing idle"))?,
        total: vals.iter().sum(),
    })
}
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn mach_port_deallocate(
        task: libc::mach_port_t,
        name: libc::mach_port_t,
    ) -> libc::kern_return_t;
}
#[cfg(target_os = "macos")]
fn read_cpu(_: Option<&str>) -> std::io::Result<CpuTicks> {
    unsafe {
        let host = libc::mach_host_self();
        let mut value: libc::host_cpu_load_info = std::mem::zeroed();
        let mut count = libc::HOST_CPU_LOAD_INFO_COUNT;
        let result = libc::host_statistics(
            host,
            libc::HOST_CPU_LOAD_INFO,
            (&mut value as *mut libc::host_cpu_load_info).cast(),
            &mut count,
        );
        mach_port_deallocate(libc::mach_task_self(), host);
        if result != libc::KERN_SUCCESS {
            return Err(invalid("host_statistics cpu failed"));
        }
        Ok(CpuTicks {
            idle: value.cpu_ticks[libc::CPU_STATE_IDLE as usize] as u64,
            total: value.cpu_ticks.iter().map(|v| u64::from(*v)).sum(),
        })
    }
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_cpu(_: Option<&str>) -> std::io::Result<CpuTicks> {
    // Match the upstream null monitor: unsupported OS statistics must not
    // prevent a media node from starting; report idle until supported.
    Ok(CpuTicks { idle: 0, total: 0 })
}
#[cfg(unix)]
fn load_average() -> std::io::Result<[f64; 3]> {
    let mut v = [0.0; 3];
    if unsafe { libc::getloadavg(v.as_mut_ptr(), 3) } != 3 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(v)
}
#[cfg(not(unix))]
fn load_average() -> std::io::Result<[f64; 3]> {
    Ok([0.0; 3])
}
#[cfg(target_os = "macos")]
fn memory() -> std::io::Result<(u64, u64)> {
    unsafe {
        let host = libc::mach_host_self();
        let mut v: libc::vm_statistics64 = std::mem::zeroed();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        let result = libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            (&mut v as *mut libc::vm_statistics64).cast(),
            &mut count,
        );
        mach_port_deallocate(libc::mach_task_self(), host);
        if result != libc::KERN_SUCCESS {
            return Err(invalid("host_statistics64 memory failed"));
        }
        let page = libc::sysconf(libc::_SC_PAGESIZE);
        if page <= 0 {
            return Err(invalid("invalid page size"));
        }
        let used = (u64::from(v.wire_count)
            + u64::from(v.compressor_page_count)
            + u64::from(v.active_count)
            + u64::from(v.inactive_count)
            + u64::from(v.speculative_count))
        .wrapping_sub(u64::from(v.purgeable_count) + u64::from(v.external_page_count));
        let total = used
            + u64::from(v.purgeable_count)
            + u64::from(v.external_page_count)
            + u64::from(v.free_count);
        Ok((used * page as u64, total * page as u64))
    }
}
#[cfg(target_os = "linux")]
fn memory() -> std::io::Result<(u64, u64)> {
    use std::fs;
    let text = fs::read_to_string("/proc/meminfo")?;
    let get = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n * 1024)
            .unwrap_or_default()
    };
    let total = get("MemTotal:");
    let available = get("MemAvailable:");
    let used = if available > 0 {
        total.saturating_sub(available)
    } else {
        total.saturating_sub(get("MemFree:") + get("Buffers:") + get("Cached:"))
    };
    for (usage, max, stat, key, cap) in [
        (
            "/sys/fs/cgroup/memory/memory.usage_in_bytes",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            "/sys/fs/cgroup/memory/memory.stat",
            "total_inactive_file",
            true,
        ),
        (
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory.stat",
            "inactive_file",
            false,
        ),
    ] {
        if let Ok(n) = fs::read_to_string(usage) {
            let mut used = n
                .trim()
                .parse::<u64>()
                .map_err(|_| invalid("invalid memory usage"))?;
            let inactive = fs::read_to_string(stat)
                .ok()
                .and_then(|s| {
                    s.lines().find_map(|l| {
                        l.strip_prefix(&format!("{key} "))
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                })
                .unwrap_or_default();
            if used > inactive {
                used -= inactive;
            }
            let limit = fs::read_to_string(max)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(total);
            return Ok((used, if cap { limit.min(total) } else { limit }));
        }
    }
    Ok((used, total))
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn memory() -> std::io::Result<(u64, u64)> {
    Err(invalid("native memory monitor unsupported on this target"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_host_counters_are_consumable_and_bounded() {
        let mut cpu = CpuMonitor::new().unwrap();
        cpu.poll().unwrap();
        assert!((0.0..=1.0).contains(&cpu.load));
        assert!(cpu.effective_cpus() > 0.0);
        let (used, total) = memory().unwrap();
        assert!(total > 0);
        assert!(used <= total);
        assert!(
            load_average()
                .unwrap()
                .iter()
                .all(|v| v.is_finite() && *v >= 0.0)
        );
    }
    #[test]
    fn rate_windows_keep_time_weights_and_cumulative_totals() {
        let rows = vec![
            pb::NodeStats {
                updated_at: 1,
                bytes_in: 10,
                ..Default::default()
            },
            pb::NodeStats {
                updated_at: 3,
                bytes_in: 30,
                cpu_load: 0.25,
                memory_used: 25,
                memory_total: 100,
                ..Default::default()
            },
            pb::NodeStats {
                updated_at: 6,
                bytes_in: 60,
                cpu_load: 0.75,
                memory_used: 75,
                memory_total: 100,
                ..Default::default()
            },
        ];
        let r = rate(&rows).unwrap();
        assert_eq!(r.duration, 5);
        assert_eq!(r.bytes_in, 10.0);
        assert_eq!(r.cpu_load, 0.55);
        assert_eq!(r.memory_load, 0.55);
        assert!(rate(&rows[..1]).is_none());
        assert_eq!(quota("250000 100000").unwrap(), 2.5);
        assert!(quota("1 0").is_err());
    }
}
