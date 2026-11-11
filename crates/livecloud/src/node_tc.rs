//! Fixed Go `telemetry/prometheus/node_linux.go`: sum legacy TCA_STATS across
//! all qdiscs. STATS2 is deliberately not substituted for that source contract.
use std::io;

#[cfg(any(target_os = "linux", test))]
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed qdisc netlink response",
    )
}

#[cfg(any(target_os = "linux", test))]
fn attributes(mut data: &[u8], totals: &mut (u32, u32)) -> io::Result<()> {
    while !data.is_empty() {
        if data.len() < 4 {
            return Err(invalid());
        }
        let len = u16::from_ne_bytes(data[..2].try_into().unwrap()) as usize;
        let kind = u16::from_ne_bytes(data[2..4].try_into().unwrap()) & 0x3fff;
        if len < 4 || len > data.len() {
            return Err(invalid());
        }
        if kind == 3 {
            // TCA_STATS: struct tc_stats, not nested TCA_STATS2.
            if len < 4 + 36 {
                return Err(invalid());
            }
            let stats = &data[4..len];
            totals.0 = totals
                .0
                .wrapping_add(u32::from_ne_bytes(stats[8..12].try_into().unwrap()));
            totals.1 = totals
                .1
                .wrapping_add(u32::from_ne_bytes(stats[12..16].try_into().unwrap()));
        }
        let aligned = (len + 3) & !3;
        if aligned > data.len() {
            return Err(invalid());
        }
        data = &data[aligned..];
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn messages(mut data: &[u8], sequence: u32, totals: &mut (u32, u32)) -> io::Result<bool> {
    let mut done = false;
    while !data.is_empty() {
        if data.len() < 16 {
            return Err(invalid());
        }
        let len = u32::from_ne_bytes(data[..4].try_into().unwrap()) as usize;
        let kind = u16::from_ne_bytes(data[4..6].try_into().unwrap());
        let flags = u16::from_ne_bytes(data[6..8].try_into().unwrap());
        let seq = u32::from_ne_bytes(data[8..12].try_into().unwrap());
        if len < 16 || len > data.len() || seq != sequence {
            return Err(invalid());
        }
        if flags & 0x10 != 0 {
            // NLM_F_DUMP_INTR: incomplete snapshots cannot be accepted.
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "qdisc dump interrupted",
            ));
        }
        match kind {
            2 => {
                // NLMSG_ERROR contains a signed negative errno, or an ACK.
                if len < 20 {
                    return Err(invalid());
                }
                let code = i32::from_ne_bytes(data[16..20].try_into().unwrap());
                if code != 0 {
                    return Err(io::Error::from_raw_os_error(code.saturating_neg()));
                }
            }
            3 => {
                if len >= 20 {
                    let code = i32::from_ne_bytes(data[16..20].try_into().unwrap());
                    if code != 0 {
                        return Err(io::Error::from_raw_os_error(code.saturating_neg()));
                    }
                }
                done = true;
            }
            36 => {
                // RTM_NEWQDISC, followed by the 20-byte tcmsg.
                if len < 36 {
                    return Err(invalid());
                }
                attributes(&data[36..len], totals)?;
            }
            4 => return Err(io::Error::other("qdisc netlink overrun")),
            _ => {}
        }
        let aligned = (len + 3) & !3;
        if aligned > data.len() {
            return Err(invalid());
        }
        data = &data[aligned..];
    }
    Ok(done)
}

#[cfg(target_os = "linux")]
pub(crate) fn sample() -> io::Result<(u32, u32)> {
    use std::{
        mem,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };
    // SAFETY: all socket buffers and address lengths below refer to live,
    // initialized storage; OwnedFd closes the socket on every return path.
    unsafe {
        let raw = libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        );
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = OwnedFd::from_raw_fd(raw);
        let timeout = libc::timeval {
            tv_sec: 1,
            tv_usec: 0,
        };
        if libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout as *const _ as *const _,
            mem::size_of_val(&timeout) as _,
        ) < 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut address: libc::sockaddr_nl = mem::zeroed();
        address.nl_family = libc::AF_NETLINK as _;
        let mut request = [0u8; 36];
        request[..4].copy_from_slice(&36u32.to_ne_bytes());
        request[4..6].copy_from_slice(&38u16.to_ne_bytes()); // RTM_GETQDISC
        request[6..8].copy_from_slice(&0x301u16.to_ne_bytes()); // REQUEST | DUMP
        request[8..12].copy_from_slice(&1u32.to_ne_bytes());
        let sent = libc::sendto(
            socket.as_raw_fd(),
            request.as_ptr() as _,
            request.len(),
            0,
            &address as *const _ as *const _,
            mem::size_of_val(&address) as _,
        );
        if sent != request.len() as isize {
            return Err(io::Error::last_os_error());
        }
        let mut buffer = [0u8; 65536];
        let mut totals = (0, 0);
        loop {
            let mut sender: libc::sockaddr_nl = mem::zeroed();
            let mut sender_len = mem::size_of_val(&sender) as libc::socklen_t;
            let count = libc::recvfrom(
                socket.as_raw_fd(),
                buffer.as_mut_ptr() as _,
                buffer.len(),
                libc::MSG_TRUNC,
                &mut sender as *mut _ as *mut _,
                &mut sender_len,
            );
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 || count as usize > buffer.len() || sender.nl_pid != 0 {
                return Err(invalid());
            }
            if messages(&buffer[..count as usize], 1, &mut totals)? {
                return Ok(totals);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn sample() -> io::Result<(u32, u32)> {
    // Upstream node_nonlinux.go reports no TC counters on these platforms.
    Ok((0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message(kind: u16, flags: u16, payload: &[u8]) -> Vec<u8> {
        let len = 16 + payload.len();
        let mut out = vec![0; (len + 3) & !3];
        out[..4].copy_from_slice(&(len as u32).to_ne_bytes());
        out[4..6].copy_from_slice(&kind.to_ne_bytes());
        out[6..8].copy_from_slice(&flags.to_ne_bytes());
        out[8..12].copy_from_slice(&1u32.to_ne_bytes());
        out[16..len].copy_from_slice(payload);
        out
    }
    fn qdisc(packets: u32, drops: u32) -> Vec<u8> {
        let mut payload = vec![0; 20];
        let mut attr = vec![0; 44];
        attr[..2].copy_from_slice(&44u16.to_ne_bytes());
        attr[2..4].copy_from_slice(&3u16.to_ne_bytes());
        attr[12..16].copy_from_slice(&packets.to_ne_bytes());
        attr[16..20].copy_from_slice(&drops.to_ne_bytes());
        payload.extend(attr);
        // Modern kernels can provide both; source deliberately counts only
        // the correctly decoded legacy struct once, ignoring Stats2 contents.
        payload.extend([8, 0, 7, 0, 0xff, 0xff, 0xff, 0xff]);
        message(36, 2, &payload)
    }
    #[test]
    fn multipart_qdisc_legacy_counts_wrap_without_double_counting_stats2() {
        let mut data = qdisc(u32::MAX, 7);
        data.extend(qdisc(4, 9));
        let mut totals = (0, 0);
        assert!(!messages(&data, 1, &mut totals).unwrap());
        assert_eq!(totals, (3, 16));
        assert!(messages(&message(3, 2, &0i32.to_ne_bytes()), 1, &mut totals).unwrap());
        assert_eq!(totals, (3, 16));
    }
    #[test]
    fn interrupted_wrong_sequence_truncated_and_kernel_error_are_not_partial_success() {
        for malformed in [
            message(3, 0x10, &[]),
            message(2, 0, &(-13i32).to_ne_bytes()),
            vec![0; 15],
        ] {
            assert!(messages(&malformed, 1, &mut (0, 0)).is_err());
        }
        assert!(messages(&qdisc(1, 2), 2, &mut (0, 0)).is_err());
        let mut truncated = qdisc(1, 2);
        truncated.pop();
        assert!(messages(&truncated, 1, &mut (0, 0)).is_err());
    }
    #[test]
    #[cfg(target_os = "linux")]
    fn actual_kernel_qdisc_dump_closes_its_owned_socket() {
        sample().expect("actual NETLINK_ROUTE qdisc dump");
    }
}
