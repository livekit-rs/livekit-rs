use super::*;
fn stun(user: &str) -> Vec<u8> {
    let padded = (user.len() + 3) & !3;
    let mut b = vec![0; 24 + padded];
    b[..2].copy_from_slice(&1u16.to_be_bytes());
    b[2..4].copy_from_slice(&((4 + padded) as u16).to_be_bytes());
    b[4..8].copy_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
    b[8..20].copy_from_slice(&[7; 12]);
    b[20..22].copy_from_slice(&6u16.to_be_bytes());
    b[22..24].copy_from_slice(&(user.len() as u16).to_be_bytes());
    b[24..24 + user.len()].copy_from_slice(user.as_bytes());
    b
}
#[cfg(target_os = "linux")]
async fn interleaved(two_owners: bool) {
    let a: IpAddr = "127.0.0.2".parse().unwrap();
    let b: IpAddr = "127.0.0.3".parse().unwrap();
    let alias: IpAddr = "127.0.0.1".parse().unwrap();
    let reserved = std::net::UdpSocket::bind((a, 0)).unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(port),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![
            LocalInterface {
                bind: a,
                advertise: alias,
            },
            LocalInterface {
                bind: b,
                advertise: b,
            },
        ],
    })
    .await
    .unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let one = TransportId::next();
    let candidates = net.open_transport(one, "owner-a", tx.clone()).unwrap();
    assert_eq!(candidates.len(), 2);
    assert_ne!(candidates[0].addr, candidates[1].addr);
    assert_ne!(candidates[0].base, candidates[1].base);
    let two = if two_owners {
        let id = TransportId::next();
        net.open_transport(id, "owner-b", tx).unwrap();
        id
    } else {
        one
    };
    let peer = UdpSocket::bind((alias, 0)).await.unwrap();
    peer.send_to(&stun("owner-a:peer"), (a, port))
        .await
        .unwrap();
    peer.send_to(
        &stun(if two_owners {
            "owner-b:peer"
        } else {
            "owner-a:peer"
        }),
        (b, port),
    )
    .await
    .unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let packets = [first, second];
    let pa = packets.iter().find(|p| p.bound.ip() == a).unwrap();
    let pb = packets.iter().find(|p| p.bound.ip() == b).unwrap();
    assert_eq!(pa.transport, one);
    assert_eq!(pb.transport, two);
    assert_eq!(pa.destination, SocketAddr::new(alias, port));
    assert_eq!(pb.destination, SocketAddr::new(b, port));
    // Both STUN paths have really been delivered before replying to A. A last
    // remote/alias cache cannot satisfy these original physical-source assertions.
    let mut buffer = [0u8; 64];
    for (packet, body, ip) in [
        (pa, b"reply-a".as_slice(), a),
        (pb, b"reply-b".as_slice(), b),
    ] {
        net.sender().for_transport(packet.transport).unwrap().send(
            NetProto::Udp,
            packet.bound,
            packet.source,
            body,
        );
        let (n, from) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            peer.recv_from(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buffer[..n], body);
        assert_eq!(from, SocketAddr::new(ip, port));
    }
    peer.send_to(b"non-stun-on-a", (a, port)).await.unwrap();
    let actual = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actual.transport, one,
        "another owner on same advertised/remote tuple must not steal physical A path"
    );
    assert_eq!(actual.bound, SocketAddr::new(a, port));
    assert_eq!(actual.data, b"non-stun-on-a");
    net.close_transport(one);
    if two_owners {
        net.close_transport(two);
    }
    assert!(read(&net.inner.by_remote).is_empty());
    net.shutdown();
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn same_peer_two_admitted_sockets_delayed_first_reply_retains_actual_source() {
    interleaved(false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn same_peer_two_owners_keep_distinct_admitted_routes_and_late_reply_sources() {
    interleaved(true).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn dedicated_alias_bases_are_distinct_and_release_both_real_ports() {
    let a: IpAddr = "127.0.0.2".parse().unwrap();
    let b: IpAddr = "127.0.0.3".parse().unwrap();
    let alias: IpAddr = "127.0.0.1".parse().unwrap();
    let reserve = std::net::UdpSocket::bind((a, 0)).unwrap();
    let port = reserve.local_addr().unwrap().port();
    drop(reserve);
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: None,
        port_range: (port, port),
        tcp_port: None,
        interfaces: vec![
            LocalInterface {
                bind: a,
                advertise: alias,
            },
            LocalInterface {
                bind: b,
                advertise: alias,
            },
        ],
    })
    .await
    .unwrap();
    let (tx, _) = mpsc::channel(1);
    let id = TransportId::next();
    let c = net.open_transport(id, "dedicated", tx).unwrap();
    assert_eq!(c[0].addr, c[1].addr);
    assert_ne!(c[0].base, c[1].base);
    assert_eq!(read(&net.inner.udp_sockets).len(), 2);
    net.close_transport(id);
    assert!(read(&net.inner.udp_sockets).is_empty());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Ok(first) = std::net::UdpSocket::bind((a, port)) {
                if let Ok(second) = std::net::UdpSocket::bind((b, port)) {
                    drop((first, second));
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    net.shutdown();
}
#[tokio::test]
async fn retired_transport_cannot_leave_an_old_route_after_explicit_id_reuse() {
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: "127.0.0.1".parse().unwrap(),
            advertise: "127.0.0.1".parse().unwrap(),
        }],
    })
    .await
    .unwrap();
    let (tx, _) = mpsc::channel(1);
    let id = TransportId::next();
    let local = net.open_transport(id, "old", tx).unwrap()[0].base;
    let remote = "127.0.0.1:19001".parse().unwrap();
    assert!(
        net.route(NetProto::Udp, local, remote, &stun("old:peer"))
            .is_some()
    );
    net.close_transport(id);
    assert!(read(&net.inner.by_remote).is_empty());
    let (tx, _) = mpsc::channel(1);
    net.open_transport(id, "new", tx).unwrap();
    assert!(
        net.route(NetProto::Udp, local, remote, &stun("old:peer"))
            .is_none()
    );
    let route = net
        .route(NetProto::Udp, local, remote, &stun("new:peer"))
        .unwrap();
    assert_eq!(route.transport, id);
    net.close_transport(id);
    net.shutdown();
    assert!(
        net.route(NetProto::Udp, local, remote, &stun("new:peer"))
            .is_none()
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn rewritten_shared_udp_duplicate_has_no_transport_ufrag_on_second_base() {
    let a: IpAddr = "127.0.0.2".parse().unwrap();
    let b: IpAddr = "127.0.0.3".parse().unwrap();
    let alias: IpAddr = "127.0.0.1".parse().unwrap();
    let reserved = std::net::UdpSocket::bind((a, 0)).unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(port),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![
            LocalInterface {
                bind: a,
                advertise: alias,
            },
            LocalInterface {
                bind: b,
                advertise: alias,
            },
        ],
    })
    .await
    .unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let id = TransportId::next();
    let c = net.open_transport(id, "dedup", tx).unwrap();
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].base, SocketAddr::new(a, port));
    assert_eq!(
        read(&net.inner.udp_sockets).len(),
        2,
        "all real muxes remain owned"
    );
    let remote = "127.0.0.1:19000".parse().unwrap();
    assert!(
        net.route(
            NetProto::Udp,
            SocketAddr::new(b, port),
            remote,
            &stun("dedup:peer")
        )
        .is_none()
    );
    let peer = UdpSocket::bind((alias, 0)).await.unwrap();
    peer.send_to(&stun("dedup:peer"), (a, port)).await.unwrap();
    let packet = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(packet.bound, SocketAddr::new(a, port));
    assert!(
        read(&net.inner.by_remote)
            .keys()
            .all(|(_, local, _)| local.ip() == a)
    );
    net.close_transport(id);
    net.shutdown();
}

#[test]
fn tcp_wildcard_selects_actual_family_bases_without_inferring_nat_alias() {
    let actual = [
        "10.0.0.9".parse().unwrap(),
        "127.0.0.1".parse().unwrap(),
        "::1".parse().unwrap(),
    ];
    assert_eq!(
        tcp_interface_bases("0.0.0.0".parse().unwrap(), &actual),
        actual[..2]
    );
    assert_eq!(
        tcp_interface_bases("::".parse().unwrap(), &actual),
        actual[2..]
    );
    assert!(tcp_interface_bases("0.0.0.0".parse().unwrap(), &[]).is_empty());
    let concrete = "10.2.0.1".parse().unwrap();
    assert_eq!(tcp_interface_bases(concrete, &[]), vec![concrete]);
}

#[tokio::test]
async fn wildcard_tcp_listener_routes_real_frames_and_replies_on_concrete_base() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: Some(0),
        interfaces: vec![LocalInterface {
            bind: "0.0.0.0".parse().unwrap(),
            advertise: "198.51.100.8".parse().unwrap(),
        }],
    })
    .await
    .unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let id = TransportId::next();
    let candidates = net.open_transport(id, "tcp-wildcard", tx).unwrap();
    assert_eq!(
        candidates.len(),
        1,
        "equal public TCP candidates retain the first selected concrete base"
    );
    let c = candidates[0];
    assert_eq!(c.proto, NetProto::Tcp);
    assert!(!c.base.ip().is_unspecified());
    assert_ne!(
        c.base.ip(),
        c.addr.ip(),
        "external alias is not invented as a local base"
    );
    let mut peer = tokio::net::TcpStream::connect(c.base).await.unwrap();
    let body = stun("tcp-wildcard:peer");
    peer.write_all(&(body.len() as u16).to_be_bytes())
        .await
        .unwrap();
    peer.write_all(&body).await.unwrap();
    let packet = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(packet.bound, c.base);
    assert_eq!(packet.destination, c.addr);
    assert_eq!(packet.source, peer.local_addr().unwrap());
    assert_eq!(packet.data, body);
    net.sender().send(
        NetProto::Tcp,
        packet.bound,
        packet.source,
        b"actual-tcp-reply",
    );
    let mut len = [0; 2];
    tokio::time::timeout(std::time::Duration::from_secs(2), peer.read_exact(&mut len))
        .await
        .unwrap()
        .unwrap();
    let mut reply = vec![0; u16::from_be_bytes(len) as usize];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        peer.read_exact(&mut reply),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(reply, b"actual-tcp-reply");
    net.close_transport(id);
    net.shutdown();
}

// Source MultiPortsUDPMux balances one port per physical IP per gather.
// These sockets require only an existing loopback, so Darwin and Linux both
// run the same real transport source-identity assertions without aliases.
#[tokio::test]
async fn configured_two_udp_ports_select_source_round_robin_and_real_reply_port() {
    let loopback: IpAddr = "127.0.0.1".parse().unwrap();
    let one = std::net::UdpSocket::bind((loopback, 0)).unwrap();
    let two = std::net::UdpSocket::bind((loopback, 0)).unwrap();
    let ports = [
        one.local_addr().unwrap().port(),
        two.local_addr().unwrap().port(),
    ];
    assert_ne!(ports[0], ports[1]);
    drop((one, two));
    let net = NetRouter::start_with_udp_ports(
        NetConfig {
            udp_enabled: true,
            udp_port: Some(ports[0]),
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: loopback,
                advertise: loopback,
            }],
        },
        &ports,
    )
    .await
    .unwrap();
    assert_eq!(read(&net.inner.udp_sockets).len(), 2);
    let peer = UdpSocket::bind((loopback, 0)).await.unwrap();
    for (i, index) in [1usize, 0, 1, 0].into_iter().enumerate() {
        let id = TransportId::next();
        let ufrag = format!("multi-port-{i}");
        let (tx, mut rx) = mpsc::channel(4);
        let candidates = net.open_transport(id, &ufrag, tx).unwrap();
        assert_eq!(
            candidates.len(),
            1,
            "source advertises one configured port per physical IP"
        );
        let c = candidates[0];
        assert_eq!(c.base, SocketAddr::new(loopback, ports[index]));
        assert_eq!(c.addr, c.base);
        let wrong = SocketAddr::new(loopback, ports[1 - index]);
        assert!(
            net.route(
                NetProto::Udp,
                wrong,
                peer.local_addr().unwrap(),
                &stun(&format!("{ufrag}:peer"))
            )
            .is_none(),
            "nonselected mux has no ufrag for this gather"
        );
        let body = stun(&format!("{ufrag}:peer"));
        peer.send_to(&body, c.base).await.unwrap();
        let packet = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.transport, id);
        assert_eq!(packet.bound, c.base);
        assert_eq!(packet.data, body);
        net.sender().for_transport(packet.transport).unwrap().send(
            NetProto::Udp,
            packet.bound,
            packet.source,
            b"actual-configured-port-reply",
        );
        let mut buf = [0u8; 64];
        let (n, source) =
            tokio::time::timeout(std::time::Duration::from_secs(2), peer.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buf[..n], b"actual-configured-port-reply");
        assert_eq!(source, c.base);
        net.close_transport(id);
    }
    net.shutdown();
}

#[tokio::test]
async fn second_configured_udp_bind_failure_releases_first_before_readers_start() {
    let reserve = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let first = reserve.local_addr().unwrap();
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let second = occupied.local_addr().unwrap();
    drop(reserve);
    let result = NetRouter::start_with_udp_ports(
        NetConfig {
            udp_enabled: true,
            udp_port: Some(first.port()),
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: first.ip(),
                advertise: first.ip(),
            }],
        },
        &[first.port(), second.port()],
    )
    .await;
    assert!(
        result.is_err(),
        "second genuine bind must fail, never silently truncate ports"
    );
    let rebound = std::net::UdpSocket::bind(first).unwrap();
    drop(rebound);
    drop(occupied);
}

#[tokio::test]
#[ignore = "requires existing UP non-loopback IPv4; Darwin/Linux physical-interface profile, no aliases"]
async fn existing_two_physical_interfaces_match_full_agent_rewritten_udp_dedup() {
    let a: IpAddr = "127.0.0.1".parse().unwrap();
    let mut actual = None;
    for device in getifs::interfaces().unwrap() {
        if !device.flags().contains(getifs::Flags::UP)
            || device.flags().contains(getifs::Flags::LOOPBACK)
        {
            continue;
        }
        let Ok(addresses) = device.addrs() else {
            continue;
        };
        for address in addresses {
            let ip = address.addr();
            if ip.is_ipv4() && !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast() {
                actual = Some(ip);
                break;
            }
        }
        if actual.is_some() {
            break;
        }
    }
    let b = actual.expect(
        "actual UP non-loopback IPv4 required; missing physical profile is failure, never PASS",
    );
    let reserved = std::net::UdpSocket::bind((a, 0)).unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(port),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![
            LocalInterface {
                bind: a,
                advertise: a,
            },
            LocalInterface {
                bind: b,
                advertise: a,
            },
        ],
    })
    .await
    .unwrap();
    let id = TransportId::next();
    let (tx, mut rx) = mpsc::channel(4);
    let candidates = net.open_transport(id, "existing-physical", tx).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].base, SocketAddr::new(a, port));
    assert_eq!(read(&net.inner.udp_sockets).len(), 2);
    assert!(
        std::net::UdpSocket::bind((b, port)).is_err(),
        "second real socket is owned even though Agent did not admit its ufrag"
    );
    let peer = UdpSocket::bind((a, 0)).await.unwrap();
    let body = stun("existing-physical:peer");
    assert!(
        net.route(
            NetProto::Udp,
            SocketAddr::new(b, port),
            peer.local_addr().unwrap(),
            &body
        )
        .is_none()
    );
    peer.send_to(&body, (a, port)).await.unwrap();
    let packet = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(packet.bound, SocketAddr::new(a, port));
    assert_eq!(packet.data, body);
    net.sender().send(
        NetProto::Udp,
        packet.bound,
        packet.source,
        b"actual-existing-physical-reply",
    );
    let mut buf = [0; 64];
    let (n, from) =
        tokio::time::timeout(std::time::Duration::from_secs(2), peer.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&buf[..n], b"actual-existing-physical-reply");
    assert_eq!(from, packet.bound);
    net.close_transport(id);
    net.shutdown();
}

#[tokio::test]
async fn standalone_stun_udp_port_is_advertised_and_routes_every_gather() {
    let loopback = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let cfg = NetConfig {
        udp_enabled: true,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: loopback,
            advertise: loopback,
        }],
    };
    // All three are real sockets on independent OS-assigned ports. The public
    // config selects 3478 for the third socket; ephemeral binding isolates this
    // transport test from other services using the fixed public port.
    let net = NetRouter::start_with_standalone_udp_port(cfg, &[0, 0], Some(0), false)
        .await
        .unwrap();
    assert_eq!(read(&net.inner.udp_sockets).len(), 3);
    let regular = &net.inner.udp_groups[0];
    assert_eq!(regular.len(), 2);
    let standalone = net.inner.shared_candidates[0];
    assert_eq!(standalone.proto, NetProto::Udp);
    assert!(
        regular
            .iter()
            .all(|candidate| candidate.base != standalone.base)
    );
    let peer = UdpSocket::bind((loopback, 0)).await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    for (gather, index) in [1usize, 0, 1, 0].into_iter().enumerate() {
        let id = TransportId::next();
        let ufrag = format!("standalone-gather-{gather}");
        let (tx, mut rx) = mpsc::channel(4);
        let candidates = net.open_transport(id, &ufrag, tx).unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].base, regular[index].base);
        assert_eq!(candidates[1].base, standalone.base);
        let body = stun(&format!("{ufrag}:peer"));
        assert!(
            net.route(NetProto::Udp, regular[1 - index].base, peer_addr, &body)
                .is_none(),
            "the unselected regular port still rejects this transport"
        );
        for candidate in candidates {
            peer.send_to(&body, candidate.base).await.unwrap();
            let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(packet.transport, id);
            assert_eq!(packet.bound, candidate.base);
            assert_eq!(packet.data, body);
            net.sender().for_transport(id).unwrap().send(
                NetProto::Udp,
                packet.bound,
                packet.source,
                b"mux-identity-reply",
            );
            let mut buf = [0u8; 64];
            let (n, from) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&buf[..n], b"mux-identity-reply");
            assert_eq!(from, candidate.base);
        }
        net.close_transport(id);
        assert!(
            net.route(NetProto::Udp, standalone.base, peer_addr, &body)
                .is_none()
        );
    }
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn mux_ice_restart_retires_old_routes_and_keeps_the_standalone_port() {
    let loopback = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let public = "203.0.113.9".parse().unwrap();
    let cfg = NetConfig {
        udp_enabled: true,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: loopback,
            advertise: public,
        }],
    };
    let net = NetRouter::start_with_standalone_udp_port(cfg, &[0, 0, 0], Some(0), true)
        .await
        .unwrap();
    let regular = &net.inner.udp_groups[0];
    let standalone = net.inner.shared_candidates[0].base;
    let id = TransportId::next();
    let (tx, mut rx) = mpsc::channel(4);
    let initial = net.open_transport(id, "first", tx).unwrap();
    let old = regular[1].base;
    assert_eq!(initial[0].base, old);
    let peer = UdpSocket::bind((loopback, 0)).await.unwrap();
    let remote = peer.local_addr().unwrap();
    let sender = net.sender().for_transport(id).unwrap();
    sender.send(NetProto::Udp, old, remote, b"first-generation");
    let mut buf = [0u8; 64];
    let (len, source) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((&buf[..len], source), (b"first-generation".as_slice(), old));
    let captured = net.route(NetProto::Udp, old, remote, b"learned").unwrap();

    let new = regular[2].base;
    let restarted = net.update_ufrag(id, "second", false);
    for base in [new, standalone] {
        assert!(restarted.iter().any(|c| c.base == base && c.addr == base));
        assert!(
            restarted
                .iter()
                .any(|c| c.base == base && c.addr == SocketAddr::new(public, base.port()))
        );
    }
    assert_eq!(restarted.len(), 4);
    assert!(!read(&net.inner.by_remote).contains_key(&(NetProto::Udp, old, remote)));
    assert!(
        net.route(NetProto::Udp, old, remote, &stun("second:peer"))
            .is_none()
    );
    net.deliver(&captured, NetProto::Udp, old, old, remote, vec![1]);
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    for base in [new, standalone] {
        peer.send_to(&stun("second:peer"), base).await.unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((packet.transport, packet.bound), (id, base));
    }
    assert!(net.update_ufrag(id, "second", false).is_empty());
    assert_eq!(
        net.update_ufrag(id, "third", false)[0].base,
        regular[0].base
    );
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn standalone_mux_duplicate_regular_port_fails_and_releases_startup_socket() {
    let loopback = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let probe = std::net::UdpSocket::bind((loopback, 0)).unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let cfg = NetConfig {
        udp_enabled: true,
        udp_port: Some(port),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: loopback,
            advertise: loopback,
        }],
    };
    let error = NetRouter::start_with_standalone_udp_port(cfg, &[port], Some(port), false)
        .await
        .err()
        .expect("original standalone UDP bind is separate");
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    std::net::UdpSocket::bind((loopback, port))
        .expect("failed startup releases the first mux socket");
}
