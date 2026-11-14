// Minimal reproducer for the en0 userspace-accept platform residual (SPEC-0077 C20).
//
// Proves the anomaly is BELOW livekit-rs and independent of bind_addresses / C20:
// a bare Rust std::net listener (no tokio, no axum, no livekit code) bound to the
// host's en0 IPv4 never accepts a connection destined to that IP on this macOS host,
// while the SAME socket options serve 127.0.0.1, and a simultaneous Python asyncio /
// Go en0 server serve en0 fine. Every socket option is exercised: plain, SO_REUSEADDR,
// SO_REUSEPORT, IPv4 wildcard (0.0.0.0) and dual-stack IPv6 ([::], V6ONLY=0).
//
// Build (offline, socket2 0.6.5 is in the workspace cargo cache):
//   cp reproducer.rs /tmp/en0-repro/src/main.rs   # with the Cargo.toml beside it
//   CARGO_NET_OFFLINE=true cargo build --release
// Run:
//   ./target/release/tokio-en0 "$(ipconfig getifaddr en0)"   # prints ports JSON, parks
// Probe each printed port from another shell:
//   curl --max-time 4 http://<en0>:<port>/        # en0 cases hang (000, rc 28)
//   curl --max-time 4 http://127.0.0.1:<v4wild>/  # loopback returns 200
// The ACCEPT lines on stderr show accept() never fires for the en0-destined flow.
//
// Observed on this host (en0=192.168.1.177, macOS 27.0.0, Clash utun0..4 present,
// `route get 192.168.1.177` -> interface lo0): see observed.json and README.md.

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::thread;

fn make(addr: SocketAddr, reuse_addr: bool, reuse_port: bool, v6only: Option<bool>) -> Socket {
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let s = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).unwrap();
    if reuse_addr { s.set_reuse_address(true).unwrap(); }
    if reuse_port { s.set_reuse_port(true).unwrap(); }
    if let Some(v) = v6only { s.set_only_v6(v).unwrap(); }
    s.bind(&SockAddr::from(addr)).unwrap();
    s.listen(128).unwrap();
    s
}

fn serve(name: &'static str, s: Socket) {
    let listener: std::net::TcpListener = s.into();
    eprintln!("[{name}] listening on {:?}", listener.local_addr().unwrap());
    for stream in listener.incoming() {
        match stream {
            Ok(mut st) => {
                eprintln!("[{name}] ACCEPT peer={:?}", st.peer_addr());
                thread::spawn(move || {
                    use std::io::{Read, Write};
                    let mut b = [0u8; 512];
                    let _ = st.read(&mut b);
                    let _ = st.write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nOK");
                    let _ = st.flush();
                });
            }
            Err(e) => eprintln!("[{name}] ERR {e}"),
        }
    }
}

fn main() {
    let en0: Ipv4Addr = std::env::args().nth(1).unwrap().parse().unwrap();
    let mut ports = Vec::new();
    let cases: Vec<(&'static str, SocketAddr, bool, bool, Option<bool>)> = vec![
        ("en0_plain", SocketAddr::new(en0.into(), 0), false, false, None),
        ("en0_reuseaddr", SocketAddr::new(en0.into(), 0), true, false, None),
        ("en0_reuseport", SocketAddr::new(en0.into(), 0), false, true, None),
        ("v4wild_reuseaddr", SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0), true, false, None),
        ("v6dual_reuseaddr", SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0), true, false, Some(false)),
    ];
    for (name, addr, ra, rp, v6) in cases {
        let s = make(addr, ra, rp, v6);
        ports.push(format!("\"{}\":{}", name, s.local_addr().unwrap().as_socket().unwrap().port()));
        thread::spawn(move || serve(name, s));
    }
    println!("{{{}}}", ports.join(","));
    loop { thread::park(); }
}