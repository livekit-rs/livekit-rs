//! Isolated PERF-CRYPTO-01 comparison. Never link this binary into production.
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde_json::{Value, json};
use str0m_proto::crypto::{
    AeadAes128Gcm, AeadAes128GcmCipher, Aes128CmSha1_80, Aes128CmSha1_80Cipher, CryptoProvider,
};

struct CountedAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for CountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: GlobalAlloc forwards the caller's valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr/layout are exactly those allocated through System above.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: GlobalAlloc's caller supplies the live allocation and valid size.
        unsafe { System.realloc(ptr, old, size) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: as for alloc; System owns this allocation.
        unsafe { System.alloc_zeroed(layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

const HEADER: usize = 12;
const BATCH: usize = 1024;
const SSRC: u32 = 0x1234_5678;

enum Cipher {
    Gcm(Box<dyn AeadAes128GcmCipher>),
    Cm(Box<dyn Aes128CmSha1_80Cipher>),
}

/// Same RFC 3711/7714 packet envelope and provider traits for both candidates.
/// SRTP session keys are supplied directly; key derivation, replay window, packet
/// parsing, transport, SCTP and DTLS are deliberately outside the measured scope.
struct PacketCipher {
    cipher: Cipher,
    crypto: CryptoProvider,
    salt: [u8; 14],
    auth_key: [u8; 20],
}
impl PacketCipher {
    fn new(crypto: CryptoProvider, gcm: bool, sample: u8, domain: u64) -> Self {
        let mut key = [sample; 16];
        key[..8].copy_from_slice(&domain.to_be_bytes());
        key[8] = u8::from(gcm);
        let cipher = if gcm {
            Cipher::Gcm(
                crypto
                    .srtp_provider
                    .aead_aes_128_gcm()
                    .create_cipher(key, true),
            )
        } else {
            Cipher::Cm(
                crypto
                    .srtp_provider
                    .aes_128_cm_sha1_80()
                    .create_cipher(key, true),
            )
        };
        Self {
            cipher,
            crypto,
            salt: [sample.wrapping_add(31); 14],
            auth_key: [sample.wrapping_add(17); 20],
        }
    }
    fn tag_len(&self) -> usize {
        if matches!(self.cipher, Cipher::Gcm(_)) {
            16
        } else {
            10
        }
    }
    fn encrypt(&mut self, index: u64, payload: &[u8], output: &mut [u8]) {
        output[..HEADER].copy_from_slice(&header(index));
        match &mut self.cipher {
            Cipher::Gcm(c) => {
                let iv = AeadAes128Gcm::rtp_iv(
                    self.salt[..12].try_into().unwrap(),
                    SSRC,
                    (index >> 16) as u32,
                    index as u16,
                );
                let (aad, ciphertext) = output.split_at_mut(HEADER);
                c.encrypt(&iv, aad, payload, ciphertext).unwrap();
            }
            Cipher::Cm(c) => {
                let iv = Aes128CmSha1_80::rtp_iv(self.salt, SSRC, index);
                c.encrypt(&iv, payload, &mut output[HEADER..HEADER + payload.len()])
                    .unwrap();
                Aes128CmSha1_80::rtp_hmac(
                    |key, parts| self.crypto.sha1_hmac_provider.sha1_hmac(key, parts),
                    &self.auth_key,
                    output,
                    index,
                    HEADER + payload.len(),
                );
            }
        }
    }
    fn decrypt(&mut self, index: u64, packet: &[u8], clear: &mut [u8]) -> bool {
        match &mut self.cipher {
            Cipher::Gcm(c) => {
                let iv = AeadAes128Gcm::rtp_iv(
                    self.salt[..12].try_into().unwrap(),
                    SSRC,
                    (index >> 16) as u32,
                    index as u16,
                );
                c.decrypt(&iv, &[&packet[..HEADER]], &packet[HEADER..], clear)
                    .is_ok()
            }
            Cipher::Cm(c) => {
                let tag_at = packet.len() - Aes128CmSha1_80::HMAC_TAG_LEN;
                if !Aes128CmSha1_80::rtp_verify(
                    |key, parts| self.crypto.sha1_hmac_provider.sha1_hmac(key, parts),
                    &self.auth_key,
                    &packet[..tag_at],
                    index,
                    &packet[tag_at..],
                ) {
                    return false;
                }
                let iv = Aes128CmSha1_80::rtp_iv(self.salt, SSRC, index);
                c.decrypt(&iv, &packet[HEADER..tag_at], clear).is_ok()
            }
        }
    }
}
fn header(index: u64) -> [u8; HEADER] {
    let mut h = [0; HEADER];
    h[0] = 0x80;
    h[1] = 111;
    h[2..4].copy_from_slice(&(index as u16).to_be_bytes());
    h[4..8].copy_from_slice(&(index.wrapping_mul(960) as u32).to_be_bytes());
    h[8..12].copy_from_slice(&SSRC.to_be_bytes());
    h
}
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided correctly sized rusage on success.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage failed");
    // SAFETY: successful getrusage initialized every field we read.
    let usage = unsafe { usage.assume_init() };
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1e6
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1e6
}
fn provider(name: &str) -> CryptoProvider {
    match name {
        "rustcrypto" => rtc_crypto::provider(),
        "aws-lc" => str0m_aws_lc_rs::default_provider(),
        _ => panic!("unknown provider"),
    }
}
fn correctness() {
    for gcm in [false, true] {
        for index in [0, 65535, 65536, 65537, 1_000_000] {
            let mut rust = PacketCipher::new(provider("rustcrypto"), gcm, 42, 0);
            let mut aws = PacketCipher::new(provider("aws-lc"), gcm, 42, 0);
            let plain = vec![0xa5; 160];
            let mut a = vec![0; HEADER + plain.len() + rust.tag_len()];
            let mut b = a.clone();
            rust.encrypt(index, &plain, &mut a);
            aws.encrypt(index, &plain, &mut b);
            assert_eq!(a, b, "provider wire output differs at index {index}");
            let mut recovered = vec![0; plain.len()];
            assert!(aws.decrypt(index, &a, &mut recovered));
            assert_eq!(recovered, plain);
            assert!(rust.decrypt(index, &b, &mut recovered));
            assert_eq!(recovered, plain);
            a[HEADER] ^= 1;
            assert!(!aws.decrypt(index, &a, &mut recovered));
            assert!(!rust.decrypt(index, &a, &mut recovered));
        }
    }
}

fn sample(
    name: &str,
    gcm: bool,
    decrypt: bool,
    payload_len: usize,
    packets: u64,
    round: u8,
) -> Value {
    let mut cipher = PacketCipher::new(
        provider(name),
        gcm,
        round + if decrypt { 64 } else { 1 },
        payload_len as u64 | (u64::from(decrypt) << 32),
    );
    let packet_len = HEADER + payload_len + cipher.tag_len();
    let mut payload = vec![0x5a; payload_len];
    let mut clear = vec![0; payload_len];
    let mut encoded = vec![0; packet_len * BATCH];
    let mut cpu = 0.0;
    let mut wall = 0.0;
    let mut allocations = 0;
    let mut latencies = vec![];
    let mut checksum = 0u64;
    for first in (0..packets).step_by(BATCH) {
        let count = (packets - first).min(BATCH as u64) as usize;
        if decrypt {
            // Ciphertext fixture creation is excluded from all measurement windows.
            for i in 0..count {
                let index = first + i as u64;
                payload[..8].copy_from_slice(&index.to_be_bytes());
                cipher.encrypt(
                    index,
                    &payload,
                    &mut encoded[i * packet_len..(i + 1) * packet_len],
                );
            }
        }
        let cpu_start = cpu_seconds();
        let start = Instant::now();
        let alloc_start = ALLOCATIONS.load(Ordering::Relaxed);
        for i in 0..count {
            let index = first + i as u64;
            let packet = &mut encoded[i * packet_len..(i + 1) * packet_len];
            if decrypt {
                assert!(cipher.decrypt(index, black_box(packet), &mut clear));
                checksum = checksum.wrapping_add(black_box(clear[0]) as u64);
            } else {
                payload[..8].copy_from_slice(&index.to_be_bytes());
                cipher.encrypt(index, black_box(&payload), packet);
                checksum = checksum.wrapping_add(black_box(packet[HEADER]) as u64);
            }
        }
        allocations += ALLOCATIONS.load(Ordering::Relaxed) - alloc_start;
        let elapsed = start.elapsed().as_secs_f64();
        cpu += cpu_seconds() - cpu_start;
        wall += elapsed;
        latencies.push(elapsed);
    }
    black_box(checksum);
    latencies.sort_by(f64::total_cmp);
    let quantile = |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize] * 1e3;
    json!({"provider": name, "suite": if gcm {"AEAD_AES_128_GCM"} else {"AES128_CM_SHA1_80"},
        "operation": if decrypt {"decrypt"} else {"encrypt"}, "round": round, "packets": packets,
        "payload_bytes": payload_len, "rtp_header_bytes": HEADER, "extension_bytes": 0, "tag_bytes": cipher.tag_len(), "wire_packet_bytes": packet_len,
        "cpu_seconds": cpu, "cpu_seconds_per_million": cpu * 1e6 / packets as f64,
        "measured_wall_seconds": wall, "packets_per_second": packets as f64 / wall,
        "effective_payload_gbps": packets as f64 * payload_len as f64 * 8.0 / wall / 1e9,
        "allocation_calls": allocations, "allocation_calls_per_packet": allocations as f64 / packets as f64,
        "batch_size": BATCH, "batch_count": latencies.len(), "batch_latency_ms_p50": quantile(0.5),
        "batch_latency_ms_p95": quantile(0.95), "batch_latency_ms_p99": quantile(0.99), "checksum": checksum,
        "cycles_per_packet": null})
}
fn option(key: &str, default: &str) -> String {
    let args: Vec<_> = std::env::args().collect();
    args.iter()
        .position(|s| s == key)
        .map(|i| args.get(i + 1).expect("missing argument").clone())
        .unwrap_or_else(|| default.into())
}
fn command(cmd: &str, args: &[&str]) -> String {
    std::process::Command::new(cmd)
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().into())
        .unwrap_or_default()
}
fn main() {
    correctness();
    if std::env::args().any(|a| a == "--self-test") {
        println!("cross-provider ciphertext, decryption, rollover and tampering checks PASS");
        return;
    }
    let packets: u64 = option("--packets", "1000000").parse().unwrap();
    let rounds: u8 = option("--rounds", "5").parse().unwrap();
    let smoke = std::env::args().any(|a| a == "--smoke");
    assert!(
        smoke || (packets >= 1_000_000 && rounds >= 5),
        "evidence runs require >=1M packets and >=5 rounds; use --smoke only for development"
    );
    let output = option("--output", "results/latest.json");
    let platform = json!({"os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "uname": command("uname", &["-a"]), "cpu": if cfg!(target_os="macos") {command("sysctl", &["-n", "machdep.cpu.brand_string"])} else {command("lscpu", &[])},
        "rustc": command("rustc", &["-Vv"]), "rustflags": std::env::var("RUSTFLAGS").unwrap_or_default(),
        "target_cpu": "Cargo profile defaults unless overridden by recorded build environment",
        "affinity": "NOT_SET", "frequency_governor": "NOT_CONTROLLED", "hardware_acceleration": "provider runtime detection; exact dispatch NOT_INSPECTED"});
    let mut results = vec![];
    for name in ["rustcrypto", "aws-lc"] {
        for gcm in [false, true] {
            for size in [160, 1200] {
                for decrypt in [false, true] {
                    let _ = sample(name, gcm, decrypt, size, 10_000, 0);
                    for round in 1..=rounds {
                        let result = sample(name, gcm, decrypt, size, packets, round);
                        eprintln!(
                            "{name} {} {} {size}B round {round}: {:.4}s CPU/1M",
                            if gcm { "GCM" } else { "CM" },
                            if decrypt { "decrypt" } else { "encrypt" },
                            result["cpu_seconds_per_million"].as_f64().unwrap()
                        );
                        results.push(result);
                    }
                }
            }
        }
    }
    let report = json!({"benchmark": "PERF-CRYPTO-01", "scope": "identical RFC RTP-protection envelope + provider trait comparison; not complete str0m SrtpContext or SFU",
        "status": if smoke {"SMOKE_ONLY"} else {"LOCAL_EVIDENCE"}, "platform": platform, "packets_per_round": packets, "rounds": rounds,
        "warmup_packets_per_case": 10000, "release": {"lto":"thin", "codegen_units":1},
        "correctness": "PASS: cross-provider equality, decrypt, ROC/sequence wrap, tampering",
        "allocation_measurement": "Rust global allocator calls only; aws-lc C-internal malloc calls are NOT_COUNTED; relaxed atomic overhead is present in both candidates",
        "cpu_measurement": "getrusage(RUSAGE_SELF) user+system CPU; sum of measured 1024-packet batches; fixture encryption excluded from decrypt windows",
        "not_run": ["Linux x86_64", "Linux aarch64", "SRTCP", "fan-out 1/4/16", "SFU capacity", "end-to-end queue latency", "replay window", "cycles/packet"], "samples": results});
    std::fs::write(&output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    eprintln!("saved {output}");
}
