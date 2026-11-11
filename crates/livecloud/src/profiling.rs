//! Actual CPU/allocation sampling for the official profiling flags.
//!
//! File and owner lifetime follow fixed main.go260–294. Samples describe this
//! Rust process, not Go runtime/GC internals. Heap sampling uses System through
//! the binary's GlobalAlloc wrapper; no RSS-to-heap or synthetic samples.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use flate2::Compression;
use flate2::write::GzEncoder;
use rpprof::protos::Message;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static CPU_ACTIVE: AtomicBool = AtomicBool::new(false);
static HEAP_USERS: AtomicUsize = AtomicUsize::new(0);
static HEAP_CONTROL: Mutex<()> = Mutex::new(());
static HEAP_HTTP: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
static SYMBOLS: Mutex<BTreeMap<u64, String>> = Mutex::new(BTreeMap::new());
static FUNCTION_ADDRESSES: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());
#[cfg(test)]
pub(crate) static TEST_SAMPLING: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn capture_active() -> bool {
    CPU_ACTIVE.load(Ordering::Acquire)
}

/// A live CPU/wall sampler shared by file and HTTP producers. The guard stays
/// owned by the request future, so cancellation cannot detach a collector.
pub(crate) struct Capture {
    guard: Option<rpprof::ProfilerGuard<'static>>,
}
impl Capture {
    pub(crate) fn start(clock: rpprof::ClockType) -> Result<Self, ProfileError> {
        if CPU_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ProfileError("cpu profiling already in use".into()));
        }
        match rpprof::ProfilerGuardBuilder::default()
            .frequency(100)
            .clock_type(clock)
            .build()
        {
            Ok(guard) => Ok(Self { guard: Some(guard) }),
            Err(error) => {
                CPU_ACTIVE.store(false, Ordering::Release);
                Err(ProfileError(format!("could not start sampler: {error}")))
            }
        }
    }
    pub(crate) fn report(&self) -> Result<rpprof::Report, ProfileError> {
        rpprof::alloc::without_tracking(|| self.report_inner())
    }
    fn report_inner(&self) -> Result<rpprof::Report, ProfileError> {
        let report = self
            .guard
            .as_ref()
            .expect("live capture")
            .report()
            .build()
            .map_err(|error| ProfileError(format!("could not build profile: {error}")))?;
        let mut symbols = SYMBOLS.lock().unwrap_or_else(|error| error.into_inner());
        let mut addresses = FUNCTION_ADDRESSES
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for frames in report.data.keys() {
            for frame in &frames.frames {
                for symbol in frame {
                    if let Some(address) = symbol.addr {
                        let name = symbol.name();
                        symbols.insert(address as u64, name.clone());
                        addresses.insert(name, address as u64);
                    }
                }
            }
        }
        Ok(report)
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        drop(self.guard.take());
        CPU_ACTIVE.store(false, Ordering::Release);
    }
}

/// Debug listeners and a CLI heap file share actual allocation sampling; no
/// request resets another owner's retained live-allocation history.
pub(crate) struct HeapSampling {
    _private: (),
}
impl HeapSampling {
    pub(crate) fn start() -> Self {
        let _control = HEAP_CONTROL
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if HEAP_USERS.fetch_add(1, Ordering::AcqRel) == 0 {
            rpprof::alloc::reset();
            rpprof::alloc::set_sample_rate(512 * 1024);
            rpprof::alloc::start();
        }
        Self { _private: () }
    }
    /// Native symbolization is blocking work. A cancelled HTTP waiter can
    /// detach only this bounded collector job; its lock/exclusion guards stay
    /// owned by the job and restore collection state when it finishes.
    pub(crate) async fn snapshot_async() -> Result<rpprof::protos::Profile, ProfileError> {
        let permit = HEAP_HTTP
            .acquire()
            .await
            .map_err(|_| ProfileError("heap collector closed".into()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            Self::snapshot()
        })
        .await
        .map_err(|error| ProfileError(format!("heap collector task failed: {error}")))?
    }
    pub(crate) fn snapshot() -> Result<rpprof::protos::Profile, ProfileError> {
        let _control = HEAP_CONTROL
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // The vendored collector snapshots raw addresses/counters under lock,
        // resolves each distinct PC once outside that lock, and excludes only
        // this collector thread. Other workers keep tracking deallocations.
        rpprof::alloc::without_tracking(|| {
            rpprof::alloc::heap_report()
                .and_then(|report| report.pprof())
                .map_err(|error| ProfileError(format!("could not build heap profile: {error}")))
        })
    }
}
impl Drop for HeapSampling {
    fn drop(&mut self) {
        let _control = HEAP_CONTROL
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if HEAP_USERS.fetch_sub(1, Ordering::AcqRel) == 1 {
            rpprof::alloc::stop();
        }
    }
}

pub(crate) fn gzip_profile(profile: rpprof::protos::Profile) -> Result<Vec<u8>, ProfileError> {
    rpprof::alloc::without_tracking(|| gzip_profile_inner(profile))
}
fn gzip_profile_inner(mut profile: rpprof::protos::Profile) -> Result<Vec<u8>, ProfileError> {
    // rpprof's standard exporter emits symbolized Location lines but leaves
    // addresses at zero. Attach ONLY addresses captured from those same native
    // stack symbols, so /symbol can resolve profile locations without inventing
    // program counters or deriving them from location IDs.
    let addresses = FUNCTION_ADDRESSES
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let functions = profile
        .function
        .iter()
        .map(|function| (function.id, function))
        .collect::<std::collections::HashMap<_, _>>();
    for location in &mut profile.location {
        if location.address != 0 {
            continue;
        }
        for line in &location.line {
            if let Some(function) = functions.get(&line.function_id)
                && let Some(name) = profile.string_table.get(function.name as usize)
                && let Some(address) = addresses.get(name)
            {
                location.address = *address;
                break;
            }
        }
    }
    drop(addresses);
    let index = profile.string_table.len() as i64;
    profile
        .string_table
        .push("runtime=rust; collector=rpprof; native stack/allocation samples".into());
    profile.comment.push(index);
    let mut output = GzEncoder::new(Vec::new(), Compression::default());
    output
        .write_all(&profile.encode_to_vec())
        .and_then(|()| output.finish())
        .map_err(|error| ProfileError(format!("could not encode profile: {error}")))
}
pub(crate) fn symbol_names(addresses: &[u64]) -> Vec<(u64, String)> {
    let symbols = SYMBOLS.lock().unwrap_or_else(|error| error.into_inner());
    addresses
        .iter()
        .filter_map(|address| symbols.get(address).map(|name| (*address, name.clone())))
        .collect()
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProfileError(String);

fn create(path: &Path) -> Result<File, ProfileError> {
    // Fixed Go os.Create is O_RDWR|O_CREATE|O_TRUNC (os/file.go399–400),
    // including existing write-only files and FIFOs, not Rust write-only create.
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|error| {
            // os.PathError uses the actual operation/path and OS cause, without an
            // OS-code suffix. Do not serialize a DSN or arbitrary configuration.
            let cause = match error.kind() {
                io::ErrorKind::NotFound => "no such file or directory".into(),
                io::ErrorKind::PermissionDenied => "permission denied".into(),
                io::ErrorKind::IsADirectory => "is a directory".into(),
                io::ErrorKind::NotADirectory => "not a directory".into(),
                _ => error.to_string(),
            };
            ProfileError(format!("open {}: {cause}", path.display()))
        })
}

fn write_profile(file: &mut File, profile: rpprof::protos::Profile) -> Result<(), ProfileError> {
    let mut gzip = GzEncoder::new(file, Compression::default());
    gzip.write_all(&profile.encode_to_vec())
        .and_then(|()| gzip.finish().map(|_| ()))
        .map_err(|error| ProfileError(format!("could not write profile: {error}")))
}

/// One process-local owner; inactive construction allocates no profiler/file.
/// Flushes heap then CPU, matching the upstream LIFO deferred cleanup order.
pub(crate) struct Profiles {
    cpu: Option<(File, Capture)>,
    heap: Option<(File, HeapSampling)>,
    owns: bool,
}
impl Profiles {
    pub(crate) fn start(cpu: Option<&Path>, heap: Option<&Path>) -> Result<Self, ProfileError> {
        let mut owner = Self {
            cpu: None,
            heap: None,
            owns: false,
        };
        if cpu.is_none() && heap.is_none() {
            return Ok(owner);
        }
        if ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ProfileError("profiling already in use".into()));
        }
        owner.owns = true;
        if let Some(path) = cpu {
            let file = create(path)?;
            // Go runtime's default CPU profiling frequency is100Hz. Explicit
            // CPU time (not wall-clock) keeps idle waits out of CPU samples.
            let guard = Capture::start(rpprof::ClockType::Cpu)?;
            owner.cpu = Some((file, guard));
        }
        if let Some(path) = heap {
            let file = create(path)?;
            owner.heap = Some((file, HeapSampling::start()));
        }
        Ok(owner)
    }

    fn finish(&mut self) -> Result<(), ProfileError> {
        // Stop before snapshot allocation/symbolization, preventing profiler
        // implementation allocations from recursively entering heap tracking.
        let mut failures = Vec::new();
        if let Some((mut file, sampling)) = self.heap.take() {
            let result =
                HeapSampling::snapshot().and_then(|profile| write_profile(&mut file, profile));
            drop(sampling);
            if let Err(error) = result {
                failures.push(error.to_string());
            }
        }
        if let Some((mut file, guard)) = self.cpu.take() {
            let result = guard
                .report()
                .and_then(|report| {
                    report.pprof().map_err(|error| {
                        ProfileError(format!("could not build CPU profile: {error}"))
                    })
                })
                .and_then(|profile| write_profile(&mut file, profile));
            drop(guard);
            if let Err(error) = result {
                failures.push(error.to_string());
            }
        }
        if self.owns {
            ACTIVE.store(false, Ordering::Release);
            self.owns = false;
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ProfileError(failures.join("; ")))
        }
    }
}
impl Drop for Profiles {
    fn drop(&mut self) {
        // Go's deferred heap writer ignores write errors; keep server result
        // semantics while exposing a real write failure to the logger.
        if let Err(error) = self.finish() {
            tracing::error!(%error, "profile finalization failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::time::{Duration, Instant};

    fn decoded(path: &Path) -> rpprof::protos::Profile {
        let mut bytes = Vec::new();
        flate2::read::GzDecoder::new(File::open(path).unwrap())
            .read_to_end(&mut bytes)
            .unwrap();
        rpprof::protos::Profile::decode(bytes.as_slice()).unwrap()
    }
    #[test]
    fn profile_create_is_truncating_and_read_write_like_go_os_create() {
        use std::io::{Seek, SeekFrom};
        let dir = std::env::temp_dir().join(format!("livecloud-profile-rw-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("source-create.pprof");
        std::fs::write(&path, b"previous-owned-content").unwrap();
        let mut file = create(&path).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0, "source O_TRUNC remains");
        file.write_all(b"real-file-read-write").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(
            actual, b"real-file-read-write",
            "source os.Create returns an O_RDWR handle"
        );
        drop(file);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn actual_cpu_and_live_allocations_are_sampled_and_early_failure_releases_owner() {
        let _serial = TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = std::env::temp_dir().join(format!("livecloud-profile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cpu = dir.join("cpu.pprof");
        let heap = dir.join("heap.pprof");
        let missing = dir.join("does-not-exist/heap.pprof");
        assert!(Profiles::start(None, None).is_ok());
        assert!(Profiles::start(Some(&cpu), Some(&missing)).is_err());
        assert!(!ACTIVE.load(Ordering::Acquire));
        assert!(
            cpu.metadata().unwrap().len() > 0,
            "earlier CPU owner must flush on later heap-open failure"
        );
        let early = decoded(&cpu);
        assert!(
            early
                .sample_type
                .iter()
                .any(|kind| early.string_table[kind.ty as usize] == "cpu")
        );
        let unused = dir.join("must-not-create-heap.pprof");
        assert!(Profiles::start(Some(&missing), Some(&unused)).is_err());
        assert!(
            !unused.exists(),
            "CPU creation is source-ordered before heap creation"
        );
        assert!(!ACTIVE.load(Ordering::Acquire));
        let mut owner = Profiles::start(Some(&cpu), Some(&heap)).unwrap();
        assert!(Profiles::start(Some(&cpu), None).is_err());
        // Real retained allocation workload; no tests insert invented samples.
        let allocations: Vec<_> = (0..128).map(|n| vec![n as u8; 128 * 1024]).collect();
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut checksum = 1u64;
        while Instant::now() < deadline {
            for n in 1..10000u64 {
                checksum = std::hint::black_box(
                    checksum.wrapping_mul(6364136223846793005).wrapping_add(n),
                );
            }
        }
        std::hint::black_box((&allocations, checksum));
        owner.finish().unwrap();
        assert!(!rpprof::alloc::is_active());
        let cpu = decoded(&cpu);
        let heap = decoded(&heap);
        assert!(
            cpu.sample
                .iter()
                .any(|s| !s.location_id.is_empty() && s.value.iter().any(|v| *v > 0)),
            "real CPU stack samples"
        );
        let types: Vec<_> = heap
            .sample_type
            .iter()
            .map(|t| heap.string_table[t.ty as usize].as_str())
            .collect();
        assert_eq!(
            types,
            [
                "alloc_objects",
                "alloc_space",
                "inuse_objects",
                "inuse_space"
            ]
        );
        assert!(
            heap.sample.iter().any(|s| s.value[1] > 0 && s.value[3] > 0),
            "real retained heap samples"
        );
        drop(allocations);
        drop(owner);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
