pub(crate) fn count() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(source_cpu_count)
}
#[cfg(target_os = "linux")]
fn source_cpu_count() -> usize {
    // Go runtime.getCPUCount: real raw sched_getaffinity, fixed 8192-byte
    // buffer, count only returned bytes, error/zero -> 1. No cgroup quota.
    let mut mask = [0u8; 8192];
    // SAFETY: the kernel writes at most mask.len() bytes into this live buffer.
    let n = unsafe {
        libc::syscall(
            libc::SYS_sched_getaffinity,
            0,
            mask.len(),
            mask.as_mut_ptr(),
        )
    };
    if n < 0 {
        return 1;
    }
    mask[..usize::try_from(n).expect("nonnegative affinity length")]
        .iter()
        .map(|v| v.count_ones() as usize)
        .sum::<usize>()
        .max(1)
}
#[cfg(target_os = "macos")]
fn source_cpu_count() -> usize {
    // Go runtime.getCPUCount uses CTL_HW/HW_NCPU, not available_parallelism.
    let mut mib = [libc::CTL_HW, libc::HW_NCPU];
    let mut value = 0u32;
    let mut size = std::mem::size_of_val(&value);
    // SAFETY: both old-value storage and its length are valid; no new value.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut value).cast(),
            &raw mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result >= 0 && (value as i32) > 0 {
        value as usize
    } else {
        1
    }
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn source_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
