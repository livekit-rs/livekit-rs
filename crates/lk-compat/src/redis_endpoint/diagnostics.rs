use std::io::Write;

/// Emit a go-redis dependency diagnostic with its local wall-clock stamp.
pub fn write_redis_diagnostic(caller: &str, message: &[u8]) {
    let mut stderr = std::io::stderr().lock();
    let _ = write!(stderr, "redis: {} {caller}: ", local_stamp());
    let _ = stderr.write_all(message);
    let _ = stderr.write_all(b"\n");
}

fn local_stamp() -> String {
    #[cfg(unix)]
    unsafe {
        let now = libc::time(std::ptr::null_mut());
        let mut tm: libc::tm = std::mem::zeroed();
        if !libc::localtime_r(&now, &mut tm).is_null() {
            let mut bytes = [0; 64];
            let count = libc::strftime(
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                c"%Y/%m/%d %H:%M:%S".as_ptr(),
                &tm,
            );
            return String::from_utf8_lossy(&bytes[..count]).into_owned();
        }
    }
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}/{:02}/{:02} {:02}:{:02}:{:02}",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}
