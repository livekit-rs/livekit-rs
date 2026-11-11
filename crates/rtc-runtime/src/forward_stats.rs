//! Manager-owned periodic collector worker, independent of rooms or media traffic.
use rtc_core::forward_stats::ForwardStats;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

pub(crate) struct ForwardStatsWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl ForwardStatsWorker {
    pub(crate) fn new(stats: Option<&Arc<ForwardStats>>) -> Self {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let task = stats.map(|stats| {
            let stats = stats.clone();
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("forward-stats".into())
                .spawn(move || {
                    loop {
                        let (flag, wake) = &*stop;
                        let closed = flag.lock().unwrap_or_else(|p| p.into_inner());
                        if *closed {
                            break;
                        }
                        let delay = stats
                            .next_deadline()
                            .saturating_duration_since(Instant::now());
                        let (closed, _) = wake
                            .wait_timeout(closed, delay)
                            .unwrap_or_else(|p| p.into_inner());
                        if *closed {
                            break;
                        }
                        drop(closed);
                        stats.tick(Instant::now());
                    }
                })
                .expect("create forwarding statistics worker")
        });
        Self {
            stop,
            task: Mutex::new(task),
        }
    }
    pub(crate) fn stop(&self) {
        let (closed, wake) = &*self.stop;
        *closed.lock().unwrap_or_else(|p| p.into_inner()) = true;
        wake.notify_all();
        if let Some(task) = self.task.lock().unwrap_or_else(|p| p.into_inner()).take() {
            task.join().expect("forwarding statistics worker panicked");
        }
    }
}
impl Drop for ForwardStatsWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_worker_expires_empty_windows_and_stops_joined() {
        let now = Instant::now();
        let stats = ForwardStats::new(1_000_000, 1_000_000, 1_000_000, now).unwrap();
        stats.record_forward_samples(now, [now - std::time::Duration::from_millis(1)]);
        let worker = ForwardStatsWorker::new(Some(&stats));
        let deadline = now + std::time::Duration::from_secs(1);
        while stats.snapshot().histogram_count == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(stats.snapshot().histogram_count, 1);
        while stats.get_stats_ns(0) != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        worker.stop();
        assert!(worker.task.lock().unwrap().is_none());
        let before = stats.snapshot().histogram_count;
        stats.record_forward_samples(Instant::now(), [Instant::now()]);
        std::thread::sleep(std::time::Duration::from_millis(3));
        assert_eq!(
            stats.snapshot().histogram_count,
            before,
            "joined worker cannot publish new samples"
        );
    }
}
