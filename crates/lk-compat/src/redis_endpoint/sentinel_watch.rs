//! Owned Sentinel event consumer. Fixed go-redis sentinel.go1208/1238 listens
//! to +switch-master, rather than periodically probing a readable old replica.
use super::*;
use crate::twirp::error::go_quote;
use redis::{PushInfo, PushKind};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(super) struct SentinelEvents {
    state: Arc<Mutex<EventState>>,
    epoch: watch::Sender<u64>,
    selection: watch::Sender<()>,
    pub(super) discovery_gate: tokio::sync::Mutex<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
#[derive(Default)]
struct EventState {
    master: Option<String>,
    discovered: Vec<String>,
    // The configured addresses precede the append-only list of discovered
    // peers. RESP2 and RESP3 connection views share this selection.
    selected_index: Option<usize>,
    diagnostics_started: bool,
}
impl EventState {
    fn addresses(&self, configured: &[String]) -> Vec<String> {
        let mut addresses = configured.to_vec();
        addresses.extend(
            self.discovered
                .iter()
                .filter(|value| !configured.contains(value))
                .cloned(),
        );
        addresses
    }
}
impl Default for SentinelEvents {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(EventState::default())),
            epoch: watch::channel(0).0,
            selection: watch::channel(()).0,
            discovery_gate: tokio::sync::Mutex::new(()),
            worker: Mutex::new(None),
        }
    }
}
impl Drop for SentinelEvents {
    fn drop(&mut self) {
        // The worker owns only its state/sender and target material, not this
        // owner. Last endpoint drop therefore actually closes its live PubSub.
        if let Some(worker) = lock_mut(&self.worker).take() {
            worker.abort();
        }
    }
}
impl SentinelEvents {
    pub(super) fn selected_index(&self) -> Option<usize> {
        lock(&self.state).selected_index
    }

    pub(super) fn select(&self, index: usize, sentinel: &str, resolved: &str, name: &str) {
        let mut state = lock_mut(&self.state);
        let previous = state.selected_index.replace(index);
        if previous == Some(index) {
            return;
        }
        write_redis_diagnostic(
            "sentinel.go:1032",
            format!("sentinel: selected addr={sentinel} masterAddr={resolved}").as_bytes(),
        );
        if !state.diagnostics_started {
            report_master(name, state.master.as_deref().unwrap_or(resolved));
            state.diagnostics_started = true;
        }
        self.selection.send_replace(());
    }

    pub(super) fn clear_selection(&self) {
        let mut state = lock_mut(&self.state);
        if state.selected_index.take().is_some() {
            self.selection.send_replace(());
        }
    }

    pub(super) fn receiver(&self) -> watch::Receiver<u64> {
        self.epoch.subscribe()
    }
    pub(super) fn epoch(&self) -> u64 {
        *self.epoch.borrow()
    }
    pub(super) fn reconcile(&self, address: String, observed: u64, name: &str) -> String {
        let mut state = lock_mut(&self.state);
        // A discovery request started before a switch event must not publish
        // its older reply over that newer event's master address.
        if observed != self.epoch() {
            return state.master.clone().unwrap_or(address);
        }
        install_master(&mut state, &self.epoch, address.clone(), name);
        address
    }
    pub(super) fn addresses(&self, configured: &[String]) -> Vec<String> {
        lock(&self.state).addresses(configured)
    }
    pub(super) fn start(&self, target: SentinelTarget, tls: Option<Arc<TlsSniOverride>>) {
        let mut worker = lock_mut(&self.worker);
        if worker.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let state = self.state.clone();
        let epoch = self.epoch.clone();
        let mut selection = self.selection.subscribe();
        *worker = Some(tokio::spawn(async move {
            let initial_delay = Duration::from_millis(100);
            let mut delay = initial_delay;
            loop {
                selection.borrow_and_update();
                let addresses = {
                    let state = lock(&state);
                    state.selected_index.map(|index| {
                        let mut values = state.addresses(&target.addresses);
                        values.swap(0, index);
                        values
                    })
                };
                let Some(addresses) = addresses else {
                    if selection.changed().await.is_err() {
                        break;
                    }
                    continue;
                };
                let observed_selection = selection.clone();
                let subscriptions = async {
                    for address in addresses {
                        let _ = consume(
                            &target,
                            &address,
                            tls.as_deref(),
                            &state,
                            &epoch,
                            &observed_selection,
                        )
                        .await;
                    }
                    tokio::time::sleep(delay).await;
                };
                tokio::select! {
                    biased;
                    changed = selection.changed() => {
                        // Dropping the old consumer also releases its live Pub/Sub socket.
                        if changed.is_err() {
                            break;
                        }
                        delay = initial_delay;
                    }
                    () = subscriptions => delay = (delay * 2).min(Duration::from_secs(2)),
                }
            }
        }));
    }
}
fn report_master(name: &str, address: &str) {
    write_redis_diagnostic(
        "sentinel.go:1194",
        format!(
            "sentinel: new master={} addr={}",
            go_quote(name),
            go_quote(address)
        )
        .as_bytes(),
    );
}

fn install_master(state: &mut EventState, epoch: &watch::Sender<u64>, address: String, name: &str) {
    if state.master.as_ref() == Some(&address) {
        return;
    }
    let changed = state.master.is_some();
    let master = state.master.insert(address);
    if changed {
        if state.diagnostics_started {
            report_master(name, master);
        }
        epoch.send_modify(|value| {
            *value = value.checked_add(1).expect("Sentinel generation exhausted")
        });
    }
}
fn switch_address(master: &str, payload: &[u8]) -> Option<String> {
    let payload = std::str::from_utf8(payload).ok()?;
    let parts = payload.split(' ').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != master || parts[3].is_empty() {
        return None;
    }
    let port = parts[4].parse::<u16>().ok()?;
    Some(addr_string(&ip_addr(parts[3].to_owned(), port)))
}
async fn consume(
    target: &SentinelTarget,
    address: &str,
    tls: Option<&TlsSniOverride>,
    state: &Arc<Mutex<EventState>>,
    epoch: &watch::Sender<u64>,
    selection: &watch::Receiver<()>,
) -> redis::RedisResult<()> {
    let (push, mut messages) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
    let info = (target.sentinel_info)(address).map_err(|reason| {
        RedisError::from((ErrorKind::Client, "sentinel configuration", reason))
    })?;
    let config = target.discovery_config().set_push_sender(push);
    let mut connection = dial_with(resp3(info), &config, tls).await?;
    // ACK must complete before re-discovery. A switch during initial discovery
    // is either visible in the reply or queued in this actual PubSub stream.
    connection
        .subscribe(&["+switch-master", "+replica-reconf-done"])
        .await?;
    let baseline = *epoch.borrow();
    let discovery = target.discovery_config();
    if let Some(master) = query_sentinel(target, address, &discovery, tls).await? {
        let mut guard = lock_mut(state);
        let current = *epoch.borrow();
        if current == baseline && matches!(selection.has_changed(), Ok(false)) {
            install_master(&mut guard, epoch, addr_string(&master), &target.master_name);
        }
    }
    // Preserve upstream's discovered Sentinel fallback list. Failure to list
    // peers does not invalidate the already ACKed master event subscription.
    if let Ok(discovered) = redis::cmd("SENTINEL")
        .arg("SENTINELS")
        .arg(&target.master_name)
        .query_async::<Vec<std::collections::HashMap<String, String>>>(&mut connection)
        .await
    {
        let mut guard = lock_mut(state);
        for row in discovered {
            if let (Some(host), Some(port)) = (row.get("ip"), row.get("port"))
                && !host.is_empty()
                && let Ok(port) = port.parse::<u16>()
            {
                let address = addr_string(&ip_addr(host.clone(), port));
                if !target.addresses.contains(&address) && !guard.discovered.contains(&address) {
                    guard.discovered.push(address);
                }
            }
        }
    }
    while let Some(message) = messages.recv().await {
        match message.kind {
            PushKind::Disconnection => break,
            PushKind::Message => {
                if let [Value::BulkString(channel), Value::BulkString(payload)] =
                    message.data.as_slice()
                    && channel == b"+switch-master"
                    && let Some(address) = switch_address(&target.master_name, payload)
                {
                    let mut guard = lock_mut(state);
                    if !matches!(selection.has_changed(), Ok(false)) {
                        break;
                    }
                    let first_event = guard.master.is_none();
                    install_master(&mut guard, epoch, address, &target.master_name);
                    if first_event {
                        epoch.send_modify(|value| {
                            *value = value.checked_add(1).expect("Sentinel generation exhausted")
                        });
                    }
                }
            }
            _ => {}
        }
    }
    drop(connection);
    Err(RedisError::from((
        ErrorKind::Io,
        "Sentinel event connection closed",
    )))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn switch_events_filter_master_and_preserve_epoch_fence_and_ipv6() {
        let events = SentinelEvents::default();
        assert_eq!(
            events.reconcile("127.0.0.1:6379".into(), 0, "main"),
            "127.0.0.1:6379"
        );
        assert_eq!(events.epoch(), 0);
        let mut receiver = events.receiver();
        let address = switch_address("main", b"main 127.0.0.1 6379 ::1 6380").unwrap();
        install_master(
            &mut lock_mut(&events.state),
            &events.epoch,
            address.clone(),
            "main",
        );
        assert_eq!(address, "[::1]:6380");
        assert!(receiver.has_changed().unwrap());
        assert_eq!(*receiver.borrow_and_update(), 1);
        assert_eq!(
            events.reconcile("127.0.0.1:6379".into(), 0, "main"),
            address
        );
        assert_eq!(events.epoch(), 1);
        assert!(switch_address("main", b"other a 6379 b 6380").is_none());
        assert!(switch_address("main", b"main a 6379 b 999999").is_none());
        assert!(switch_address("main", b"main malformed").is_none());
    }
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;
    #[tokio::test]
    async fn final_event_owner_cancels_its_live_worker() {
        struct Exit(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Exit {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let events = Arc::new(SentinelEvents::default());
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (exit, exited) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _exit = Exit(Some(exit));
            let _ = entered.send(());
            std::future::pending::<()>().await;
        });
        *lock_mut(&events.worker) = Some(task);
        ready.await.unwrap();
        let other = events.clone();
        drop(events);
        assert!(!lock(&other.worker).as_ref().unwrap().is_finished());
        drop(other);
        tokio::time::timeout(Duration::from_secs(1), exited)
            .await
            .unwrap()
            .unwrap();
    }
}
