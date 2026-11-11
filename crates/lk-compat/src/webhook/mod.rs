//! Webhook notifier (impl-notes/api-auth-webhook.md §4; protocol `webhook/`
//! `ResourceURLNotifier`).
//!
//! * Static URLs each own resource FIFO queues. All extra URLs share one
//!   independent set of resource FIFOs, with URL/signing key owned by each item.
//!   All events of a room share a queue within that notifier, so
//!   per-room order is preserved and rooms are independent. One worker task per
//!   queue delivers sequentially.
//! * `max_depth` (default 200) is enforced at enqueue ("queue is full"),
//!   `max_age` (default 30 s) at dequeue ("age").
//! * A queue idle for 5 min is retired by its worker.
//! * Body: camelCase protojson of `WebhookEvent` (defaults omitted). Headers:
//!   `Authorization: <raw JWT>` (no `Bearer`), `content-type:
//!   application/webhook+json`, `User-Agent: LiveKit`. The JWT (5 min, `sha256`
//!   of the body) is minted when the event is dequeued and reused across retries.
//!
//! HTTPS uses the explicit RustCrypto TLS 1.3 provider with WebPKI chain,
//! validity and hostname verification. Per-attempt requests are bounded (30 s connect, 30 s total),
//! unlike upstream's unbounded client.

pub mod delivery;
pub mod events;
mod retry_after;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rtc_core::config::{PolicyDuration, ServerConfig, WebhookConfig};
use rtc_core::events::{EngineEvent, EventSink};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::{KeyStore, TokenMinter};
use delivery::DeliveryOutcome;

pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_DEPTH: usize = 200;
/// `defaultTimeout`: idle queues are stopped after this long.
pub const QUEUE_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Bound on distinct queues per URL (≈ concurrently active rooms).
pub const MAX_QUEUES_PER_URL: usize = 10_000;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("api_key is required to use webhooks")]
    MissingApiKey,
    #[error("invalid webhook URL {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("webhook notifier must be created inside a tokio runtime")]
    NoRuntime,
    #[error("could not build webhook HTTP client: {0}")]
    Client(String),
    #[error("no secret for provided signing key")]
    UnknownSigningKey,
    #[error("no singing key or secret was provided")]
    NoSigningKey,
    #[error("notifier is closed")]
    Closed,
    #[error("queue is full")]
    QueueFull,
    #[error("too many queues")]
    TooManyQueues,
    #[error("could not encode webhook event: {0}")]
    Encoding(String),
}

const OPEN: u8 = 0;
const DRAINING: u8 = 1;
const DROPPING: u8 = 2;

struct Item {
    url: String,
    api_key: String,
    event: String,
    body: Bytes,
    enqueued: Instant,
}

struct QueueEntry {
    id: u64,
    tx: mpsc::Sender<Item>,
    worker: JoinHandle<()>,
}

struct UrlQueues {
    url: String,
    queues: Mutex<HashMap<String, QueueEntry>>,
}

struct Inner {
    api_key: String,
    minter: TokenMinter,
    keys: Arc<KeyStore>,
    include: Vec<String>,
    exclude: Vec<String>,
    full_room: bool,
    /// `None` means a negative limit, so every nonnegative queue age expires.
    max_age: Option<Duration>,
    max_depth: usize,
    client: reqwest::Client,
    https_client: rtc_tls::http::HttpsClient,
    urls: Vec<Arc<UrlQueues>>,
    extra: Arc<UrlQueues>,
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    state: AtomicU8,
    next_queue_id: AtomicU64,
    dropped: AtomicU64,
    delivered: AtomicU64,
}

/// Engine event sink that posts LiveKit webhooks.
#[derive(Clone)]
pub struct WebhookNotifier {
    inner: Arc<Inner>,
}

/// Only HTTP and verified HTTPS endpoints are accepted.
pub fn validate_url(url: &str) -> Result<(), WebhookError> {
    let parsed = reqwest::Url::parse(url).map_err(|e| WebhookError::InvalidUrl {
        url: url.to_string(),
        reason: e.to_string(),
    })?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        other => Err(WebhookError::InvalidUrl {
            url: url.to_string(),
            reason: format!("unsupported scheme {other:?}"),
        }),
    }
}

impl WebhookNotifier {
    pub fn from_server_config(
        config: &ServerConfig,
        keys: Arc<KeyStore>,
    ) -> Result<Self, WebhookError> {
        Self::new(
            &config.webhook,
            keys,
            config.room.enable_full_room_in_webhooks,
        )
    }

    /// Fails like upstream startup when URLs are configured without a known
    /// `api_key`, and for URLs this build cannot reach.
    pub fn new(
        config: &WebhookConfig,
        keys: Arc<KeyStore>,
        full_room: bool,
    ) -> Result<Self, WebhookError> {
        let mut runtime = None;
        if !config.urls.is_empty() {
            if config.api_key.is_empty() || keys.secret(&config.api_key).is_none() {
                return Err(WebhookError::MissingApiKey);
            }
            for url in &config.urls {
                validate_url(url)?;
            }
            runtime =
                Some(tokio::runtime::Handle::try_current().map_err(|_| WebhookError::NoRuntime)?);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(ATTEMPT_TIMEOUT)
            .build()
            .map_err(|e| WebhookError::Client(e.to_string()))?;
        let https_client = rtc_tls::http::HttpsClient::new(CONNECT_TIMEOUT, ATTEMPT_TIMEOUT)
            .map_err(|e| WebhookError::Client(e.to_string()))?;
        let urls = config
            .urls
            .iter()
            .map(|u| {
                Arc::new(UrlQueues {
                    url: u.clone(),
                    queues: Mutex::new(HashMap::new()),
                })
            })
            .collect();
        Ok(Self {
            inner: Arc::new(Inner {
                api_key: config.api_key.clone(),
                minter: TokenMinter::new(keys.clone()),
                keys,
                include: config.include_events.clone(),
                exclude: config.exclude_events.clone(),
                full_room,
                max_age: if config.max_age == PolicyDuration::ZERO {
                    Some(DEFAULT_MAX_AGE)
                } else {
                    config.max_age.nonnegative_to_std()
                },
                max_depth: match config.max_depth {
                    0 => DEFAULT_MAX_DEPTH,
                    depth if depth < 0 => 0,
                    // Larger limits exceed addressable queue memory before
                    // they can be reached. Stay within Tokio's permit range.
                    depth => usize::try_from(depth)
                        .unwrap_or(usize::MAX)
                        .min(tokio::sync::Semaphore::MAX_PERMITS),
                },
                client,
                https_client,
                urls,
                // One ResourceURLNotifier for every extra destination, grouped
                // only by resource. Destination and key belong to each item.
                extra: Arc::new(UrlQueues {
                    url: String::new(),
                    queues: Mutex::new(HashMap::new()),
                }),
                runtime: Mutex::new(runtime),
                state: AtomicU8::new(OPEN),
                next_queue_id: AtomicU64::new(1),
                dropped: AtomicU64::new(0),
                delivered: AtomicU64::new(0),
            }),
        })
    }

    pub fn is_enabled(&self) -> bool {
        !self.inner.urls.is_empty()
    }

    /// Events dropped (full queue, age, closed, signing failure, delivery failure).
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    pub fn delivered_count(&self) -> u64 {
        self.inner.delivered.load(Ordering::Relaxed)
    }

    /// Stop accepting events. `drain = true` lets every queue finish (upstream
    /// `Stop(false)`); `drain = false` drops queued events and aborts in-flight
    /// deliveries (`Stop(true)`).
    pub async fn stop(&self, drain: bool) {
        let target = if drain { DRAINING } else { DROPPING };
        self.inner.state.store(target, Ordering::SeqCst);
        let mut workers = Vec::new();
        for u in self
            .inner
            .urls
            .iter()
            .chain(std::iter::once(&self.inner.extra))
        {
            let mut map = u.queues.lock().unwrap_or_else(|e| e.into_inner());
            for (_, entry) in map.drain() {
                // Dropping the sender ends the worker after its backlog.
                drop(entry.tx);
                workers.push(entry.worker);
            }
        }
        for w in workers {
            if !drain {
                w.abort();
            }
            let _ = w.await;
        }
    }
}

impl Inner {
    fn note_drop(&self, url: &str, event: &str, reason: &str) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(url, event, reason, "dropped webhook");
    }

    fn enqueue(
        self: &Arc<Self>,
        urlq: &Arc<UrlQueues>,
        key: &str,
        item: Item,
    ) -> Result<(), WebhookError> {
        if self.state.load(Ordering::SeqCst) != OPEN {
            self.note_drop(&item.url, &item.event, "notifier is closed");
            return Err(WebhookError::Closed);
        }
        if self.max_depth == 0 {
            self.note_drop(&item.url, &item.event, "queue is full");
            return Err(WebhookError::QueueFull);
        }
        let runtime = {
            let mut runtime = self.runtime.lock().unwrap_or_else(|e| e.into_inner());
            if runtime.is_none() {
                *runtime = Some(
                    tokio::runtime::Handle::try_current().map_err(|_| WebhookError::NoRuntime)?,
                );
            }
            runtime.as_ref().expect("runtime present").clone()
        };
        let mut map = urlq.queues.lock().unwrap_or_else(|e| e.into_inner());
        if self.state.load(Ordering::SeqCst) != OPEN {
            self.note_drop(&item.url, &item.event, "notifier is closed");
            return Err(WebhookError::Closed);
        }
        let mut item = Some(item);
        // A closed channel (idle retirement racing the lookup) is replaced once.
        for _ in 0..2 {
            if !map.contains_key(key) {
                if map.len() >= MAX_QUEUES_PER_URL {
                    if let Some(i) = item.take() {
                        self.note_drop(&i.url, &i.event, "too many queues");
                    }
                    return Err(WebhookError::TooManyQueues);
                }
                let id = self.next_queue_id.fetch_add(1, Ordering::Relaxed);
                let (tx, rx) = mpsc::channel(self.max_depth);
                let worker = runtime.spawn(run_worker(
                    self.clone(),
                    urlq.clone(),
                    key.to_string(),
                    id,
                    rx,
                ));
                map.insert(key.to_string(), QueueEntry { id, tx, worker });
            }
            let entry = map.get(key).expect("queue present");
            match entry.tx.try_send(item.take().expect("item present")) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(i)) => {
                    self.note_drop(&i.url, &i.event, "queue is full");
                    return Err(WebhookError::QueueFull);
                }
                Err(mpsc::error::TrySendError::Closed(i)) => {
                    map.remove(key);
                    item = Some(i);
                }
            }
        }
        if let Some(i) = item {
            self.note_drop(&i.url, &i.event, "queue is closed");
        }
        Err(WebhookError::Closed)
    }

    /// Idle timeout: retire the queue unless an item raced in.
    fn retire_or_take(
        &self,
        urlq: &UrlQueues,
        key: &str,
        id: u64,
        rx: &mut mpsc::Receiver<Item>,
    ) -> Option<Item> {
        let mut map = urlq.queues.lock().unwrap_or_else(|e| e.into_inner());
        if let Ok(item) = rx.try_recv() {
            return Some(item);
        }
        if map.get(key).is_some_and(|e| e.id == id) {
            map.remove(key);
        }
        None
    }

    async fn send(&self, item: Item) {
        let url = &item.url;
        let jwt =
            match self
                .minter
                .webhook_token(&item.api_key, &item.body, rtc_core::util::unix_s())
            {
                Ok(t) => t,
                Err(_) => {
                    self.note_drop(url, &item.event, "no singing key or secret was provided");
                    return;
                }
            };
        match delivery::deliver_with_https(&self.client, &self.https_client, url, &jwt, item.body)
            .await
        {
            DeliveryOutcome::Delivered(status) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(url, event = %item.event, status, "sent webhook");
            }
            DeliveryOutcome::Failed(reason) => self.note_drop(url, &item.event, &reason),
        }
    }
}

async fn run_worker(
    inner: Arc<Inner>,
    urlq: Arc<UrlQueues>,
    key: String,
    id: u64,
    mut rx: mpsc::Receiver<Item>,
) {
    loop {
        let item = match tokio::time::timeout(QUEUE_IDLE_TIMEOUT, rx.recv()).await {
            Ok(Some(item)) => item,
            Ok(None) => return,
            Err(_) => match inner.retire_or_take(&urlq, &key, id, &mut rx) {
                Some(item) => item,
                None => return,
            },
        };
        if inner.state.load(Ordering::SeqCst) == DROPPING {
            inner.note_drop(&item.url, &item.event, "notifier is closed");
            return;
        }
        if inner
            .max_age
            .is_none_or(|max_age| item.enqueued.elapsed() > max_age)
        {
            inner.note_drop(&item.url, &item.event, "age");
            continue;
        }
        inner.send(item).await;
    }
}

impl WebhookNotifier {
    /// Queue an actual adapter-owned event using the same signing, filtering,
    /// resource ordering and shutdown lifecycle as RTC room events.
    pub fn notify_webhook(&self, ev: lk_protocol::livekit::WebhookEvent) {
        if let Err(error) = self.notify_webhook_with_extras(ev, &[]) {
            tracing::warn!(%error, "failed to notify webhook");
        }
    }

    /// The fixed DefaultNotifier sends static destinations first and then extras
    /// in request order. Its independent extra notifier has an empty filter;
    /// WebhookConfig.filter_params is not read by that fixed implementation.
    /// Return the first real enqueue/key error and do not process later extras.
    pub fn notify_webhook_with_extras(
        &self,
        mut ev: lk_protocol::livekit::WebhookEvent,
        extras: &[lk_protocol::livekit::WebhookConfig],
    ) -> Result<(), WebhookError> {
        let inner = &self.inner;
        let static_allowed = events::is_allowed(&ev.event, &inner.include, &inner.exclude);
        if (!static_allowed || inner.urls.is_empty()) && extras.is_empty() {
            return Ok(());
        }
        ev.id = rtc_core::ids::new_sid("EV_");
        ev.created_at = rtc_core::util::unix_s();
        let body = Bytes::from(
            lk_protocol::json::encode_default(&ev)
                .map_err(|e| WebhookError::Encoding(e.to_string()))?,
        );
        let key = events::event_key(&ev);
        let now = Instant::now();
        if static_allowed {
            for urlq in &inner.urls {
                inner.enqueue(
                    urlq,
                    &key,
                    Item {
                        url: urlq.url.clone(),
                        api_key: inner.api_key.clone(),
                        event: ev.event.clone(),
                        body: body.clone(),
                        enqueued: now,
                    },
                )?;
            }
        }
        for extra in extras {
            let api_key = if extra.signing_key.is_empty() {
                &inner.api_key
            } else {
                &extra.signing_key
            };
            if !extra.signing_key.is_empty() && inner.keys.secret(api_key).is_none() {
                return Err(WebhookError::UnknownSigningKey);
            }
            // DefaultNotifier resolves an explicit override before delegation;
            // the ResourceURLNotifier then checks closed before default keys.
            if inner.state.load(Ordering::SeqCst) != OPEN {
                return Err(WebhookError::Closed);
            }
            if api_key.is_empty() || inner.keys.secret(api_key).is_none() {
                return Err(WebhookError::NoSigningKey);
            }
            // Per-request filter_params is intentionally ignored by the fixed
            // source; global filters do not leak into this independent queue.
            inner.enqueue(
                &inner.extra,
                &key,
                Item {
                    url: extra.url.clone(),
                    api_key: api_key.clone(),
                    event: ev.event.clone(),
                    body: body.clone(),
                    enqueued: now,
                },
            )?;
        }
        Ok(())
    }
}

impl EventSink for WebhookNotifier {
    fn emit(&self, event: EngineEvent) {
        self.notify_webhook(events::to_webhook_event(&event, self.inner.full_room));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Arc<KeyStore> {
        Arc::new(KeyStore::new(vec![("key".into(), "secret".into())]))
    }

    fn cfg(urls: &[&str], api_key: &str) -> WebhookConfig {
        WebhookConfig {
            urls: urls.iter().map(|s| s.to_string()).collect(),
            api_key: api_key.into(),
            ..Default::default()
        }
    }

    #[test]
    fn construction_errors() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _g = rt.enter();
        assert!(matches!(
            WebhookNotifier::new(&cfg(&["http://x/"], "nokey"), keys(), false),
            Err(WebhookError::MissingApiKey)
        ));
        assert!(WebhookNotifier::new(&cfg(&["https://x/"], "key"), keys(), false).is_ok());
        assert!(WebhookNotifier::new(&cfg(&["ftp://x/"], "key"), keys(), false).is_err());
        assert!(
            WebhookNotifier::new(&cfg(&["http://127.0.0.1:1/hook"], "key"), keys(), false).is_ok()
        );
        // no URLs: no key needed, no runtime needed
        assert!(WebhookNotifier::new(&cfg(&[], ""), keys(), false).is_ok());
    }

    #[test]
    fn full_queue_drops_at_enqueue() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _g = rt.enter();
        let mut c = cfg(&["http://127.0.0.1:1/hook"], "key");
        c.max_depth = 1;
        let n = WebhookNotifier::new(&c, keys(), false).unwrap();
        let room = rtc_core::model::RoomInfo {
            turn_password: String::new(),
            sid: rtc_core::ids::RoomSid("RM_a".into()),
            name: rtc_core::ids::RoomName("r".into()),
            empty_timeout_s: 0,
            departure_timeout_s: 0,
            max_participants: 0,
            creation_time_ms: 0,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: vec![],
            version: Default::default(),
        };
        // The worker has not run yet (current-thread runtime not driven), so the
        // first event fills the single slot and the second is dropped.
        n.emit(EngineEvent::RoomStarted { room: room.clone() });
        n.emit(EngineEvent::RoomStarted { room });
        assert_eq!(n.dropped_count(), 1);
        rt.block_on(n.stop(false));
    }
}

#[cfg(test)]
mod extra_tests {
    use super::*;
    use axum::{
        Router,
        body::Bytes as AxumBytes,
        extract::State,
        http::{HeaderMap, StatusCode, Uri},
        routing::any,
    };
    use lk_protocol::livekit as pb;
    struct Captured {
        path: String,
        headers: HeaderMap,
        body: AxumBytes,
    }
    struct Server {
        url: String,
        rx: mpsc::UnboundedReceiver<Captured>,
        task: JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn receive(
        State(tx): State<mpsc::UnboundedSender<Captured>>,
        uri: Uri,
        headers: HeaderMap,
        body: AxumBytes,
    ) -> StatusCode {
        tx.send(Captured {
            path: uri.path().into(),
            headers,
            body,
        })
        .unwrap();
        StatusCode::OK
    }
    async fn server() -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let app = Router::new().fallback(any(receive)).with_state(tx);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Server {
            url: format!("http://{addr}"),
            rx,
            task,
        }
    }
    fn keys() -> Arc<KeyStore> {
        Arc::new(KeyStore::new(vec![
            ("key".into(), "secret".into()),
            ("other".into(), "other-secret".into()),
        ]))
    }
    fn config(urls: Vec<String>, api_key: &str) -> WebhookConfig {
        WebhookConfig {
            urls,
            api_key: api_key.into(),
            ..Default::default()
        }
    }
    fn event(name: &str) -> pb::WebhookEvent {
        pb::WebhookEvent {
            event: name.into(),
            egress_info: Some(pb::EgressInfo {
                egress_id: "EG_order".into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn extra(url: String, key: &str) -> pb::WebhookConfig {
        pb::WebhookConfig {
            url,
            signing_key: key.into(),
            ..Default::default()
        }
    }
    fn verify(capture: &Captured, issuer: &str, secret: &str) -> serde_json::Value {
        use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
        let token = capture
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(!token.starts_with("Bearer "));
        let claims = decode::<serde_json::Value>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &Validation::new(Algorithm::HS256),
        )
        .unwrap()
        .claims;
        assert_eq!(claims["iss"], issuer);
        assert_eq!(
            claims["sha256"],
            crate::auth::minter::body_sha256(&capture.body)
        );
        assert_eq!(
            capture.headers["content-type"],
            delivery::CONTENT_TYPE_WEBHOOK
        );
        assert_eq!(capture.headers["user-agent"], "LiveKit");
        serde_json::from_slice(&capture.body).unwrap()
    }
    fn notifier_from_input(
        url: &str,
        property: &str,
        yaml_value: &str,
        env_value: Option<&str>,
        flag_value: Option<&str>,
    ) -> WebhookNotifier {
        use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
        use std::net::IpAddr;

        struct NoNetwork;
        impl NetProbe for NoNetwork {
            fn local_addresses(&self) -> Vec<IpAddr> {
                vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)]
            }
            fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
                panic!("webhook fixture must not discover external IP")
            }
            fn sleep(&self, _: Duration) {
                panic!("webhook fixture must not retry IP discovery")
            }
        }

        let yaml = format!(
            "webhook:\n  api_key: key\n  urls: ['{url}']\n  resource_url_notifier:\n    {property}: {yaml_value}\n",
        );
        let mut args = vec![
            "livecloud".into(),
            "--dev".into(),
            "--config-body".into(),
            yaml,
        ];
        if let Some(value) = flag_value {
            args.push(format!(
                "--webhook.resource_url_notifier.{property}={value}"
            ));
        }
        let env = env_value.map_or_else(Env::default, |value| {
            let key = format!(
                "LIVEKIT_WEBHOOK_RESOURCE_URL_NOTIFIER_{}",
                property.to_ascii_uppercase()
            );
            Env::from_os_pairs([(key, value)])
        });
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "webhook-queue-fixture".into(),
            compat_version: "1.13.10",
        };
        let action = parse_with_probe(
            args.into_iter().map(std::ffi::OsString::from).collect(),
            &env,
            &info,
            &NoNetwork,
        )
        .unwrap();
        let EntryAction::RunServer(config) = action else {
            panic!("expected server config for {property}");
        };
        WebhookNotifier::from_server_config(&config.server, keys()).unwrap()
    }

    #[tokio::test]
    async fn signed_queue_age_from_yaml_env_and_cli_controls_actual_webhook_delivery() {
        let mut receiver = server().await;
        for (name, yaml_age, env_age, flag_age, deliver) in [
            ("negative YAML", "-1ns", None, None, false),
            (
                "negative environment over YAML",
                "1h",
                Some("-1ns"),
                None,
                false,
            ),
            (
                "negative CLI over environment",
                "1h",
                Some("1h"),
                Some("-1ns"),
                false,
            ),
            ("zero uses default", "0s", None, None, true),
            ("positive maximum age", "1h", None, None, true),
        ] {
            let notifier =
                notifier_from_input(&receiver.url, "max_age", yaml_age, env_age, flag_age);
            notifier
                .notify_webhook_with_extras(event("egress_started"), &[])
                .unwrap();
            notifier.stop(true).await;
            assert_eq!(notifier.dropped_count(), u64::from(!deliver), "{name}");
            assert_eq!(notifier.delivered_count(), u64::from(deliver), "{name}");
            if deliver {
                let captured = receiver.rx.try_recv().expect("actual webhook POST");
                assert_eq!(
                    verify(&captured, "key", "secret")["event"],
                    "egress_started",
                    "{name}"
                );
            }
            assert!(
                receiver.rx.try_recv().is_err(),
                "unexpected webhook POST for {name}"
            );
        }
    }

    #[tokio::test]
    async fn signed_queue_depth_rejects_negative_yaml_env_and_cli_before_dispatch() {
        let mut receiver = server().await;
        for (name, yaml_depth, env_depth, flag_depth, deliver) in [
            ("negative YAML", "-1", None, None, false),
            (
                "negative environment over YAML",
                "1",
                Some("-1"),
                None,
                false,
            ),
            (
                "negative CLI over environment",
                "1",
                Some("1"),
                Some("-1"),
                false,
            ),
            ("zero uses default", "0", None, None, true),
            ("positive depth", "1", None, None, true),
            (
                "largest signed depth",
                "9223372036854775807",
                None,
                None,
                true,
            ),
        ] {
            let notifier = notifier_from_input(
                &receiver.url,
                "max_depth",
                yaml_depth,
                env_depth,
                flag_depth,
            );
            let queued = notifier.notify_webhook_with_extras(event("egress_started"), &[]);
            if deliver {
                queued.unwrap();
            } else {
                assert!(
                    matches!(queued, Err(WebhookError::QueueFull)),
                    "{name}: {queued:?}"
                );
            }
            notifier.stop(true).await;
            assert_eq!(notifier.dropped_count(), u64::from(!deliver), "{name}");
            assert_eq!(notifier.delivered_count(), u64::from(deliver), "{name}");
            if deliver {
                let captured = receiver.rx.try_recv().expect("actual webhook POST");
                assert_eq!(
                    verify(&captured, "key", "secret")["event"],
                    "egress_started",
                    "{name}"
                );
            }
            assert!(
                receiver.rx.try_recv().is_err(),
                "unexpected webhook POST for {name}"
            );
        }
    }

    #[tokio::test]
    async fn extras_share_one_resource_fifo_across_urls_and_each_item_keeps_its_actual_signing_key()
    {
        let mut server = server().await;
        let n = WebhookNotifier::new(&config(Vec::new(), "key"), keys(), false).unwrap();
        let extra = vec![
            extra(format!("{}/first", server.url), ""),
            extra(format!("{}/second", server.url), "other"),
        ];
        n.notify_webhook_with_extras(event("egress_started"), &extra)
            .unwrap();
        n.notify_webhook_with_extras(event("egress_ended"), &extra)
            .unwrap();
        assert_eq!(
            n.inner.extra.queues.lock().unwrap().len(),
            1,
            "destinations do not create independent maps/notifiers"
        );
        n.stop(true).await;
        let mut captured = Vec::new();
        while let Ok(c) = server.rx.try_recv() {
            captured.push(c);
        }
        assert_eq!(
            captured.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
            ["/first", "/second", "/first", "/second"]
        );
        let first = verify(&captured[0], "key", "secret");
        let same = verify(&captured[1], "other", "other-secret");
        assert_eq!(captured[0].body, captured[1].body);
        assert_eq!(first["id"], same["id"]);
        assert_eq!(first["createdAt"], same["createdAt"]);
        let next = verify(&captured[2], "key", "secret");
        assert_ne!(first["id"], next["id"]);
        assert_eq!(next["event"], "egress_ended");
        verify(&captured[3], "other", "other-secret");
        assert_eq!(n.delivered_count(), 4);
        assert_eq!(n.dropped_count(), 0);
    }
    #[tokio::test]
    async fn global_filter_is_independent_and_fixed_source_does_not_consume_extra_filter_params() {
        let mut server = server().await;
        let mut c = config(vec![format!("{}/global", server.url)], "key");
        c.exclude_events = vec!["egress_started".into()];
        let n = WebhookNotifier::new(&c, keys(), false).unwrap();
        let mut e = extra(format!("{}/extra", server.url), "");
        e.filter_params = Some(pb::FilterParams {
            include_events: vec!["ingress_ended".into()],
            exclude_events: Vec::new(),
        });
        n.notify_webhook_with_extras(event("egress_started"), &[e])
            .unwrap();
        n.stop(true).await;
        let delivered = server.rx.try_recv().unwrap();
        assert_eq!(delivered.path, "/extra");
        assert_eq!(
            verify(&delivered, "key", "secret")["event"],
            "egress_started"
        );
        assert!(server.rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn unknown_override_stops_later_extras_without_undoing_prior_static_and_extra_items() {
        let mut server = server().await;
        let n = WebhookNotifier::new(
            &config(vec![format!("{}/global", server.url)], "key"),
            keys(),
            false,
        )
        .unwrap();
        let extras = [
            extra(format!("{}/first", server.url), "other"),
            extra(format!("{}/unknown", server.url), "unknown"),
            extra(format!("{}/never", server.url), "key"),
        ];
        let error = n
            .notify_webhook_with_extras(event("egress_started"), &extras)
            .unwrap_err();
        assert_eq!(error.to_string(), "no secret for provided signing key");
        n.stop(true).await;
        let mut captures = Vec::new();
        while let Ok(c) = server.rx.try_recv() {
            captures.push(c);
        }
        let mut paths = captures.iter().map(|c| c.path.as_str()).collect::<Vec<_>>();
        paths.sort();
        assert_eq!(paths, ["/first", "/global"]);
        assert_eq!(captures[0].body, captures[1].body);
        for c in captures {
            if c.path == "/first" {
                verify(&c, "other", "other-secret");
            } else {
                verify(&c, "key", "secret");
            }
        }
    }
    #[test]
    fn no_global_urls_can_capture_runtime_lazily_and_explicit_extra_key_without_a_default() {
        // Construct outside any Tokio runtime; no task is created yet.
        let n = WebhookNotifier::new(&config(Vec::new(), ""), keys(), false).unwrap();
        assert!(n.inner.runtime.lock().unwrap().is_none());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut server = server().await;
            let error = n
                .notify_webhook_with_extras(
                    event("egress_started"),
                    &[extra(format!("{}/default", server.url), "")],
                )
                .unwrap_err();
            assert_eq!(error.to_string(), "no singing key or secret was provided");
            n.notify_webhook_with_extras(
                event("egress_started"),
                &[extra(format!("{}/explicit", server.url), "other")],
            )
            .unwrap();
            assert!(n.inner.runtime.lock().unwrap().is_some());
            n.stop(true).await;
            let c = server.rx.try_recv().unwrap();
            assert_eq!(c.path, "/explicit");
            verify(&c, "other", "other-secret");
            assert!(server.rx.try_recv().is_err());
        });
    }
    #[tokio::test]
    async fn forced_stop_cancels_extra_queue_and_rejects_later_default_key_items() {
        let mut server = server().await;
        let n = WebhookNotifier::new(&config(Vec::new(), "key"), keys(), false).unwrap();
        n.notify_webhook_with_extras(
            event("egress_started"),
            &[extra(format!("{}/extra", server.url), "")],
        )
        .unwrap();
        // Current-thread executor has not yielded to the delivery worker.
        n.stop(false).await;
        assert_eq!(n.delivered_count(), 0);
        assert!(server.rx.try_recv().is_err());
        assert!(n.inner.extra.queues.lock().unwrap().is_empty());
        assert!(matches!(
            n.notify_webhook_with_extras(
                event("egress_ended"),
                &[extra(format!("{}/late", server.url), "")]
            ),
            Err(WebhookError::Closed)
        ));
    }
    #[tokio::test]
    async fn extra_idle_retirement_takes_a_racing_item_before_removing_the_resource_entry() {
        let n = WebhookNotifier::new(&config(Vec::new(), "key"), keys(), false).unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        let key = "EG_order";
        n.inner.extra.queues.lock().unwrap().insert(
            key.into(),
            QueueEntry {
                id: 77,
                tx: tx.clone(),
                worker: tokio::spawn(async {}),
            },
        );
        assert!(
            tx.try_send(Item {
                url: "http://fixture.invalid/".into(),
                api_key: "key".into(),
                event: "egress_started".into(),
                body: Bytes::new(),
                enqueued: Instant::now(),
            })
            .is_ok()
        );
        let item = n
            .inner
            .retire_or_take(&n.inner.extra, key, 77, &mut rx)
            .unwrap();
        assert_eq!(item.api_key, "key");
        assert_eq!(n.inner.extra.queues.lock().unwrap().len(), 1);
        assert!(
            n.inner
                .retire_or_take(&n.inner.extra, key, 77, &mut rx)
                .is_none()
        );
        assert!(n.inner.extra.queues.lock().unwrap().is_empty());
        n.stop(true).await;
    }
}
