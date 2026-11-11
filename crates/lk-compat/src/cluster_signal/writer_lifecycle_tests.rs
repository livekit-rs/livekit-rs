//! Actual process-local PSRPC streams and current-thread runtime liveness.
use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::TransportLimits,
    wire::{CompressionPolicy, WireLimits},
};
fn relay_config() -> RelayConfig {
    RelayConfig {
        retry_timeout: Duration::from_secs(10),
        min_retry_interval: Duration::from_millis(20),
        max_retry_interval: Duration::from_millis(80),
        stream_buffer_size: 4,
        connect_attempts: 1,
    }
}
async fn pair() -> (
    RpcStream<rpc::RelaySignalResponse, rpc::RelaySignalRequest>,
    RpcStream<rpc::RelaySignalRequest, rpc::RelaySignalResponse>,
    stream::StreamListener<rpc::RelaySignalResponse, rpc::RelaySignalRequest>,
) {
    let bus: crate::service_bus::rpc_transport::RpcBus = LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(2),
    })
    .unwrap()
    .into();
    let method = MethodSpec {
        service: "WriterLifecycle".into(),
        method: "RelaySignal".into(),
        topics: vec![rtc_core::ids::new_sid("topic_")],
        require_claim: false,
        queue: false,
    };
    let config = StreamConfig {
        timeout: Duration::from_secs(2),
        channel_size: 1,
        max_streams: 8,
    };
    let mut listener =
        stream::register_stream::<rpc::RelaySignalResponse, rpc::RelaySignalRequest>(
            bus.clone(),
            method.clone(),
            config,
            None,
        )
        .await
        .unwrap();
    let opening = tokio::spawn(async move {
        stream::open_stream::<rpc::RelaySignalRequest, rpc::RelaySignalResponse>(
            bus,
            &method,
            config,
            CallOptions::new(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    });
    let server = listener.accept().await.unwrap();
    let client = opening.await.unwrap();
    (server, client, listener)
}
#[tokio::test(flavor = "current_thread")]
async fn closed_stream_writer_exits_without_spinning_through_retry_budget() {
    let (mut server, mut client, mut listener) = pair().await;
    client.close(None).await.unwrap();
    assert!(server.recv().await.is_none());
    assert!(server.error().is_some());
    let (send, receive) = mpsc::channel(4);
    send.send(pb::SignalResponse::default().encode_to_vec())
        .await
        .unwrap();
    let started = Instant::now();
    let heartbeat = tokio::spawn(async {
        tokio::task::yield_now().await;
        true
    });
    let result = write_batches(
        server.sender(),
        receive,
        relay_config(),
        Cancellation::default(),
        true,
    )
    .await;
    assert!(result.is_err(), "terminal cause is retained");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "terminal stream must not consume original ten-second retry window"
    );
    assert!(heartbeat.await.unwrap());
    listener.close().await;
}
#[tokio::test(flavor = "current_thread")]
async fn real_unacked_message_preserves_retry_wait_and_remains_cancelable() {
    let (mut server, _client, mut listener) = pair().await;
    // Fill the real peer application's size-one receive channel. Its reader
    // cannot admit/ACK a subsequent stream message until the app consumes it.
    server
        .send(&rpc::RelaySignalResponse::default())
        .await
        .unwrap();
    let (send, receive) = mpsc::channel(4);
    send.send(pb::SignalResponse::default().encode_to_vec())
        .await
        .unwrap();
    let stop = Cancellation::default();
    let cancel = stop.clone();
    let heartbeat = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        cancel.cancel();
    });
    let started = Instant::now();
    assert!(
        write_batches(server.sender(), receive, relay_config(), stop, true)
            .await
            .is_ok()
    );
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "live no-ACK path must not be treated as a closed stream"
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "current-thread cancellation/heartbeat must remain schedulable"
    );
    heartbeat.await.unwrap();
    server.close(None).await.unwrap();
    listener.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn relay_request_close_drains_accepted_leave_before_controller_sink_finishes() {
    let (mut owner, controller, mut listener) = pair().await;
    let mut session = remote_session(controller, relay_config(), Cancellation::default());
    let ping = pb::SignalRequest {
        message: Some(pb::signal_request::Message::Ping(7)),
    }
    .encode_to_vec();
    let leave = pb::SignalRequest {
        message: Some(pb::signal_request::Message::Leave(
            pb::LeaveRequest::default(),
        )),
    }
    .encode_to_vec();
    assert!(session.send(ping.clone()).await);
    assert!(session.send(leave.clone()).await);
    session.begin_request_close();
    assert!(!session.send(ping.clone()).await);

    // The actual LocalTransport relay must deliver all queued requests and
    // application EOF while the controller has not finished closing its sink.
    // In particular, the owner cannot wait on RemoteSession::close() for Leave.
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        let mut expected = 0;
        loop {
            let frame = owner.recv().await.expect("actual relay request batch");
            received.extend(
                codec::read_batch(
                    "rpc.RelaySignalRequest",
                    "requests",
                    &frame.raw,
                    frame.message.seq,
                    &mut expected,
                )
                .unwrap(),
            );
            if frame.message.close {
                return received;
            }
        }
    })
    .await
    .expect("owner receives terminal request batch before sink completion");
    assert_eq!(received, vec![ping, leave]);
    session.close().await;
    listener.close().await;
}
