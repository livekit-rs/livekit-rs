//! Type-only future-size diagnostic: closures are never called, so no giant
//! reference future is first constructed on the diagnostic thread's stack.
use super::*;
fn size<A: 'static, F, Fut>(_: F) -> usize
where
    F: FnOnce(&'static A) -> Fut,
    Fut: std::future::Future,
{
    std::mem::size_of::<Fut>()
}
#[test]
fn production_rpc_future_sizes_without_constructing_the_futures() {
    let dispatch = size(|state: &'static AppState| {
        call(state, None, "ForwardParticipant", Codec::Json, b"{}")
    });
    let perform =
        size(|state: &'static AppState| perform_rpc(state, None, pb::PerformRpcRequest::default()));
    let forward = size(|state: &'static AppState| {
        forward_or_move::<pb::ForwardParticipantResponse>(
            state,
            None,
            "source".into(),
            "ghost".into(),
            "dest".into(),
            false,
        )
    });
    let local = size(
        |service: &'static crate::local_participant::LocalParticipantRpc| {
            service.perform(pb::PerformRpcRequest::default())
        },
    );
    eprintln!(
        "RPC_FUTURE_SIZES dispatch={dispatch} perform_rpc={perform} forward={forward} local_participant={local}"
    );
    // These are measurements, not fake business acceptance. The unchanged
    // formerly overflowing default-stack test plus full lib is the real gate.
}
