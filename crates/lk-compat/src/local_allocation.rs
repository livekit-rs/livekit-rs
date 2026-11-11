//! StandardRoomAllocator.SelectRoomNode for the fixed in-process LocalRouter.
//! The local router always returns this node; absence of Redis does not bypass
//! configured node capacity or the stale-node selector fallback.
use crate::{
    room_control::{
        catalog::NodeSelector,
        registry::node_available,
        selector::{Config, ConfiguredSelector},
    },
    routing::LocalNode,
    service_bus::rpc::RpcError,
};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct LocalAllocation {
    node: Arc<LocalNode>,
    selector: ConfiguredSelector,
}
impl LocalAllocation {
    pub(crate) fn new(node: Arc<LocalNode>, config: Config) -> Result<Self, RpcError> {
        Ok(Self {
            node,
            selector: ConfiguredSelector::new(config).map_err(RpcError::from)?,
        })
    }
    /// ValidateConnectRequest checks limits even if the sampled node is stale;
    /// the later allocator owns availability/selection and its different errors.
    pub(crate) fn at_capacity(&self) -> bool {
        self.selector.at_capacity(&self.node.snapshot())
    }

    pub(crate) fn check(&self) -> Result<(), RpcError> {
        self.check_at(rtc_core::util::unix_s())
    }
    fn check_at(&self, now_unix: i64) -> Result<(), RpcError> {
        let current = self.node.snapshot();
        if node_available(&current, now_unix) {
            if self.selector.at_capacity(&current) {
                return Err(RpcError::new(
                    "unknown",
                    "reached configured limit for node",
                ));
            }
            return Ok(());
        }
        // Source falls back to selector.SelectNode(LocalRouter.ListNodes()).
        // Stale/missing candidates must fail instead of silently admitting.
        self.selector
            .select_at(&[current], now_unix, &mut |_| 0)
            .map(|_| ())
            .map_err(RpcError::from)
    }
}

#[cfg(test)]
#[path = "local_allocation/tests.rs"]
mod tests;
