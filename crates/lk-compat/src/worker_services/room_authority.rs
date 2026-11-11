//! Late binding avoids retaining a second room authority or a service graph
//! cycle: RoomControl owns the assembled AppState/worker graph; worker room
//! consumers hold only a weak link back to that exact control authority.
use crate::{room_control::RoomControl, service_bus::rpc::RpcError};
use std::sync::{Arc, RwLock, Weak};
#[derive(Default)]
pub(super) struct RoomAuthority {
    control: RwLock<Option<Weak<RoomControl>>>,
}
impl RoomAuthority {
    pub(super) fn bind(&self, control: &Arc<RoomControl>) -> Result<(), RpcError> {
        let mut slot = self
            .control
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let weak = Arc::downgrade(control);
        if let Some(previous) = slot.as_ref() {
            if previous.ptr_eq(&weak) {
                return Ok(());
            }
            return Err(RpcError::new(
                "already_exists",
                "worker room authority already bound",
            ));
        }
        *slot = Some(weak);
        Ok(())
    }
    pub(super) fn get(&self) -> Result<Option<Arc<RoomControl>>, RpcError> {
        let slot = self
            .control
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            None => Ok(None),
            Some(control) => control
                .upgrade()
                .map(Some)
                .ok_or_else(|| RpcError::new("unavailable", "worker room authority stopped")),
        }
    }
}
