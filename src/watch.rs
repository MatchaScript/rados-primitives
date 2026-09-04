//! Watch and notify, passed through to librados.

use crate::error::Rejected;
use crate::Replicated;
use librados::{Notification, Watch};
use std::time::Duration;

impl Replicated {
    /// Registers a watch on `oid`. Dropping the returned [`Watch`] unregisters it.
    ///
    /// Both callbacks run on a librados thread and are called as they arrive, without a queue
    /// in between. `on_notify` returns the bytes to ack with; a watcher that never acks holds
    /// the notifier for its whole timeout, so librados acks every notification.
    /// `on_error` receives `-ENOTCONN` when the watch is lost; re-registering is up to the caller
    /// since this crate does not retry.
    ///
    /// The [`Watch`] holds a clone of this handle's ioctx, and unregistering ends with
    /// `set_sync_op_version` on it (`IoCtxImpl.cc:1785-1804`), which is the slot
    /// `omap_page` and `omap_get` read their [`Version`](crate::Version) from. `Watch` is
    /// `Send`, so drop it on the thread that owns this `Replicated`: a drop racing a read on
    /// that thread hands the read the unwatch's version. This is why `Replicated` is not `Sync`:
    /// two threads sharing one handle would race on the internal version slot, which the marker
    /// cannot state for a handle it does not own.
    pub fn watch(
        &self,
        oid: &str,
        on_notify: Box<dyn FnMut(Notification) -> Vec<u8> + Send>,
        on_error: Box<dyn FnMut(i32) + Send>,
    ) -> Result<Watch, Rejected> {
        Ok(self.io.watch(oid, on_notify, on_error)?)
    }

    /// Notifies every watcher of `oid` and returns once they have all acked or `timeout` has
    /// passed. The result pairs each acking watcher's gid with the bytes its `on_notify`
    /// returned; a watcher that did not ack in time is simply absent.
    ///
    /// `timeout` is rounded up to whole seconds: librados divides the millisecond value by
    /// 1000, and replaces a 0 with the client's `client_notify_timeout` (10s by default)
    /// before the op is sent (`IoCtxImpl.cc:1849-1851`), so `Duration::ZERO` is that config
    /// value rather than any OSD-side default.
    pub fn notify(
        &self,
        oid: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<(u64, Vec<u8>)>, Rejected> {
        let response = self.io.notify(oid, payload, timeout)?;
        Ok(response
            .acks
            .into_iter()
            .map(|ack| (ack.notifier_id, ack.payload))
            .collect())
    }
}
