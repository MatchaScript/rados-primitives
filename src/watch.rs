//! Watch and notify, passed through to librados.

use crate::Replicated;
use crate::error::Rejected;
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
    /// The [`Watch`] owns a separate ioctx, so unregistering it cannot replace the
    /// `last_version` used by reads on this handle.
    pub fn watch(
        &self,
        oid: &str,
        on_notify: Box<dyn FnMut(Notification) -> Vec<u8> + Send>,
        on_error: Box<dyn FnMut(i32) + Send>,
    ) -> Result<Watch, Rejected> {
        let io = self.rados.create_ioctx(self.io.pool_name())?;
        Ok(io.watch(oid, on_notify, on_error)?)
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
