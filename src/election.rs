//! Writer election.

use crate::error::{errno, Rejected};
use crate::{Guard, Mutation, Replicated, Write};
use librados::RadosError;
use std::time::Duration;

/// Where an election keeps its lease and its epoch. All four live on one object, so the epoch
/// bump is a single-object write.
pub struct Election<'a> {
    pub oid: &'a str,
    /// The `cls_lock` name.
    pub name: &'a str,
    /// The omap key holding the epoch.
    pub epoch_key: &'a [u8],
    /// The omap key holding the current holder's address.
    pub holder_key: &'a [u8],
}

/// The omap encoding of an epoch: `u64` big-endian, 8 bytes. `acquire` writes the epoch, so
/// this crate owns the encoding; callers compare against it in their own guards.
pub fn epoch_bytes(epoch: u64) -> [u8; 8] {
    epoch.to_be_bytes()
}

impl Replicated {
    /// Becomes the writer, returning the new epoch, or `None` while someone else holds the
    /// lease.
    ///
    /// The four steps run in this order so that the fence is durable before the new writer
    /// writes anything, as `fail_mds_gid` does:
    ///
    /// 1. take the lease, renewing it if this client already holds it;
    /// 2. read this client's own address out of the locker entry, and the epoch and the
    ///    previous holder out of the omap;
    /// 3. blocklist the previous holder and wait for the new OSDMap;
    /// 4. write epoch `E+1` and this client's address, guarded on the epoch that was read.
    ///
    /// A failure after step 1 releases the lease. The caller guards its own writes with the
    /// returned epoch; that guard, not the lease, is what stops the previous writer,
    /// so an environment where step 3 is skipped is still fenced.
    pub fn acquire(
        &self,
        e: &Election<'_>,
        cookie: &str,
        ttl: Duration,
    ) -> Result<Option<u64>, Rejected> {
        match self.lock_exclusive(e.oid, e.name, cookie, "", ttl) {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            // Already held under this cookie (`-EEXIST`): extend it and go on. A renew that
            // finds nothing leaves no locker entry, which step 2 reads as a lost lease.
            Err(Rejected::Exists) => {
                self.renew(e.oid, e.name, cookie, "", ttl)?;
            }
            Err(err) => return Err(err),
        }
        let acquired = self.take_epoch(e, cookie, ttl);
        if acquired.is_err() {
            let _ = self.unlock(e.oid, e.name, cookie);
        }
        acquired
    }

    /// Steps 2 to 4, with the lease held.
    fn take_epoch(
        &self,
        e: &Election<'_>,
        cookie: &str,
        ttl: Duration,
    ) -> Result<Option<u64>, Rejected> {
        // The locker entry carries the address the OSD sees, in the form
        // `rados_blocklist_add` parses (`cls_lock.cc:228-233`). `rados_getaddrs` reports an
        // addrvec instead, which that parser rejects (`RadosClient.cc:781-794`).
        let own_addr = self
            .lockers(e.oid, e.name)?
            .into_iter()
            .find(|locker| locker.cookie == cookie)
            .map(|locker| locker.addr);
        let Some(own_addr) = own_addr else {
            // The lease expired between taking it and reading it back.
            return Ok(None);
        };

        let (mut state, _) = self.omap_get(e.oid, &[e.epoch_key, e.holder_key])?;
        let raw_epoch = state.remove(e.epoch_key).unwrap_or_default();
        let holder = state.remove(e.holder_key).unwrap_or_default();
        let epoch = decode_epoch(&raw_epoch)?;

        if !holder.is_empty() && holder != own_addr.as_bytes() {
            self.fence(&holder, ttl)?;
        }

        // The guard is the raw value that was read, so a missing epoch key guards on `b""`:
        // `omap_cmp` compares a missing key as empty (`PrimaryLogPG.cc:8088-8090`). Step 1
        // created the object with the `cls_lock` xattr, so the `-ENOENT` `omap_cmp` returns
        // on a missing object (`PrimaryLogPG.cc:8047-8050`) cannot arrive here.
        let next = epoch_bytes(epoch + 1);
        self.write(
            e.oid,
            Write {
                guards: vec![Guard::OmapEq(e.epoch_key, &raw_epoch)],
                mutations: vec![Mutation::OmapSet(&[
                    (e.epoch_key, &next),
                    (e.holder_key, own_addr.as_bytes()),
                ])],
            },
        )?;
        Ok(Some(epoch + 1))
    }

    /// Blocklists the previous holder for `ttl` and waits until this client has the OSDMap
    /// that carries it, so the fence is in effect before the caller writes.
    fn fence(&self, holder: &[u8], ttl: Duration) -> Result<(), Rejected> {
        let addr = std::str::from_utf8(holder).map_err(|_| Rejected::Rados(errno::EINVAL))?;
        match self.rados.blocklist_add(addr, ttl) {
            // No mon cap for `osd blocklist add`: the monitor refuses a command the session's
            // caps do not allow with `reply_command(op, -EACCES, "access denied")`
            // (`Monitor.cc:3752-3760`), before the command body runs. The epoch guard written
            // in step 4 still fences the previous writer's writes.
            Err(RadosError::Rados(errno::EACCES)) => return Ok(()),
            other => other?,
        }
        Ok(self.rados.wait_for_latest_osdmap()?)
    }
}

/// Decodes what `epoch_bytes` writes. A missing key reads as the empty value, which is
/// epoch 0.
fn decode_epoch(raw: &[u8]) -> Result<u64, Rejected> {
    if raw.is_empty() {
        return Ok(0);
    }
    let bytes: [u8; 8] = raw.try_into().map_err(|_| Rejected::Rados(errno::EINVAL))?;
    Ok(u64::from_be_bytes(bytes))
}
