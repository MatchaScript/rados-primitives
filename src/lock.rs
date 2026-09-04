//! `cls_lock` leases.
//!
//! A lease decides who should be the writer. It does not fence a write on its own: `cls_lock`
//! expires on the OSD's wall clock, and fencing requires a write guard such as an epoch comparison.

use crate::Replicated;
use crate::error::{Rejected, errno};
use librados::{LOCK_FLAG_MUST_RENEW, Locker, RadosError};
use std::time::Duration;

impl Replicated {
    /// Takes the lease for `ttl`. `Ok(false)` means another holder has it.
    ///
    /// Taking a lease this client already holds under the same cookie is
    /// [`Rejected::Exists`], not a renewal; [`renew`](Replicated::renew) renews.
    pub fn lock_exclusive(
        &self,
        oid: &str,
        name: &str,
        cookie: &str,
        desc: &str,
        ttl: Duration,
    ) -> Result<bool, Rejected> {
        match self
            .io
            .lock_exclusive(oid, name, cookie, desc, Some(ttl), 0)
        {
            Ok(()) => Ok(true),
            Err(RadosError::Rados(errno::EBUSY)) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Extends a lease this client holds to `now + ttl`. `Ok(false)` means the lease is not
    /// held any more, which `MUST_RENEW` reports as `-ENOENT` (`cls_lock.cc:145-217`).
    pub fn renew(
        &self,
        oid: &str,
        name: &str,
        cookie: &str,
        desc: &str,
        ttl: Duration,
    ) -> Result<bool, Rejected> {
        match self
            .io
            .lock_exclusive(oid, name, cookie, desc, Some(ttl), LOCK_FLAG_MUST_RENEW)
        {
            Ok(()) => Ok(true),
            Err(RadosError::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub fn unlock(&self, oid: &str, name: &str, cookie: &str) -> Result<(), Rejected> {
        Ok(self.io.unlock(oid, name, cookie)?)
    }

    /// The holders of the lease. `Locker::addr` is the address the OSD sees, which is what
    /// `Rados::blocklist_add` takes.
    pub fn lockers(&self, oid: &str, name: &str) -> Result<Vec<Locker>, Rejected> {
        Ok(self.io.list_lockers(oid, name)?.lockers)
    }

    /// Removes another client's hold. `cls_lock` does not authenticate the caller, so
    /// mutually distrusting writers restrict this through pool or cap permissions.
    pub fn break_lock(
        &self,
        oid: &str,
        name: &str,
        client: &str,
        cookie: &str,
    ) -> Result<(), Rejected> {
        Ok(self.io.break_lock(oid, name, client, cookie)?)
    }
}
