//! Reads, and the version they were taken at.

use crate::error::{Fence, Rejected, errno};
use crate::{Bulk, Replicated, Version};
use librados::{ObjectStat, ReadOp};
use std::collections::BTreeMap;

/// One page of omap entries and the object version they were read at.
#[derive(Debug)]
pub struct Page {
    /// Key/value pairs in key order.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// The OSD stopped before the end of the prefix. Any of three caps does it: `limit`,
    /// `osd_max_omap_bytes_per_request`, or `osd_max_omap_entries_per_request`, which clamps
    /// `limit` down (`PrimaryLogPG.cc:7957-7959`, `7983-7985`). Read on from the last key.
    pub more: bool,
    pub version: Version,
}

impl Replicated {
    /// Reads at most `limit` entries whose key starts with `prefix`, after `start_after`.
    ///
    /// `at` pins the object: the `assert_version` rides in the same operation as the read, so
    /// a page is either from that exact version of the object or is
    /// [`Fenced`](Rejected::Fenced). Replay reads the whole object at the version its head
    /// read saw.
    ///
    /// `prefix` and `start_after` reach the OSD as C strings (`librados_c.cc:4411-4412`), so
    /// neither can hold an interior NUL byte.
    pub fn omap_page(
        &self,
        oid: &str,
        prefix: &str,
        start_after: &str,
        limit: u32,
        at: Option<Version>,
    ) -> Result<Page, Rejected> {
        let mut op = ReadOp::new();
        if let Some(version) = at {
            op.assert_version(version.0);
        }
        let vals = op.omap_get_vals(Some(start_after), Some(prefix), limit as u64)?;
        let mut results = op.operate(&self.io, oid).map_err(|err| fenced(err, at))?;
        let version = Version(self.io.last_version());
        let page = results.take(vals)?;
        Ok(Page {
            entries: page.entries.into_iter().collect(),
            more: page.more,
            version,
        })
    }

    /// Reads the named keys. Keys with no value are absent from the map.
    // Returning (BTreeMap<Vec<u8>, Vec<u8>>, Version) directly keeps the types explicit without
    // an extra wrapper struct.
    #[allow(clippy::type_complexity)]
    pub fn omap_get(
        &self,
        oid: &str,
        keys: &[&[u8]],
    ) -> Result<(BTreeMap<Vec<u8>, Vec<u8>>, Version), Rejected> {
        let mut op = ReadOp::new();
        let vals = op.omap_get_vals_by_keys(keys);
        let mut results = op.operate(&self.io, oid)?;
        let version = Version(self.io.last_version());
        Ok((results.take(vals)?.entries, version))
    }
}

/// A read op carrying an `assert_version` reports the stale object the same way a write does.
fn fenced(err: librados::RadosError, at: Option<Version>) -> Rejected {
    match (err, at) {
        (librados::RadosError::Rados(errno::ERANGE | errno::EOVERFLOW), Some(expected)) => {
            Rejected::Fenced(Fence::Version { expected })
        }
        (err, _) => err.into(),
    }
}

impl Bulk {
    /// Reads at most `len` bytes from `offset`, truncated to what the object holds.
    pub fn read(&self, oid: &str, offset: u64, len: usize) -> Result<Vec<u8>, Rejected> {
        let mut buf = vec![0u8; len];
        let read = self.io.read(oid, &mut buf, offset)?;
        buf.truncate(read);
        Ok(buf)
    }

    pub fn stat(&self, oid: &str) -> Result<ObjectStat, Rejected> {
        Ok(self.io.stat(oid)?)
    }
}
