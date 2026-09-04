//! Stacking the guards of a [`Write`](crate::Write) onto a write op, and reading back which
//! one rejected it.

use crate::error::{errno, Fence, Rejected};
use crate::Version;
use librados::{CmpHandle, WriteError, WriteOp, CMPXATTR_OP_EQ};

/// A comparison placed ahead of the mutations in the same operation. Every guard has to hold
/// or no mutation is applied.
pub enum Guard<'a> {
    /// The object is at this `user_version`.
    Version(Version),
    /// The object exists.
    Exists,
    /// The omap key holds this value. On an object that exists, a missing key compares equal
    /// to `b""` (`PrimaryLogPG.cc:8088-8090`). On an object that does not, `omap_cmp` returns
    /// `-ENOENT` before it compares anything (`PrimaryLogPG.cc:8047-8050`), so the write is
    /// [`Rejected::NotFound`](crate::Rejected::NotFound) and names no guard: an empty-value
    /// guard does not stand in for "the object is not there yet".
    OmapEq(&'a [u8], &'a [u8]),
    /// The xattr holds this value.
    XattrEq(&'a str, &'a [u8]),
}

/// What the guards of one operation left behind, so that a failure can name the guard that
/// caused it.
#[derive(Default)]
pub(crate) struct Guards<'a> {
    /// The omap comparisons in the order they were pushed, each with the key it compares.
    omap: Vec<(CmpHandle, &'a [u8])>,
    xattr: Option<&'a str>,
    version: Option<Version>,
    exists: bool,
}

/// Pushes `guards` onto `op` in the order given.
///
/// A `Write` holding two or more `XattrEq` is rejected here, before the op is sent:
/// `rados_write_op_cmpxattr` has no `prval` out-parameter (`librados.h:2916-2920`), so with
/// more than one xattr comparison a `-ECANCELED` cannot say which one failed, and
/// `Fence::Xattr` would have to name a guard that may have held.
pub(crate) fn push<'a>(op: &mut WriteOp, guards: &[Guard<'a>]) -> Result<Guards<'a>, Rejected> {
    let mut seen = Guards::default();
    for guard in guards {
        match guard {
            Guard::Version(version) => {
                op.assert_version(version.0);
                seen.version = Some(*version);
            }
            Guard::Exists => {
                op.assert_exists();
                seen.exists = true;
            }
            Guard::OmapEq(key, value) => {
                seen.omap
                    .push((op.omap_cmp(key, CMPXATTR_OP_EQ, value), key));
            }
            Guard::XattrEq(name, value) => {
                if seen.xattr.is_some() {
                    return Err(Rejected::Rados(errno::EINVAL));
                }
                op.cmpxattr(name, CMPXATTR_OP_EQ, value)?;
                seen.xattr = Some(name);
            }
        }
    }
    Ok(seen)
}

impl Guards<'_> {
    /// Maps a failed `operate_report` onto the guard that rejected it.
    pub(crate) fn reject(&self, err: WriteError) -> Rejected {
        match err.error {
            librados::RadosError::Rados(errno::ECANCELED) => self.canceled(err.failed_cmp),
            librados::RadosError::Rados(e @ (errno::ERANGE | errno::EOVERFLOW)) => {
                self.version.map_or(Rejected::Rados(e), |expected| {
                    Rejected::Fenced(Fence::Version { expected })
                })
            }
            librados::RadosError::NotFound if self.exists => Rejected::Fenced(Fence::Exists),
            other => other.into(),
        }
    }

    /// The OSD stops at the first failing sub-op and leaves the later `rval`s at 0
    /// (`PrimaryLogPG.cc:8379-8386`), so at most one `omap_cmp` reports a non-zero `prval`.
    /// With none reporting, the comparison that failed is the `cmpxattr`, of which `push`
    /// admits at most one.
    fn canceled(&self, failed: Option<CmpHandle>) -> Rejected {
        let key = failed.and_then(|h| self.omap.iter().find(|(handle, _)| *handle == h));
        match (key, self.xattr) {
            (Some((_, key)), _) => Rejected::Fenced(Fence::Omap { key: key.to_vec() }),
            (None, Some(name)) => Rejected::Fenced(Fence::Xattr {
                name: name.to_owned(),
            }),
            (None, None) => Rejected::Rados(errno::ECANCELED),
        }
    }
}
