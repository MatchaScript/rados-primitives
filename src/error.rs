//! The rejection type and errno mapping.

use crate::Version;
use librados::RadosError;
use thiserror::Error;

/// The errno values this crate names. They are the Linux numbers
/// (`asm-generic/errno-base.h`, `asm-generic/errno.h`); the crate depends on `librados` and
/// `thiserror` only, so it does not take `libc` for six constants.
pub(crate) mod errno {
    pub(crate) const EACCES: i32 = 13;
    pub(crate) const EBUSY: i32 = 16;
    pub(crate) const EINVAL: i32 = 22;
    pub(crate) const ERANGE: i32 = 34;
    pub(crate) const EOVERFLOW: i32 = 75;
    pub(crate) const ECANCELED: i32 = 125;
}

/// Why an operation did not happen.
///
/// `Fenced`, `Exists` and `NotFound` are the OSD's decision: it stops the op at the first
/// sub-op that fails (`PrimaryLogPG.cc:8379-8386`), so the write applied nothing. `Rados`
/// carries every other errno, including one raised while the outcome is unknown — with
/// `rados_osd_op_timeout` set, Objecter cancels an in-flight op client-side with `-ETIMEDOUT`
/// (`Objecter.cc:2478`) while the OSD may still commit it. Retries, timeouts and the
/// reconciliation of an unknown outcome are the caller's, through its own request outcome.
#[derive(Debug, Error)]
pub enum Rejected {
    /// A guard did not hold, naming the guard.
    #[error("fenced: {0}")]
    Fenced(Fence),

    /// `-EEXIST` from an exclusive create, or from taking a lease this client already holds.
    #[error("object already exists")]
    Exists,

    /// `-ENOENT` from a read, or from a write with no `Guard::Exists`.
    #[error("object not found")]
    NotFound,

    /// Any other errno, including `EBLOCKLISTED` (108). Whether the write applied is not
    /// decided here.
    #[error("librados: {}", std::io::Error::from_raw_os_error(*.0))]
    Rados(i32),
}

/// The guard that rejected the operation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Fence {
    /// `assert_version`: `-ERANGE` (the object moved past `expected`) or `-EOVERFLOW`.
    #[error("object is not at version {}", .expected.0)]
    Version { expected: Version },

    /// `assert_exists` on an object that does not exist.
    #[error("object does not exist")]
    Exists,

    /// `omap_cmp`: the value under `key` is not the one the guard named.
    #[error("omap key {} does not hold the expected value", String::from_utf8_lossy(.key))]
    Omap { key: Vec<u8> },

    /// `cmpxattr`: the xattr `name` does not hold the value the guard named.
    #[error("xattr {name} does not hold the expected value")]
    Xattr { name: String },
}

impl From<RadosError> for Rejected {
    fn from(err: RadosError) -> Self {
        match err {
            RadosError::NotFound => Rejected::NotFound,
            RadosError::AlreadyExists => Rejected::Exists,
            RadosError::Rados(errno) => Rejected::Rados(errno),
            // A NUL byte in an oid, an xattr name or an omap bound: librados passes those as C
            // strings, so the op is rejected before it is sent.
            RadosError::Nul(_) => Rejected::Rados(errno::EINVAL),
        }
    }
}
