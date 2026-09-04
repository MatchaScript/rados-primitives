//! Guarded single-object operations on RADOS.
//!
//! One rule shapes the API: a comparison and the write it protects travel in the same
//! operation. There is no entry point that only compares, so no caller can build the
//! TOCTOU between the two. On top of that rule this crate places the leases, the
//! watch and the writer election that single-object operations are enough to build.
//!
//! Pools are opened as one of two types. [`Replicated`] carries omap, append and `cls_lock`;
//! [`Bulk`] carries only write-once and read, which is all an erasure-coded pool answers.
//!
//! Everything here is synchronous, and nothing retries: a [`Rejected`] operation did not
//! happen, and what to do about it belongs to the caller.

mod election;
mod error;
mod guard;
mod lock;
mod read;
mod watch;
mod write;

pub use election::{Election, epoch_bytes};
pub use error::{Fence, Rejected};
pub use guard::Guard;
pub use read::Page;
pub use write::{Mutation, Write};

// The caller receives these librados types unchanged.
pub use librados::{Locker, Notification, ObjectStat, Rados, Watch};

use std::marker::PhantomData;

/// Makes the pool handles `Send` but not `Sync`.
///
/// `IoCtx::last_version` reports the version of whichever operation finished last on the
/// ioctx, so two threads sharing one handle would read each other's versions. Each thread
/// opens its own.
type NotSync = PhantomData<std::cell::Cell<()>>;

/// The `user_version` of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(pub u64);

/// A replicated pool: omap, append and `cls_lock` are available.
pub struct Replicated {
    pub(crate) io: librados::IoCtx,
    /// `acquire` blocklists the previous holder, which is a cluster-wide operation rather
    /// than an ioctx one.
    pub(crate) rados: Rados,
    _not_sync: NotSync,
}

/// A pool used only for write-once objects and reads. May be erasure-coded.
pub struct Bulk {
    pub(crate) io: librados::IoCtx,
    _not_sync: NotSync,
}

impl Replicated {
    pub fn open(rados: &Rados, pool: &str) -> Result<Replicated, Rejected> {
        Ok(Replicated {
            io: rados.create_ioctx(pool)?,
            rados: rados.clone(),
            _not_sync: PhantomData,
        })
    }
}

impl Bulk {
    pub fn open(rados: &Rados, pool: &str) -> Result<Bulk, Rejected> {
        Ok(Bulk {
            io: rados.create_ioctx(pool)?,
            _not_sync: PhantomData,
        })
    }
}
