//! `Replicated::write` and `Bulk::write_once`.

use crate::error::Rejected;
use crate::guard::{self, Guard};
use crate::{Bulk, Replicated};
use librados::WriteOp;

/// One step of a compound write. The steps are applied in the order given, all of them or
/// none: a single RADOS operation is one PGTransaction.
pub enum Mutation<'a> {
    CreateExclusive,
    WriteFull(&'a [u8]),
    Append(&'a [u8]),
    OmapSet(&'a [(&'a [u8], &'a [u8])]),
    OmapRemove(&'a [&'a [u8]]),
    /// Removes the keys in `[begin, end)`.
    OmapRemoveRange {
        begin: &'a [u8],
        end: &'a [u8],
    },
    SetXattr(&'a str, &'a [u8]),
    Remove,
}

/// The guards and the mutations they protect, in one operation.
pub struct Write<'a> {
    pub guards: Vec<Guard<'a>>,
    pub mutations: Vec<Mutation<'a>>,
}

impl Replicated {
    /// Runs `w` as one operation: the guards first, then the mutations, each in the order
    /// given. If a guard does not hold, no mutation is applied and the returned
    /// [`Rejected::Fenced`] names it.
    ///
    /// The version of the written object is not returned.
    pub fn write(&self, oid: &str, w: Write<'_>) -> Result<(), Rejected> {
        let mut op = WriteOp::new();
        let guards = guard::push(&mut op, &w.guards)?;
        for mutation in &w.mutations {
            push(&mut op, mutation)?;
        }
        op.operate_report(&self.io, oid)
            .map_err(|err| guards.reject(err))
    }
}

fn push(op: &mut WriteOp, mutation: &Mutation<'_>) -> Result<(), Rejected> {
    match mutation {
        Mutation::CreateExclusive => op.create(true),
        Mutation::WriteFull(data) => op.write_full(data),
        Mutation::Append(data) => op.append(data),
        Mutation::OmapSet(entries) => op.omap_set(entries),
        Mutation::OmapRemove(keys) => op.omap_rm_keys(keys),
        Mutation::OmapRemoveRange { begin, end } => op.omap_rm_range(begin, end),
        Mutation::SetXattr(name, value) => op.setxattr(name, value)?,
        Mutation::Remove => op.remove(),
    }
    Ok(())
}

impl Bulk {
    /// Creates `oid` holding `data`, in one operation. A second call returns
    /// [`Rejected::Exists`] and leaves the content alone.
    ///
    /// `Bulk` has no omap and no guarded write: an erasure-coded pool rejects omap with
    /// `-EOPNOTSUPP`, so the guarded path is not offered on this type at all.
    ///
    /// ```compile_fail
    /// use rados_primitives::{Bulk, Write};
    ///
    /// fn no_guarded_write(bulk: &Bulk) {
    ///     let _ = bulk.write("oid", Write { guards: vec![], mutations: vec![] });
    /// }
    /// ```
    pub fn write_once(&self, oid: &str, data: &[u8]) -> Result<(), Rejected> {
        let mut op = WriteOp::new();
        op.create(true);
        op.write_full(data);
        Ok(op.operate(&self.io, oid)?)
    }

    pub fn remove(&self, oid: &str) -> Result<(), Rejected> {
        Ok(self.io.remove(oid)?)
    }
}
