# rados-primitives

Guarded single-object operations, leases, watch/notify, and writer election on Ceph RADOS.

## Overview

`rados-primitives` provides building blocks for stateful services running on RADOS:

- **Guarded compound writes**: Atomic compare-and-write operations on a single RADOS object (`omap_cmp`, `cmpxattr`, `assert_version`, `assert_exists`). If any guard fails, no mutations are applied.
- **Leases**: Timed exclusive leases via `cls_lock` (`lock_exclusive`, `renew`, `unlock`, `break_lock`).
- **Version-pinned reads**: Consistent pagination over omap keys pinned to an explicit object `Version`.
- **Watch & Notify**: Event notification and delivery via librados.
- **Writer election**: Lease-backed epoch progression and fencing (with optional client blocklisting).

## Usage

Add `rados-primitives` and `librados` to your `Cargo.toml`:

```toml
[dependencies]
rados-primitives = { path = "../rados-primitives" }
librados = { path = "../ceph-rust" }
```

### Example: Guarded Write

```rust
use rados_primitives::{Guard, Mutation, Replicated, Write};

fn increment_revision(
    pool: &Replicated,
    oid: &str,
    expected_rev: &[u8],
    new_rev: &[u8],
) -> Result<(), rados_primitives::Rejected> {
    pool.write(
        oid,
        Write {
            guards: vec![Guard::OmapEq(b"revision", expected_rev)],
            mutations: vec![Mutation::OmapSet(&[(b"revision", new_rev)])],
        },
    )
}
```

## Testing

Integration tests run against a live Ceph cluster. Ensure a Ceph cluster is reachable (or start one using `hack/run-ceph.sh`), then run:

```bash
cargo test -- --ignored
```

## License

Apache-2.0
