//! Contract tests for guarded single-object operations against a real Ceph cluster.
//! They need the micro cluster `hack/run-ceph.sh` starts: `cargo test -- --include-ignored`.

use rados_primitives::{
    Bulk, Election, Fence, Guard, Mutation, Rados, Rejected, Replicated, Version, Write,
    epoch_bytes,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

const CEPH_CONF: &str = "/tmp/ceph/ceph.conf";
/// `EBLOCKLISTED` is 108 (`rados.h:513`).
const EBLOCKLISTED: i32 = 108;
const EINVAL: i32 = 22;

fn connect() -> Rados {
    let rados = Rados::with_id("admin").expect("create cluster handle");
    rados.conf_read_file(CEPH_CONF).expect("read ceph.conf");
    rados.connect().expect("connect");
    rados
}

fn temp_pool(rados: &Rados) -> String {
    let name = format!("test_pool_{}", Uuid::new_v4().simple());
    rados.create_pool(&name).expect("create pool");
    name
}

fn with_pool(f: impl FnOnce(&Rados, &str, &Replicated)) {
    let rados = connect();
    let pool = temp_pool(&rados);
    // A failing assertion unwinds out of `f`, so the pool is deleted on the way past: it
    // would otherwise stay resident in the memstore OSD, once per failing run.
    let outcome = {
        let replicated = Replicated::open(&rados, &pool).expect("open pool");
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f(&rados, &pool, &replicated)
        }))
    };
    rados.delete_pool(&pool).expect("delete pool");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

fn set(r: &Replicated, oid: &str, entries: &[(&[u8], &[u8])]) {
    r.write(
        oid,
        Write {
            guards: vec![],
            mutations: vec![Mutation::OmapSet(entries)],
        },
    )
    .expect("omap_set");
}

fn value(r: &Replicated, oid: &str, key: &[u8]) -> Option<Vec<u8>> {
    let (mut entries, _) = r.omap_get(oid, &[key]).expect("omap_get");
    entries.remove(key)
}

fn keys(r: &Replicated, oid: &str) -> Vec<Vec<u8>> {
    r.omap_page(oid, b"", b"", 100, None)
        .expect("omap_page")
        .entries
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

/// The `^v1:[^/]+/[0-9]+$` shape `profile simple-rados-client-with-blocklist` requires of an
/// address (`MonCap.cc:312-321`), checked without a regex crate.
fn is_entity_addr(addr: &str) -> bool {
    let Some(rest) = addr.strip_prefix("v1:") else {
        return false;
    };
    let Some((host, nonce)) = rest.rsplit_once('/') else {
        return false;
    };
    !host.is_empty()
        && !host.contains('/')
        && !nonce.is_empty()
        && nonce.bytes().all(|b| b.is_ascii_digit())
}

/// Contract: a guard that does not hold applies no mutation at all.
#[test]
#[ignore]
fn guard_mismatch_applies_no_mutation() {
    with_pool(|_, _, r| {
        let oid = "guarded";
        set(r, oid, &[(b"epoch", b"1"), (b"rev", b"7")]);

        let err = r
            .write(
                oid,
                Write {
                    guards: vec![Guard::OmapEq(b"epoch", b"9")],
                    mutations: vec![
                        Mutation::OmapSet(&[(b"rev", b"8")]),
                        Mutation::OmapRemove(&[b"epoch"]),
                    ],
                },
            )
            .expect_err("stale epoch");
        assert!(
            matches!(&err, Rejected::Fenced(Fence::Omap { key }) if key == b"epoch"),
            "{err}"
        );
        assert_eq!(value(r, oid, b"rev").as_deref(), Some(&b"7"[..]));
        assert_eq!(value(r, oid, b"epoch").as_deref(), Some(&b"1"[..]));
    });
}

/// Contract: the rejection names the guard that failed.
#[test]
#[ignore]
fn fenced_names_the_failing_guard() {
    with_pool(|_, _, r| {
        let oid = "named";
        let (epoch0, epoch1) = (epoch_bytes(0), epoch_bytes(1));
        // Two writes, so that the version before the last one is 1 rather than 0:
        // `assert_version(0)` is `-EINVAL`, not a fence (`PrimaryLogPG.cc:6682-6693`).
        set(r, oid, &[(b"epoch", &epoch1)]);
        set(r, oid, &[(b"rev", b"7")]);
        let (_, version) = r.omap_get(oid, &[b"epoch"]).expect("version");
        let stale = Version(version.0 - 1);
        assert!(stale.0 > 0, "{version:?}");

        let cases: Vec<(&str, Vec<Guard>, Fence)> = vec![
            (
                "epoch stale",
                vec![
                    Guard::OmapEq(b"epoch", &epoch0),
                    Guard::OmapEq(b"rev", b"7"),
                ],
                Fence::Omap {
                    key: b"epoch".to_vec(),
                },
            ),
            (
                "revision moved",
                vec![
                    Guard::OmapEq(b"epoch", &epoch1),
                    Guard::OmapEq(b"rev", b"9"),
                ],
                Fence::Omap {
                    key: b"rev".to_vec(),
                },
            ),
            (
                "version stale",
                vec![Guard::Version(stale)],
                Fence::Version { expected: stale },
            ),
        ];
        for (name, guards, expected) in cases {
            let err = r
                .write(
                    oid,
                    Write {
                        guards,
                        mutations: vec![Mutation::OmapSet(&[(b"rev", b"99")])],
                    },
                )
                .expect_err(name);
            assert!(
                matches!(&err, Rejected::Fenced(fence) if *fence == expected),
                "{name}: {err}"
            );
            assert_eq!(value(r, oid, b"rev").as_deref(), Some(&b"7"[..]), "{name}");
        }

        let err = r
            .write(
                oid,
                Write {
                    guards: vec![Guard::Version(version), Guard::Version(stale)],
                    mutations: vec![Mutation::OmapSet(&[(b"rev", b"99")])],
                },
            )
            .expect_err("two version guards");
        assert!(matches!(err, Rejected::Rados(EINVAL)), "{err}");
        assert_eq!(value(r, oid, b"rev").as_deref(), Some(&b"7"[..]));

        // -ENOENT is a fence only when the write asked for the object to exist.
        let err = r
            .write(
                "absent",
                Write {
                    guards: vec![Guard::Exists],
                    mutations: vec![Mutation::WriteFull(b"x")],
                },
            )
            .expect_err("assert_exists on a missing object");
        assert!(matches!(err, Rejected::Fenced(Fence::Exists)), "{err}");

        let err = r
            .write(
                "absent",
                Write {
                    guards: vec![],
                    mutations: vec![Mutation::Remove],
                },
            )
            .expect_err("remove of a missing object");
        assert!(matches!(err, Rejected::NotFound), "{err}");

        // On a missing object `omap_cmp` returns -ENOENT before it compares
        // (`PrimaryLogPG.cc:8047-8050`), so the empty-value guard does not hold and the
        // rejection names no guard.
        let err = r
            .write(
                "absent",
                Write {
                    guards: vec![Guard::OmapEq(b"epoch", b"")],
                    mutations: vec![Mutation::OmapSet(&[(b"epoch", &epoch1)])],
                },
            )
            .expect_err("an omap guard on a missing object");
        assert!(matches!(err, Rejected::NotFound), "{err}");
    });
}

/// Contract: a page pinned to a version stops seeing the object once it moves.
#[test]
#[ignore]
fn omap_page_pins_the_version() {
    with_pool(|_, _, r| {
        let oid = "paged";
        set(
            r,
            oid,
            &[(b"log/1", b"a"), (b"log/2", b"b"), (b"other", b"c")],
        );

        let page = r.omap_page(oid, b"log/", b"", 10, None).expect("page");
        assert!(!page.more);
        assert_eq!(
            page.entries,
            vec![
                (b"log/1".to_vec(), b"a".to_vec()),
                (b"log/2".to_vec(), b"b".to_vec()),
            ]
        );

        // `more` is set by whichever cap the answer hits first, `limit` included
        // (`PrimaryLogPG.cc:7983-7985`), so a small limit pages a three-key object.
        let first = r
            .omap_page(oid, b"log/", b"", 1, None)
            .expect("first of two");
        assert!(first.more);
        assert_eq!(first.entries, vec![(b"log/1".to_vec(), b"a".to_vec())]);
        let rest = r
            .omap_page(oid, b"log/", b"log/1", 1, None)
            .expect("rest after log/1");
        assert!(!rest.more);
        assert_eq!(rest.entries, vec![(b"log/2".to_vec(), b"b".to_vec())]);

        let at = page.version;
        let pinned = r
            .omap_page(oid, b"log/", b"", 10, Some(at))
            .expect("pinned");
        assert_eq!(pinned.entries, page.entries);
        assert_eq!(pinned.version, at);

        let after = r
            .omap_page(oid, b"log/", b"log/1", 10, Some(at))
            .expect("pinned page after log/1");
        assert_eq!(after.entries, vec![(b"log/2".to_vec(), b"b".to_vec())]);

        set(r, oid, &[(b"log/3", b"d")]);
        let err = r
            .omap_page(oid, b"log/", b"", 10, Some(at))
            .expect_err("the object moved past the pin");
        assert!(
            matches!(err, Rejected::Fenced(Fence::Version { expected }) if expected == at),
            "{err}"
        );
    });
}

/// Contract: `write_once` is write-once.
#[test]
#[ignore]
fn write_once_keeps_the_first_content() {
    with_pool(|rados, pool, _| {
        let bulk = Bulk::open(rados, pool).expect("open bulk");
        let oid = "run";
        bulk.write_once(oid, b"first").expect("write_once");

        let err = bulk.write_once(oid, b"second").expect_err("second write");
        assert!(matches!(err, Rejected::Exists), "{err}");
        assert_eq!(bulk.read(oid, 0, 64).expect("read"), b"first");
        assert_eq!(bulk.read(oid, 1, 2).expect("read at offset"), b"ir");
        assert_eq!(bulk.stat(oid).expect("stat").size, 5);

        bulk.remove(oid).expect("remove");
        assert!(
            matches!(
                bulk.stat(oid).expect_err("stat removed"),
                Rejected::NotFound
            ),
            "stat of a removed object"
        );
    });
}

/// Contract: contention, `MUST_RENEW`, and re-acquiring an expired lease.
#[test]
#[ignore]
fn leases_expire_and_renew() {
    with_pool(|_, pool, r| {
        let (oid, lease) = ("leased", "writer");
        let long = Duration::from_secs(30);

        assert!(
            r.lock_exclusive(oid, lease, "a", "holder A", long)
                .expect("acquire")
        );
        assert!(
            !r.lock_exclusive(oid, lease, "b", "", long)
                .expect("another cookie contends")
        );
        assert!(
            matches!(
                r.lock_exclusive(oid, lease, "a", "", long)
                    .expect_err("the same cookie is not a renewal"),
                Rejected::Exists
            ),
            "taking a held lease again"
        );

        let lockers = r.lockers(oid, lease).expect("lockers");
        assert_eq!(lockers.len(), 1);
        assert_eq!(lockers[0].cookie, "a");
        assert!(lockers[0].client.starts_with("client."), "{:?}", lockers[0]);
        assert!(is_entity_addr(&lockers[0].addr), "{}", lockers[0].addr);

        assert!(
            !r.renew(oid, "no such lease", "a", "", long)
                .expect("renewing an unheld lease")
        );
        // MUST_RENEW re-inserts the locker with `now + ttl` (`cls_lock.cc:195, 224-225`), so
        // renewing down to one second is what expires this lease.
        assert!(
            r.renew(oid, lease, "a", "", Duration::from_secs(1))
                .expect("renew")
        );
        std::thread::sleep(Duration::from_secs(2));
        assert!(
            r.lock_exclusive(oid, lease, "b", "", long)
                .expect("acquire after the lease expired")
        );

        let client = r.lockers(oid, lease).expect("lockers")[0].client.clone();
        r.break_lock(oid, lease, &client, "b").expect("break_lock");
        assert!(r.lockers(oid, lease).expect("lockers").is_empty());

        assert!(
            r.lock_exclusive(oid, lease, "c", "", long)
                .expect("re-acquire")
        );
        r.unlock(oid, lease, "c").expect("unlock");
        assert!(r.lockers(oid, lease).expect("lockers").is_empty());

        assert!(
            r.lock_exclusive(oid, lease, "shared", "", Duration::from_secs(1))
                .expect("acquire shared cookie")
        );
        std::thread::sleep(Duration::from_secs(2));
        let other_rados = connect();
        let other = Replicated::open(&other_rados, pool).expect("second client");
        assert!(
            other
                .lock_exclusive(oid, lease, "shared", "", long)
                .expect("re-acquire shared cookie")
        );
        assert!(
            !r.renew(oid, lease, "shared", "", long)
                .expect("old client cannot renew new client's cookie")
        );
    });
}

/// Contract: `OmapRemoveRange` removes exactly `[begin, end)`.
#[test]
#[ignore]
fn omap_remove_range_is_half_open() {
    with_pool(|_, _, r| {
        let oid = "trim";
        set(
            r,
            oid,
            &[
                (b"a", b"1"),
                (b"b", b"2"),
                (b"c", b"3"),
                (b"d", b"4"),
                (b"e", b"5"),
            ],
        );

        r.write(
            oid,
            Write {
                guards: vec![],
                mutations: vec![Mutation::OmapRemoveRange {
                    begin: b"b",
                    end: b"d",
                }],
            },
        )
        .expect("omap_rm_range");
        assert_eq!(
            keys(r, oid),
            vec![b"a".to_vec(), b"d".to_vec(), b"e".to_vec()]
        );

        r.write(
            oid,
            Write {
                guards: vec![],
                mutations: vec![Mutation::OmapRemove(&[b"d"])],
            },
        )
        .expect("omap_rm_keys");
        assert_eq!(keys(r, oid), vec![b"a".to_vec(), b"e".to_vec()]);
    });
}

/// Contract: byte mutations and the xattr guard.
#[test]
#[ignore]
fn xattr_guard_and_byte_mutations() {
    with_pool(|rados, pool, r| {
        let oid = "blob";
        // Bulk over the same pool is how the bytes are read back: `Replicated` reads omap.
        let bytes = Bulk::open(rados, pool).expect("open bulk");

        r.write(
            oid,
            Write {
                guards: vec![],
                mutations: vec![
                    Mutation::CreateExclusive,
                    Mutation::WriteFull(b"head"),
                    Mutation::Append(b"-tail"),
                    Mutation::SetXattr("epoch", b"1"),
                ],
            },
        )
        .expect("create");
        assert_eq!(bytes.read(oid, 0, 64).expect("read"), b"head-tail");

        let err = r
            .write(
                oid,
                Write {
                    guards: vec![],
                    mutations: vec![Mutation::CreateExclusive, Mutation::WriteFull(b"x")],
                },
            )
            .expect_err("second exclusive create");
        assert!(matches!(err, Rejected::Exists), "{err}");

        r.write(
            oid,
            Write {
                guards: vec![Guard::XattrEq("epoch", b"1")],
                mutations: vec![Mutation::Append(b"!")],
            },
        )
        .expect("the xattr guard holds");

        let err = r
            .write(
                oid,
                Write {
                    guards: vec![Guard::XattrEq("epoch", b"2")],
                    mutations: vec![Mutation::Append(b"?")],
                },
            )
            .expect_err("the xattr guard fails");
        assert!(
            matches!(&err, Rejected::Fenced(Fence::Xattr { name }) if name == "epoch"),
            "{err}"
        );

        // Two xattr guards cannot be told apart in a -ECANCELED, so the write is refused
        // before it is sent.
        let err = r
            .write(
                oid,
                Write {
                    guards: vec![Guard::XattrEq("epoch", b"1"), Guard::XattrEq("other", b"1")],
                    mutations: vec![Mutation::Append(b"?")],
                },
            )
            .expect_err("two xattr guards");
        assert!(matches!(err, Rejected::Rados(EINVAL)), "{err}");
        assert_eq!(bytes.read(oid, 0, 64).expect("read"), b"head-tail!");

        r.write(
            oid,
            Write {
                guards: vec![Guard::Exists],
                mutations: vec![Mutation::Remove],
            },
        )
        .expect("remove");
        assert!(
            matches!(
                bytes.stat(oid).expect_err("stat removed"),
                Rejected::NotFound
            ),
            "stat of a removed object"
        );
    });
}

/// Contract: `notify` waits for the acks it can get and returns each watcher's reply.
#[test]
#[ignore]
fn notify_returns_every_ack() {
    with_pool(|_, pool, r| {
        let oid = "watched";
        r.write(
            oid,
            Write {
                guards: vec![],
                mutations: vec![Mutation::CreateExclusive],
            },
        )
        .expect("create");

        let notifier_rados = connect();
        let notifier = Replicated::open(&notifier_rados, pool).expect("notifier");
        let timeout = Duration::from_secs(1);

        let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let first = r
            .watch(
                oid,
                Box::new(move |n| {
                    recorded.lock().expect("lock").push(n.payload);
                    b"ack-1".to_vec()
                }),
                Box::new(|_| {}),
            )
            .expect("watch");
        let second = r
            .watch(oid, Box::new(|_| b"ack-2".to_vec()), Box::new(|_| {}))
            .expect("second watch");

        let mut acks = notifier.notify(oid, b"ping", timeout).expect("notify");
        acks.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            acks.iter().map(|(_, r)| r.as_slice()).collect::<Vec<_>>(),
            vec![b"ack-1".as_slice(), b"ack-2".as_slice()]
        );
        assert!(acks.iter().all(|(gid, _)| *gid != 0), "{acks:?}");
        assert_eq!(*seen.lock().expect("lock"), vec![b"ping".to_vec()]);

        drop(second);
        let acks = notifier
            .notify(oid, b"ping", timeout)
            .expect("notify one watcher");
        assert_eq!(
            acks.iter().map(|(_, r)| r.as_slice()).collect::<Vec<_>>(),
            vec![b"ack-1".as_slice()]
        );
        drop(first);

        // A watcher still sleeping when the timeout passes is absent from the acks, and the
        // notify returns at the timeout rather than waiting for it.
        let slow = r
            .watch(
                oid,
                Box::new(|_| {
                    std::thread::sleep(Duration::from_secs(5));
                    b"late".to_vec()
                }),
                Box::new(|_| {}),
            )
            .expect("slow watch");
        let started = Instant::now();
        let acks = notifier
            .notify(oid, b"ping", timeout)
            .expect("notify a slow watcher");
        let elapsed = started.elapsed();
        assert!(acks.is_empty(), "{acks:?}");
        assert!(
            elapsed >= timeout && elapsed < Duration::from_secs(5),
            "{elapsed:?}"
        );
        drop(slow);
    });
}

/// Contract: the epoch `acquire` returns is the guard the new writer writes with.
#[test]
#[ignore]
fn acquire_hands_out_a_guarding_epoch() {
    with_pool(|_, pool, r| {
        let e = Election {
            oid: "role",
            name: "writer",
            epoch_key: b"meta/writer-epoch",
            holder_key: b"meta/writer-addr",
        };
        let ttl = Duration::from_secs(30);

        assert_eq!(r.acquire(&e, "a", ttl).expect("acquire"), Some(1));
        assert_eq!(
            value(r, e.oid, e.epoch_key).as_deref(),
            Some(&epoch_bytes(1)[..])
        );
        let holder =
            String::from_utf8(value(r, e.oid, e.holder_key).expect("holder")).expect("utf-8 addr");
        assert!(is_entity_addr(&holder), "{holder}");

        let epoch = epoch_bytes(1);
        r.write(
            e.oid,
            Write {
                guards: vec![Guard::OmapEq(e.epoch_key, &epoch)],
                mutations: vec![Mutation::OmapSet(&[(b"rev", b"1")])],
            },
        )
        .expect("write as the writer of epoch 1");

        // The object had no epoch key before the acquire, so what a writer from before it
        // would guard with is the empty value.
        let err = r
            .write(
                e.oid,
                Write {
                    guards: vec![Guard::OmapEq(e.epoch_key, b"")],
                    mutations: vec![Mutation::OmapSet(&[(b"rev", b"2")])],
                },
            )
            .expect_err("write from before the acquire");
        assert!(
            matches!(&err, Rejected::Fenced(Fence::Omap { key }) if key == e.epoch_key),
            "{err}"
        );
        assert_eq!(value(r, e.oid, b"rev").as_deref(), Some(&b"1"[..]));

        // A second client cannot take the role while the lease is held.
        let other_rados = connect();
        let other = Replicated::open(&other_rados, pool).expect("second handle");
        assert_eq!(
            other.acquire(&e, "b", ttl).expect("contended acquire"),
            None
        );
        assert_eq!(
            value(r, e.oid, e.epoch_key).as_deref(),
            Some(&epoch_bytes(1)[..])
        );
    });
}

/// Contract: the holder `acquire` replaces is blocklisted, and its writes stop.
#[test]
#[ignore]
fn acquire_blocklists_the_previous_holder() {
    with_pool(|_, pool, b| {
        // A has its own cluster handle: blocklisting it must not disturb the other tests.
        let a_rados = connect();
        let a = Replicated::open(&a_rados, pool).expect("open A");
        let e = Election {
            oid: "role",
            name: "writer",
            epoch_key: b"epoch",
            holder_key: b"holder",
        };

        assert_eq!(
            a.acquire(&e, "a", Duration::from_secs(1))
                .expect("A acquires"),
            Some(1)
        );
        let a_addr =
            String::from_utf8(value(&a, e.oid, e.holder_key).expect("holder")).expect("utf-8");
        assert!(is_entity_addr(&a_addr), "{a_addr}");

        // A's lease expires on the OSD's clock.
        std::thread::sleep(Duration::from_secs(2));

        assert_eq!(
            b.acquire(&e, "b", Duration::from_secs(60))
                .expect("B acquires"),
            Some(2)
        );
        assert_eq!(
            value(b, e.oid, e.holder_key).expect("holder after B"),
            b.lockers(e.oid, e.name).expect("lockers")[0]
                .addr
                .as_bytes()
        );

        // A's op carries A's own osdmap epoch. The OSD holds an op from a newer epoch until it
        // has that map (`OSD.cc:11331-11335`) and then rejects a blocklisted source
        // (`PrimaryLogPG.cc:2139-2143`), so A waiting for the map is what makes this
        // deterministic rather than a race with the OSD picking the map up.
        a_rados
            .wait_for_latest_osdmap()
            .expect("A waits for the new osdmap");
        let err = a
            .write(
                e.oid,
                Write {
                    guards: vec![],
                    mutations: vec![Mutation::OmapSet(&[(b"rev", b"x")])],
                },
            )
            .expect_err("A is fenced");
        assert!(matches!(err, Rejected::Rados(EBLOCKLISTED)), "{err}");
    });
}
