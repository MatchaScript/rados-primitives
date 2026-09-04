#!/bin/bash
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../ceph-rust/hack" && pwd)/run-ceph.sh" "$@"
