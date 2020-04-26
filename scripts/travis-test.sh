#!/usr/bin/env bash

set -euvxo pipefail

make clippy
make release
make test
cargo bench --no-run
