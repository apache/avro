#!/usr/bin/env bash

set -euvxo pipefail

make release
make test

if [ "$TRAVIS_RUST_VERSION" = "nightly" ]; then
  make benchmark
fi
