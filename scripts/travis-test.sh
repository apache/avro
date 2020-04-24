#!/usr/bin/env bash

set -euvxo pipefail

make release
make test

if [ "$TRAVIS_RUST_VERSION" = "nightly" ]; then
  travis_wait 20 make benchmark
fi
