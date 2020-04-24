#!/usr/bin/env bash

set -euvxo pipefail

make release
make test
