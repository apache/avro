#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e  # exit on error

root_dir=$(pwd)
build_dir="../../build/rust"
dist_dir="../../dist/rust"


function clean {
  if [ -d $build_dir ]; then
    find $build_dir | xargs chmod 755
    rm -rf $build_dir
  fi
}


function prepare_build {
  clean
  mkdir -p $build_dir
}

cd `dirname "$0"`

for target in "$@"
do
  case "$target" in
    clean)
      cargo clean
      ;;
    lint)
      cargo clippy --all-targets --all-features -- -Dclippy::all
      ;;
    test)
      cargo test
      ;;
    dist)
      cargo build --release --lib --all-features
      cargo package
      mkdir -p  ../../dist/rust
      cp target/package/apache-avro-*.crate $dist_dir
      ;;
    interop-data-generate)
      prepare_build
      export RUST_LOG=avro_rs=debug
      export RUST_BACKTRACE=1
      cargo run --all-features --example generate_interop_data
      ;;

    interop-data-test)
      prepare_build
      cargo run --all-features --example test_interop_data
      ;;
    *)
      echo "Usage: $0 {lint|test|dist|clean|interop-data-generate|interop-data-test}" >&2
      exit 1
  esac
done
