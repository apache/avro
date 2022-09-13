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

build_dir="../../build/rust"
dist_dir="../../dist/rust"

function clean {
  rm -rf $build_dir
  rm -rf $dist_dir
}


function prepare_build {
  clean
  mkdir -p $build_dir
}

cd $(dirname "$0")

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
      mkdir -p ${dist_dir}
      cargo build --release --lib --all-features --workspace
      git archive --output=apache-avro.tgz HEAD
      mv apache-avro.tgz ${dist_dir}/
      ;;
    interop-data-generate)
      prepare_build
      RUST_LOG=apache_avro=debug RUST_BACKTRACE=1 cargo run --features snappy,zstandard,bzip,xz --example generate_interop_data
      ;;
    interop-data-test)
      prepare_build
      echo "Running interop data tests"
      RUST_LOG=apache_avro=debug RUST_BACKTRACE=1 cargo run --features snappy,zstandard,bzip,xz --example test_interop_data
      echo -e "\nRunning single object encoding interop data tests"
      RUST_LOG=apache_avro=debug RUST_BACKTRACE=1 cargo run --example test_interop_single_object_encoding
      ;;
    *)
      echo "Usage: $0 {lint|test|dist|clean|interop-data-generate|interop-data-test}" >&2
      exit 1
  esac
done
