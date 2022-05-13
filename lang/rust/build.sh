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

cd "$(dirname "$0")" # If being called from another folder, cd into the directory containing this script.

# shellcheck disable=SC1091
source ../../share/build-helper.sh "Rust"

build_dir="$BUILD_ROOT/build/rust/"
dist_dir="$BUILD_ROOT/dist/rust/"
modules=(
  "avro_derive"
  "avro"
)


function clean {
  if [ -d "$build_dir" ]; then
    execute find "$build_dir" -exec chmod 755 {} +
    execute rm -rf "$build_dir"
  fi
}

function prepare_build {
  clean
  execute mkdir -p "$build_dir"
}

function command_clean()
{
  execute rm -rf "$dist_dir"
  execute cargo clean
}

function command_lint()
{
  execute cargo clippy --all-targets --all-features -- -Dclippy::all
}

function command_test()
{
  execute cargo test
}

function command_dist()
{
  # Local distribution cannot be created because Cargo projects with
  # virtual manifests which use Path dependencies cannot resolve
  # non-published dependencies
  # Read https://doc.rust-lang.org/cargo/reference/workspaces.html
  # and https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-path-dependencies
  # for more details

  # The source distribution (the .crate) is created at the end of
  # the `release` step
  true
}

function command_release()
{
  execute cargo login "$CARGO_API_TOKEN"

  for module in "${modules[@]}"; do
    pushd "${module}"
    execute cargo build --release --lib --all-features
    execute cargo publish
    execute mkdir -p "../${dist_dir}"
    popd
  done
  execute cp target/package/apache-avro-*.crate "$dist_dir"

}

function command_interop-data-generate()
{
  prepare_build
  export RUST_LOG=apache_avro=debug
  export RUST_BACKTRACE=1
  execute cargo run  --features snappy,zstandard,bzip,xz --example generate_interop_data
}

function command_interop-data-test()
{
  prepare_build
  execute cargo run --all-features --example test_interop_data
  echo "Running interop data tests"
  execute cargo run --features snappy,zstandard,bzip,xz --example test_interop_data
  echo -e "\nRunning single object encoding interop data tests"
  execute cargo run --example test_interop_single_object_encoding
}

build-run "$@"
