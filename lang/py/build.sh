#!/usr/bin/env bash

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

set -eu

usage() {
  echo "Usage: $0 {clean|dist|doc|interop-data-generate|interop-data-test|lint|format|test|coverage|typechecks}"
  exit 1
}

clean() {
  git clean -xdf
}

dist() (
  local destination
  destination=$(
    d=../../dist/py
    mkdir -p "$d"
    cd -P "$d"
    pwd
  )
  uv build --out-dir "${destination}"
)

doc() {
  local doc_dir version
  version=$(cat ../../share/VERSION.txt)
  doc_dir="../../build/avro-doc-$version/api/py"
  uv run sphinx-build -b html docs/source/ docs/build/html
  mkdir -p "$doc_dir"
  cp -a docs/build/* "$doc_dir"
}

interop-data-generate() {
  uv run ./setup.py generate_interop_data
  cp -r avro/test/interop/data ../../build/interop
}

interop-data-test() {
  mkdir -p avro/test/interop ../../build/interop/data
  cp -r ../../build/interop/data avro/test/interop
  uv run python3 -m unittest avro.test.test_datafile_interop
}

lint() {
  uvx ruff check .
}

format() {
  uvx ruff format --diff .
}

test_() {
  format
  lint
  typechecks
  coverage
}

coverage() {
  SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
  TEST_INTEROP_DIR=$(mktemp -d)
  mkdir -p "${TEST_INTEROP_DIR}" "${SCRIPT_DIR}"/../../build/interop/data
  cp -r "${SCRIPT_DIR}"/../../build/interop/data "${TEST_INTEROP_DIR}"
  uv run coverage run -pm avro.test.gen_interop_data avro/interop.avsc "${TEST_INTEROP_DIR}"/data/py.avro
  cp -r "${TEST_INTEROP_DIR}"/data "${SCRIPT_DIR}"/../../build/interop
  uv run coverage run -pm unittest discover --buffer --failfast
  uv run coverage combine --append
  uv run coverage report
}

typechecks() {
  uv run mypy avro/
}

main() {
  (( $# )) || usage
  for target; do
    case "$target" in
      clean) clean;;
      dist) dist;;
      doc) doc;;
      interop-data-generate) interop-data-generate;;
      interop-data-test) interop-data-test;;
      lint) lint;;
      format) format;;
      test) test_;;
      coverage) coverage ;;
      typechecks) typechecks ;;
      *) usage;;
    esac
  done
}

main "$@"
