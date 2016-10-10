#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRCMAKEIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_build_tool build
add_test_type buildtest

BUILDFILE="build.sh"
BUILD="./build.sh"


## @description  get the name of the build filename
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       make build file
function build_buildfile
{
  echo "${BUILDFILE}"
}

## @description  get the name of the build binary
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       filename
## @param        testname
function build_executor
{
  echo "${BUILD}"
}

## @description  build worker
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       status
## @param        repostatus
## @param        test
function build_modules_worker
{
  case "$2" in
    'distclean')
        modules_workers "dist clean"
    ;;
    *)
      echo "${BUILD}"
      modules_workers $2
    ;;
    esac
}

### @description  build module queuer
### @audience     private
### @stability    evolving
### @replaceable  no
function build_builtin_personality_modules
{
  declare repostatus=$1
  declare testtype=$2

  declare module

  yetus_debug "Using builtin personality_modules"
  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  for module in "${CHANGED_MODULES[@]}"; do
    personality_enqueue_module "${module}"
  done
}

### @description  build test determiner
### @audience     private
### @stability    evolving
### @replaceable  no
### @param        filename
function build_builtin_personality_file_tests
{
  declare filename=$1
  yetus_debug "Using builtin build personality_file_tests"
  add_test buildtest
}

function buildtest_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} = build ]]; then
      yetus_debug "run the tests ${filename}"
      add_test buildtest
    fi
}

function buildtest_postcompile
{
  declare repostatus=$1
  declare result=0
  if [[ ${BUILDTOOL} != build ]]; then
    return 0
  fi
#
  if [[ "${repostatus}" = branch ]]; then
    big_console_header "build test : ${PATCH_BRANCH}"
  else
    big_console_header "build test : ${BUILDMODE}"
  fi

  modules_workers "test"
  result=$?
  modules_messages "${repostatus}" buildtest true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}
