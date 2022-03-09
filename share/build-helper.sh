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

BUILD_HELPER_DIR="$(dirname "${BASH_SOURCE[0]}")"
BUILD_ROOT="$(realpath --relative-to="$(pwd)" "$BUILD_HELPER_DIR/..")"

# Read version
BUILD_VERSION="$(cat "$BUILD_ROOT/share/VERSION.txt")"

OPTION_YES=""
OPTION_DRY_RUN=""

[ "$BUILD_LANGUAGE" ] || BUILD_LANGUAGE="$1"

# Colors
COLOR_RED="\033[0;31m"
COLOR_GREEN="\033[0;32m"
COLOR_YELLOW="\033[0;33m"
#COLOR_BLUE="\033[0;34m"
#COLOR_PURPLE="\033[0;35m"
COLOR_CYAN="\033[0;36m"
COLOR_NONE="\033[0m"

function usage()
{
    echo "Usage: $(basename "$0") [OPTION]... [COMMAND]..."
    echo "Build script for Apache Avro $BUILD_LANGUAGE"
    echo ""
    echo "Options:"
    echo "  -y, --yes                             Answer yes to all question"
    echo "      --dry-run                         Dont execute commands, just echo them"
    echo "      --no-colors                       No colors"
    echo "  -v, --verbose                         Verbose output"
    echo "  -V, --version                         Version"
    echo "  -h, --help                            Shows help"
    echo ""
    echo "Commands:"
    echo "  lint                                  Lint the code"
    echo "  test                                  Run unit tests"
    echo "  clean                                 Clean the build files"
    echo "  dist                                  Create a distribution tarball"
    echo "  release                               Release project"
    echo "  verify-release                        Verify release"
    echo "  perf                                  Run performance tests"
    echo "  interop-data-generate                 Generate interop data"
    echo "  interop-data-test                     Test interop data"
}

function cleanup()
{
    local RC="$?"

    trap "" INT TERM EXIT
    set +e

    [ "$RC" == "0" ] && ok "Done." || error "FAILED!"

    exit "$RC"
}

function ok()
{
    echo -e "$COLOR_GREEN$1$COLOR_NONE"
}

function warn()
{
    echo -e "$COLOR_YELLOW$1$COLOR_NONE"
}

function error()
{
    echo -e "$COLOR_RED$1$COLOR_NONE"
}

function fatal()
{
    error "$1"
    [ "$2" ] && exit "$2" || exit 1
}

function disable-colors()
{
    COLOR_RED=""
    COLOR_GREEN=""
    COLOR_YELLOW=""
    #COLOR_BLUE=""
    #COLOR_PURPLE=""
    COLOR_CYAN=""
    COLOR_NONE=""
}

function ask()
{
  local prompt="$COLOR_YELLOW$1 ([y]es/[n]o/([a]bort)? $COLOR_NONE"
  [ "$OPTION_YES" == "1" ] && echo -e "$prompt y" && return 0
  [ "$OPTION_DRY_RUN" == "1" ] && return 0
  while true; do
    answer="$(read -r -n 1 -p "$(echo -e $prompt)" answer && echo $answer)"
    echo
    case ${answer} in
        [Yy])
          return 0
          ;;

        [Nn]) 
          echo "Skip..."
          return 1
          ;;

        [Aa]) 
          fatal "ABORT"
          ;;
    esac
  done
}

function execute()
{
  echo -e "$COLOR_CYAN$*$COLOR_NONE"
  [ "$OPTION_DRY_RUN" == "1" ] && return 0
  eval "$@"
}

function build-run()
{
  # Iterate through arguments
  while [ $# -gt 0 ]
  do
    case "$1" in
      -h|--help)
        usage
        # Do not show done message
        trap "" INT TERM EXIT
        exit 0
        ;;

      -v|--verbose)
        set -x
        ;;

      -y|--yes)
        OPTION_YES="1"
        ;;

      --dry-run)
        OPTION_DRY_RUN="1"
        ;;

      --no-colors)
        disable-colors
        ;;

      -V|--version)
        echo "$BUILD_VERSION"
        ;;

      lint|test|perf|dist|release|verify-release|interop-data-generate|interop-data-test|clean)
        COMMAND="command_$1"
        [[ $(type -t $COMMAND) == function ]] || { echo "Command '$1' is not implemented"; exit 1; }
        # Execute command
        $COMMAND
        ;;

      *)
        fatal "Unknown option or command: $1"
        ;;
    esac

    shift
  done
}

# Turn off colors automatically if not terminal
[ -t 1 ] || disable-colors

trap "cleanup" EXIT INT TERM