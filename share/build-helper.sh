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

# Commands
declare -A BUILD_COMMANDS

BUILD_COMMANDS["lint"]="Lint the code"
BUILD_COMMANDS["test"]="Run unit tests"
BUILD_COMMANDS["clean"]="Clean the build files"
BUILD_COMMANDS["dist"]="Create a distribution tarball"
BUILD_COMMANDS["release"]="Release project"
BUILD_COMMANDS["verify-release"]="Verify release"
BUILD_COMMANDS["perf"]="Run performance tests"
BUILD_COMMANDS["interop-data-generate"]="Generate interop data"
BUILD_COMMANDS["interop-data-test"]="Test interop data"

# Example to define additional command. Put the following into the language specific build.sh
#
#function command_new-command() {
#  echo "This is a new command"
#}
#build-add-command "new-command" "Description of new command"

BUILD_START_TIME="$(date +%s)"

function usage()
{
    echo "Usage: $(basename "$0") [OPTION]... [COMMAND]..."
    echo "Build script for Apache Avro $BUILD_LANGUAGE"
    echo ""
    echo "Options:"
    printf "  %-40s%s\n" "-y, --yes"       "Answer yes to all question"
    printf "  %-40s%s\n" "    --dry-run"   "Do not execute commands, just echo them"
    printf "  %-40s%s\n" "    --no-colors" "No colors"
    printf "  %-40s%s\n" "-v, --verbose"   "Verbose output"
    printf "  %-40s%s\n" "-V, --version"   "Shows version"
    printf "  %-40s%s\n" "-h, --help"      "Shows help"
    echo ""
    echo "Commands:"
    for cmd in "${!BUILD_COMMANDS[@]}"
    do
      printf "  %-40s%s\n" "$cmd" "${BUILD_COMMANDS[$cmd]}"
    done | sort
}

function cleanup()
{
    local RC="$?"

    trap "" INT TERM EXIT
    set +e

    BUILD_END_TIME="$(date +%s)"

    [ "$RC" == "0" ] && ok "Done in $(( BUILD_END_TIME - BUILD_START_TIME ))s." || error "FAILED!"

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

function build-add-command()
{
  BUILD_COMMANDS["$1"]="$2"
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

      -*)
        fatal "Unknown option: $1"
        ;;

      *)
        # Check if command
        if [ "${BUILD_COMMANDS[$1]}" ] 
        then
          # Check if command function is available
          if [ "$(type -t command_"$1")" == "function" ]
          then
            # Execute command
            command_$1
          else
            fatal "Command $1 is not implemented"
          fi
        else
          fatal "Unknown command: $1"
        fi
        ;;
    esac

    shift
  done
}

# Turn off colors automatically if not terminal
[ -t 1 ] || disable-colors

trap "cleanup" EXIT INT TERM