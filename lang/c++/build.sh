#!/bin/bash

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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e # exit on error

function usage {
  echo "Usage: $0 {test|dist|clean}"
  exit 1
}

if [ $# -eq 0 ]
then
  usage
fi

cd `dirname "$0"` # connect to root

VERSION=`cat ../../share/VERSION.txt`

root_dir=$(pwd)
build_dir="../../build/avro-cpp-$VERSION"
dist_dir="../../dist/cpp"
doc_dir="../../build/avro-doc-$VERSION/api/cpp/html"

tarfile=avro-cpp-$VERSION.tar.gz

set -x # echo commands

for target in "$@"
do

function do_autoreconf {
    if [ ! -f configure ]; then
        autoreconf -f -i
    fi
    if [ ! -f configure ]; then
        exit 1
    fi
}

function check_dir {
    if [ ! -d $1 ]; then
        mkdir -p $1
    fi
    if [ ! -d $1 ]; then
        exit 1
    fi
}

function do_configure {

    do_autoreconf

    check_dir $build_dir

    if [ ! -f $build_dir/Makefile ]; then
        (cd $build_dir && ../../lang/c++/configure)
    fi

    if [ ! -f $build_dir/Makefile ]; then
        exit 1
    fi
}

function do_build {
    (cd $build_dir && make)
    (cd $build_dir && make check)
}

function do_docs {
    check_dir $doc_dir
    (cd $build_dir && make doc)
    if [ ! -f $build_dir/doc/html/index.html ]; then
        exit 1
    fi
    cp -rf $build_dir/doc/html/* $doc_dir/
}

function do_tar_file {
    check_dir $dist_dir
    (cd $build_dir && make dist)
    if [ ! -f $build_dir/$tarfile ]; then
        exit 1
    fi
    cp -f $build_dir/$tarfile $dist_dir/$tarfile
    md5sum $dist_dir/$tarfile > $dist_dir/$tarfile.md5
}

function do_dist {
    (cd $build_dir && make)
    (cd $build_dir && make check)
    do_docs
    do_tar_file
}

function do_clean {

    if [ -d $build_dir ]; then
        rm -rf $build_dir
    fi
    if [ -d $doc_dir ]; then
        rm -rf $doc_dir
    fi
    rm -rf $dist_dir/$tarfile
    rm -rf $dist_dir/$tarfile.md5

}

case "$target" in


    test)
    do_configure
    do_build
	;;

    dist)
    do_configure
    do_dist
    ;;

    clean)
    do_clean 
	;;

    *)
        usage
esac

done

exit 0
