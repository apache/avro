#!/bin/bash
set -e						  # exit on error
#set -x		

root_dir=$(pwd)
build_dir="../../build/c"
dist_dir="../../dist"
version=$(./version.sh project)
tarball="avro-c-$version.tar.gz"
doc_dir="../../build/avro-doc-$version/api/c"

function autoreconf_check {
  if [ ! -f configure ]; then
    autoreconf -f -i
  fi
}

function prepare_build {
  autoreconf_check
  clean
  mkdir -p $build_dir
  (cd $build_dir && $root_dir/configure)
}

function clean {
  if [ -d $build_dir ]; then
	find $build_dir | xargs chmod 755
	rm -rf $build_dir
  fi
}

case "$1" in

    test)
	prepare_build
	make -C $build_dir check
        clean
	;;

    dist)
	prepare_build
	make -C $build_dir distcheck
	if [ ! -d $dist_dir ]; then 
           mkdir -p $dist_dir 
        fi
	if [ ! -d $doc_dir ]; then
           mkdir -p $doc_dir
	fi
	mv $build_dir/$tarball $dist_dir
	cp $build_dir/docs/*.html $doc_dir
        md5file="$dist_dir/$tarball.md5"
	md5sum $dist_dir/$tarball > $md5file 2>/dev/null || md5 $dist_dir/$tarball > $md5file 2>/dev/null
        clean
	;;

    clean)
        clean
	;;

    *)
        echo "Usage: $0 {test|dist|clean}"
        exit 1
esac

exit 0
