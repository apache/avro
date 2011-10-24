#!/bin/bash
set -e						  # exit on error
#set -x		

root_dir=$(pwd)
build_dir="../../build/c"
dist_dir="../../dist/c"
version=$(./version.sh project)
tarball="avro-c-$version.tar.gz"
doc_dir="../../build/avro-doc-$version/api/c"

function prepare_build {
  clean
  mkdir -p $build_dir
  (cd $build_dir && cmake $root_dir -DCMAKE_BUILD_TYPE=RelWithDebInfo)
}

function clean {
  if [ -d $build_dir ]; then
	find $build_dir | xargs chmod 755
	rm -rf $build_dir
  fi
}

case "$1" in

    interop-data-generate)
	prepare_build
	make -C $build_dir
	$build_dir/tests/generate_interop_data "../../share/test/schemas/interop.avsc"  "../../build/interop/data"
	;;

    interop-data-test)
	prepare_build
	make -C $build_dir
	$build_dir/tests/test_interop_data "../../build/interop/data"
	;;

    test)
	prepare_build
	make -C $build_dir
	make -C $build_dir test
        clean
	;;

    dist)
	prepare_build
	make -C $build_dir docs
        # This is a hack to force the built documentation to be included
        # in the source package.
	cp $build_dir/docs/*.html $root_dir/docs
	make -C $build_dir package_source
	rm $root_dir/docs/*.html
	if [ ! -d $dist_dir ]; then 
           mkdir -p $dist_dir 
        fi
	if [ ! -d $doc_dir ]; then
           mkdir -p $doc_dir
	fi
	mv $build_dir/$tarball $dist_dir
	cp $build_dir/docs/*.html $doc_dir
        clean
	;;

    clean)
        clean
	;;

    *)
        echo "Usage: $0 {interop-data-generate|interop-data-test|test|dist|clean}"
        exit 1
esac

exit 0
