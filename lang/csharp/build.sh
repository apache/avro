#!/bin/bash
set -e						  # exit on error
set -x		

case "$1" in

    test)
	CONFIGURATION=Release TARGETFRAMEWORKVERSION=3.5 xbuild
	nunit-console Avro.nunit
	;;

    clean)
	rm -rf src/apache/{main,test,codegen}/obj
        rm -rf build
	;;

    *)
        echo "Usage: $0 {test|clean}"
        exit 1
esac

exit 0
