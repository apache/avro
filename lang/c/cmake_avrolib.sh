#!/bin/bash

mkdir -p build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=avrolib -DCMAKE_BUILD_TYPE=Debug
make
make test
make install
mkdir -p avrolib/lib/static
cp avrolib/lib/libavro.a avrolib/lib/static/libavro.a
