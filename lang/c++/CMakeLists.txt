#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
cmake_minimum_required (VERSION 2.8)

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt" AVRO_VERSION)
else (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)

set (AVRO_VERSION_MAJOR ${AVRO_VERSION})
set (AVRO_VERSION_MINOR "0")
set (BUILD_DIRECTORY build)
set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ${BUILD_DIRECTORY})
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${BUILD_DIRECTORY})
set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${BUILD_DIRECTORY})

project (Avro-cpp)
include_directories (api ${BUILD_DIRECTORY})
add_library (avrocpp SHARED impl/Compiler.cc
        impl/Compiler.cc impl/CompilerNode.cc impl/Node.cc
        impl/NodeImpl.cc impl/Resolver.cc impl/ResolverSchema.cc impl/Schema.cc
        impl/Types.cc impl/Validator.cc impl/ValidSchema.cc impl/Zigzag.cc
        ${BUILD_DIRECTORY}/AvroYacc.cc ${BUILD_DIRECTORY}/AvroLex.cc)
target_link_libraries (avrocpp boost_regex-mt)

add_executable (precompile test/precompile.cc)

add_dependencies(avrocpp parser lexer)

target_link_libraries (precompile avrocpp boost_regex-mt)

find_package(BISON)

find_package(FLEX)

add_custom_command (OUTPUT ${BUILD_DIRECTORY}/bigrecord
    COMMAND precompile jsonschemas/bigrecord ${BUILD_DIRECTORY}/bigrecord
    DEPENDS precompile)

add_custom_target (testgen
    COMMAND python ../scripts/gen-cppcode.py -n testgen
    -i bigrecord -o testgen.hh
    WORKING_DIRECTORY ${BUILD_DIRECTORY}
    DEPENDS ${BUILD_DIRECTORY}/bigrecord)

add_custom_command (OUTPUT ${BUILD_DIRECTORY}/bigrecord2
    COMMAND precompile jsonschemas/bigrecord2 ${BUILD_DIRECTORY}/bigrecord2
    DEPENDS precompile)

add_custom_target (testgen2
    COMMAND python ../scripts/gen-cppcode.py -n testgen2
    -i bigrecord2 -o testgen2.hh
    WORKING_DIRECTORY ${BUILD_DIRECTORY}
    DEPENDS ${BUILD_DIRECTORY}/bigrecord2)

BISON_TARGET (parser parser/AvroYacc.yy ${BUILD_DIRECTORY}/AvroYacc.cc)
FLEX_TARGET (lexer parser/AvroLex.ll ${BUILD_DIRECTORY}/AvroLex.cc)

macro (test name)
    add_executable (${name} test/${name}.cc)
    target_link_libraries (${name} avrocpp boost_regex-mt)
endmacro (test)

test(buffertest)
test(unittest)

add_executable (testgentest test/testgen.cc)
add_dependencies (testgentest testgen testgen2)
target_link_libraries (testgentest avrocpp boost_regex-mt)

include (InstallRequiredSystemLibraries)

set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install(TARGETS avrocpp
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib)

install(DIRECTORY api/ DESTINATION include/avro
    FILES_MATCHING PATTERN *.hh)
