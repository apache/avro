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
cmake_minimum_required (VERSION 2.6)

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

find_package (Boost 1.38 COMPONENTS regex filesystem system program_options)

include_directories (api ${BUILD_DIRECTORY})

add_library (avrocpp SHARED
        impl/Compiler.cc impl/CompilerNode.cc impl/Node.cc
        impl/NodeImpl.cc impl/Resolver.cc impl/ResolverSchema.cc impl/Schema.cc
        impl/Types.cc impl/Validator.cc impl/ValidSchema.cc impl/Zigzag.cc
        impl/BinaryEncoder.cc impl/BinaryDecoder.cc
        impl/Stream.cc impl/FileStream.cc
        impl/Generic.cc
        impl/DataFile.cc
        impl/parsing/Symbol.cc
        impl/parsing/ValidatingCodec.cc
        impl/parsing/JsonCodec.cc
        impl/parsing/ResolvingDecoder.cc
        ${BUILD_DIRECTORY}/AvroYacc.cc ${BUILD_DIRECTORY}/AvroLex.cc)

target_link_libraries (avrocpp ${Boost_LIBRARIES})

add_executable (precompile test/precompile.cc)

add_dependencies(avrocpp parser lexer)

target_link_libraries (precompile avrocpp ${Boost_LIBRARIES})

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

add_custom_command (OUTPUT ${BUILD_DIRECTORY}/AvroYacc.cc
    COMMAND bison --defines=AvroYacc.hh -o AvroYacc.cc ../parser/AvroYacc.yy
    WORKING_DIRECTORY ${BUILD_DIRECTORY})

add_custom_command (OUTPUT ${BUILD_DIRECTORY}/AvroLex.cc
    COMMAND flex -oAvroLex.cc ../parser/AvroLex.ll
    WORKING_DIRECTORY ${BUILD_DIRECTORY})
    
add_custom_target (bigrecord_hh
    COMMAND avrogencpp -i jsonschemas/bigrecord
        -o ${BUILD_DIRECTORY}/bigrecord.hh -n testgen
    DEPENDS avrogencpp)

add_custom_target (bigrecord2_hh
    COMMAND avrogencpp -i jsonschemas/bigrecord2
        -o ${BUILD_DIRECTORY}/bigrecord2.hh -n testgen2
    DEPENDS avrogencpp)

add_custom_target (union_array_union_hh
    COMMAND avrogencpp -i jsonschemas/union_array_union
        -o ${BUILD_DIRECTORY}/union_array_union.hh -n uau
    DEPENDS avrogencpp)

add_custom_target (union_map_union_hh
    COMMAND avrogencpp -i jsonschemas/union_map_union
        -o ${BUILD_DIRECTORY}/union_map_union.hh -n umu
    DEPENDS avrogencpp)

add_custom_target (recursive_hh
    COMMAND avrogencpp -i jsonschemas/recursive
        -o ${BUILD_DIRECTORY}/recursive.hh -n rec
    DEPENDS avrogencpp)

add_custom_target (reuse_hh
    COMMAND avrogencpp -i jsonschemas/reuse
        -o ${BUILD_DIRECTORY}/reuse.hh -n reu
    DEPENDS avrogencpp)

add_custom_target (circulardep_hh
    COMMAND avrogencpp -i jsonschemas/circulardep
        -o ${BUILD_DIRECTORY}/circulardep.hh -n reu
    DEPENDS avrogencpp)

macro (test name)
    add_executable (${name} test/${name}.cc)
    target_link_libraries (${name} avrocpp ${Boost_LIBRARIES})
endmacro (test)

test(buffertest)
test(unittest)

add_executable (CodecTests test/CodecTests.cc)
target_link_libraries (CodecTests avrocpp ${Boost_LIBRARIES})

add_executable (StreamTests test/StreamTests.cc)
target_link_libraries (StreamTests avrocpp ${Boost_LIBRARIES})

add_executable (SpecificTests test/SpecificTests.cc)
target_link_libraries (SpecificTests avrocpp ${Boost_LIBRARIES})

add_executable (avrogencpp impl/avrogencpp.cc)
target_link_libraries (avrogencpp avrocpp ${Boost_LIBRARIES})

add_executable (testgentest test/testgen.cc)
add_dependencies (testgentest testgen testgen2)
target_link_libraries (testgentest avrocpp ${Boost_LIBRARIES})

add_executable (AvrogencppTests test/AvrogencppTests.cc)
add_dependencies (AvrogencppTests bigrecord_hh bigrecord2_hh
    union_array_union_hh union_map_union_hh recursive_hh reuse_hh
    circulardep_hh)
target_link_libraries (AvrogencppTests avrocpp ${BOOST_LIBRARIES})

add_executable (DataFileTests test/DataFileTests.cc)
target_link_libraries (DataFileTests avrocpp ${Boost_LIBRARIES})

include (InstallRequiredSystemLibraries)

set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install(TARGETS avrocpp
    LIBRARY DESTINATION local/lib
    ARCHIVE DESTINATION local/lib
    RUNTIME DESTINATION local/lib)

install(DIRECTORY api/ DESTINATION local/include/avro
    FILES_MATCHING PATTERN *.hh)

set (CMAKE_BUILD_TYPE Release)

