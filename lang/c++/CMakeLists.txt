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

set (CMAKE_LEGACY_CYGWIN_WIN32 0)

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt" AVRO_VERSION)
else (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)

set (AVRO_VERSION_MAJOR ${AVRO_VERSION})
set (AVRO_VERSION_MINOR "0")

project (Avro-cpp)

if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "-Wall")
endif ()

find_package (Boost 1.38 REQUIRED
    COMPONENTS regex filesystem system program_options)

include_directories (api ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIRS})

set (SOURCE_FILES
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
        ${CMAKE_CURRENT_BINARY_DIR}/AvroYacc.cc ${CMAKE_CURRENT_BINARY_DIR}/AvroLex.cc)

add_library (avrocpp SHARED ${SOURCE_FILES})

add_library (avrocpp_s STATIC ${SOURCE_FILES})

set_target_properties (avrocpp PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

set_target_properties (avrocpp_s PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

target_link_libraries (avrocpp ${Boost_LIBRARIES})

add_executable (precompile test/precompile.cc)

add_dependencies(avrocpp parser lexer)

target_link_libraries (precompile avrocpp ${Boost_LIBRARIES})

add_custom_command (OUTPUT bigrecord
    COMMAND precompile ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/bigrecord
        bigrecord
    DEPENDS precompile)

add_custom_target (testgen
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/gen-cppcode.py
        -n testgen -i bigrecord -o testgen.hh
    DEPENDS bigrecord)

add_custom_command (OUTPUT bigrecord2
    COMMAND precompile ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/bigrecord2
        bigrecord2
    DEPENDS precompile)

add_custom_target (testgen2
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/gen-cppcode.py
        -n testgen2 -i bigrecord2 -o testgen2.hh
    DEPENDS bigrecord2)

add_custom_command (OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/AvroYacc.cc
    COMMAND bison --defines=${CMAKE_CURRENT_BINARY_DIR}/AvroYacc.hh -o ${CMAKE_CURRENT_BINARY_DIR}/AvroYacc.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/AvroYacc.yy)

add_custom_command (OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/AvroLex.cc
    COMMAND flex -oAvroLex.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/AvroLex.ll)
    
add_custom_target (bigrecord_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/bigrecord
        -o bigrecord.hh -n testgen
    DEPENDS avrogencpp)

add_custom_target (bigrecord2_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/bigrecord2
        -o bigrecord2.hh -n testgen2
    DEPENDS avrogencpp)

add_custom_target (union_array_union_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/union_array_union
        -o union_array_union.hh -n uau
    DEPENDS avrogencpp)

add_custom_target (union_map_union_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/union_map_union
        -o union_map_union.hh -n umu
    DEPENDS avrogencpp)

add_custom_target (union_conflict_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/union_conflict
        -o union_conflict.hh -n uc -U
    DEPENDS avrogencpp)

add_custom_target (recursive_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/recursive
        -o recursive.hh -n rec
    DEPENDS avrogencpp)

add_custom_target (reuse_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/reuse
        -o reuse.hh -n reu
    DEPENDS avrogencpp)

add_custom_target (circulardep_hh
    COMMAND avrogencpp
        -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/circulardep
        -o circulardep.hh -n reu
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
    union_array_union_hh union_map_union_hh union_conflict_hh
    recursive_hh reuse_hh circulardep_hh)
target_link_libraries (AvrogencppTests avrocpp ${BOOST_LIBRARIES})

add_executable (DataFileTests test/DataFileTests.cc)
target_link_libraries (DataFileTests avrocpp ${Boost_LIBRARIES})

include (InstallRequiredSystemLibraries)

set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install (TARGETS avrocpp avrocpp_s
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib)

install (TARGETS avrogencpp RUNTIME DESTINATION bin)

install (DIRECTORY api/ DESTINATION include/avro
    FILES_MATCHING PATTERN *.hh)

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
endif (NOT CMAKE_BUILD_TYPE)

