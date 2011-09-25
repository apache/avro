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
cmake_minimum_required(VERSION 2.4)
project(AvroC)
enable_testing()

#-----------------------------------------------------------------------
# Retrieve the current version number

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh project
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE AVRO_VERSION_RESULT
    OUTPUT_VARIABLE AVRO_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(AVRO_VERSION_RESULT)
    message(FATAL_ERROR "Cannot determine Avro version number")
endif(AVRO_VERSION_RESULT)

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh libtool
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE LIBAVRO_VERSION_RESULT
    OUTPUT_VARIABLE LIBAVRO_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(LIBAVRO_VERSION_RESULT)
    message(FATAL_ERROR "Cannot determine libavro version number")
endif(LIBAVRO_VERSION_RESULT)


if(APPLE)
    if (NOT CMAKE_OSX_ARCHITECTURES)
        set(CMAKE_OSX_ARCHITECTURES "i386;x86_64" CACHE STRING "Build architectures for Mac OS X" FORCE)
    endif (NOT CMAKE_OSX_ARCHITECTURES)
endif(APPLE)

if(CMAKE_COMPILER_IS_GNUCC)
    add_definitions(-W -Wall)
endif(CMAKE_COMPILER_IS_GNUCC)

include_directories(${AvroC_SOURCE_DIR}/src)
include_directories(${AvroC_SOURCE_DIR}/jansson/src)

add_subdirectory(src)
add_subdirectory(examples)
add_subdirectory(tests)
add_subdirectory(docs)

add_custom_target(pretty
    "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake_pretty.cmake")

