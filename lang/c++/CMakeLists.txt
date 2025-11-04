#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
cmake_minimum_required (VERSION 3.20)

set (CMAKE_LEGACY_CYGWIN_WIN32 0)

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif()

if (CMAKE_CXX_STANDARD LESS 17)
    message(FATAL_ERROR "Avro requires at least C++17")
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (APPLE)
    # Enable MACOSX_RPATH by default
    cmake_policy (SET CMP0042 NEW)
endif()

if (NOT DEFINED CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS)
    set (CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS ON)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt" AVRO_VERSION)
else (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)

string(REPLACE "\n" "" AVRO_VERSION  ${AVRO_VERSION})
string(REPLACE "." ";" AVRO_VERSION  ${AVRO_VERSION})
list(GET AVRO_VERSION 0 AVRO_VERSION_MAJOR)
list(GET AVRO_VERSION 1 AVRO_VERSION_MINOR)
list(GET AVRO_VERSION 2 AVRO_VERSION_PATCH)
set(AVRO_VERSION "${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR}.${AVRO_VERSION_PATCH}")

project (Avro-cpp)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

option(AVRO_BUILD_EXECUTABLES "Build executables" ON)
option(AVRO_BUILD_TESTS "Build tests" ON)
option(AVRO_BUILD_SHARED "Build shared library" ON)
option(AVRO_BUILD_STATIC "Build static library" ON)
option(AVRO_USE_BOOST "Use Boost" OFF)

if (NOT AVRO_BUILD_STATIC AND NOT AVRO_BUILD_SHARED)
    message (FATAL_ERROR "At least one of AVRO_BUILD_STATIC or AVRO_BUILD_SHARED must be ON.")
endif ()

if (WIN32 AND NOT CYGWIN AND NOT MSYS)
    add_definitions (/EHa)
    add_definitions (
        -DNOMINMAX
        -DBOOST_ALL_NO_LIB)
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wduplicated-cond -Wduplicated-branches -Wlogical-op -Wuseless-cast -Wconversion -pedantic -Werror")
endif ()

if (AVRO_BUILD_TESTS OR AVRO_USE_BOOST)
    # Boost 1.70 and above provide a BoostConfig.cmake package configuration file.
    # See https://cmake.org/cmake/help/latest/policy/CMP0167.html
    find_package (Boost 1.70 REQUIRED CONFIG)
endif ()

find_package(fmt QUIET)
if (NOT fmt_FOUND)
    include(FetchContent)
    FetchContent_Declare(
            fmt
            GIT_REPOSITORY  https://github.com/fmtlib/fmt.git
            GIT_TAG         10.2.1
            GIT_PROGRESS    TRUE
            USES_TERMINAL_DOWNLOAD TRUE
    )
    FetchContent_MakeAvailable(fmt)
endif ()

find_package(Snappy CONFIG)
if (Snappy_FOUND)
    # Use CONFIG mode to guarantee that Snappy::snappy target exists if found.
    add_definitions(-DSNAPPY_CODEC_AVAILABLE)
    message("Enabled snappy codec, version: ${Snappy_VERSION}")
else ()
    message("Disabled snappy codec.")
endif ()

find_package(zstd CONFIG)
if(zstd_FOUND)
    message("Enabled zstd codec, version: ${zstd_VERSION}")
    set(ZSTD_TARGET $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
    add_definitions(-DZSTD_CODEC_AVAILABLE)
else()
    message("Disabled zstd codec.")
    set(ZSTD_TARGET "")
endif()

# FindZLIB guarantees that ZLIB::ZLIB target exists if found
# See https://cmake.org/cmake/help/latest/module/FindZLIB.html#imported-targets
find_package(ZLIB REQUIRED)

include_directories (include/avro ${CMAKE_CURRENT_BINARY_DIR})

set (AVRO_SOURCE_FILES
        impl/Compiler.cc impl/Node.cc impl/LogicalType.cc
        impl/NodeImpl.cc impl/ResolverSchema.cc impl/Schema.cc
        impl/Types.cc impl/ValidSchema.cc impl/Zigzag.cc
        impl/BinaryEncoder.cc impl/BinaryDecoder.cc
        impl/Stream.cc impl/FileStream.cc
        impl/Generic.cc impl/GenericDatum.cc
        impl/DataFile.cc
        impl/ZstdCompressWrapper.cc
        impl/ZstdDecompressWrapper.cc
        impl/parsing/Symbol.cc
        impl/parsing/ValidatingCodec.cc
        impl/parsing/JsonCodec.cc
        impl/parsing/ResolvingDecoder.cc
        impl/json/JsonIO.cc
        impl/json/JsonDom.cc
        impl/Resolver.cc impl/Validator.cc
        impl/CustomAttributes.cc
        )

function (setup_avro_lib target lib_type)
    add_library (${target} ${lib_type} ${AVRO_SOURCE_FILES})
    target_compile_definitions (${target} PRIVATE AVRO_SOURCE)
    if (lib_type STREQUAL SHARED)
        target_compile_definitions (${target} PUBLIC AVRO_DYN_LINK)
    endif ()
    set_target_properties (${target} PROPERTIES VERSION ${AVRO_VERSION})
    target_link_libraries (${target} PUBLIC
      $<BUILD_INTERFACE:fmt::fmt-header-only>
      $<BUILD_INTERFACE:ZLIB::ZLIB>
      $<BUILD_INTERFACE:$<TARGET_NAME_IF_EXISTS:Snappy::snappy>>
      $<$<BOOL:${zstd_FOUND}>:$<BUILD_INTERFACE:$<TARGET_NAME_IF_EXISTS:${ZSTD_TARGET}>>>
      $<INSTALL_INTERFACE:ZLIB::ZLIB>
      $<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:Snappy::snappy>>
      $<$<BOOL:${zstd_FOUND}>:$<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:${ZSTD_TARGET}>>>
    )
    target_include_directories (${target} PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
    )
endfunction (setup_avro_lib)

set (AVRO_INSTALL_LIBS)

if (AVRO_BUILD_SHARED)
    setup_avro_lib (avrocpp SHARED)
    list (APPEND AVRO_INSTALL_LIBS avrocpp)
    set (AVRO_LINK_LIB avrocpp)
endif ()

if (AVRO_BUILD_STATIC)
    setup_avro_lib (avrocpp_s STATIC)
    list (APPEND AVRO_INSTALL_LIBS avrocpp_s)
    # Static takes precedence for linking if both are set.
    set (AVRO_LINK_LIB avrocpp_s)
endif ()

if (AVRO_BUILD_EXECUTABLES)
    add_executable (precompile test/precompile.cc)

    target_link_libraries (precompile ${AVRO_LINK_LIB})

    add_executable (avrogencpp impl/avrogencpp.cc)
    target_link_libraries (avrogencpp ${AVRO_LINK_LIB})
    target_compile_definitions(avrogencpp PRIVATE AVRO_VERSION="${AVRO_VERSION}")
endif ()

if (AVRO_BUILD_TESTS)
    enable_testing()

    macro (gen file ns)
        add_custom_command (OUTPUT ${file}.hh
            COMMAND avrogencpp
                -p -
                -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/${file}
                -o ${file}.hh -n ${ns}
            DEPENDS avrogencpp ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/${file})
        add_custom_target (${file}_hh DEPENDS ${file}.hh)
    endmacro (gen)

    gen (empty_record empty)
    gen (bigrecord testgen)
    gen (bigrecord_r testgen_r)
    gen (bigrecord2 testgen2)
    gen (tweet testgen3)
    gen (union_array_union uau)
    gen (union_map_union umu)
    gen (union_conflict uc)
    gen (union_empty_record uer)
    gen (recursive rec)
    gen (reuse ru)
    gen (circulardep cd)
    gen (tree1 tr1)
    gen (tree2 tr2)
    gen (crossref cr)
    gen (primitivetypes pt)
    gen (cpp_reserved_words cppres)
    gen (cpp_reserved_words_union_typedef cppres_union)
    gen (big_union big_union)
    gen (union_redundant_types redundant_types)

    macro (unittest name)
        add_executable (${name} test/${name}.cc)
        target_link_libraries (${name} ${AVRO_LINK_LIB} ZLIB::ZLIB $<TARGET_NAME_IF_EXISTS:Snappy::snappy> $<$<BOOL:${zstd_FOUND}>:$<TARGET_NAME_IF_EXISTS:${ZSTD_TARGET}>>)
        add_test (NAME ${name} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${name})
    endmacro (unittest)

    unittest (buffertest)
    unittest (unittest)
    unittest (SchemaTests)
    unittest (LargeSchemaTests)
    unittest (CodecTests)
    unittest (StreamTests)
    unittest (SpecificTests)
    unittest (DataFileTests)
    unittest (JsonTests)
    unittest (AvrogencppTests)
    unittest (CompilerTests)
    unittest (AvrogencppTestReservedWords)
    unittest (CommonsSchemasTests)

    add_dependencies (AvrogencppTestReservedWords cpp_reserved_words_hh)
    add_dependencies (AvrogencppTestReservedWords cpp_reserved_words_hh
        cpp_reserved_words_union_typedef_hh)

    add_dependencies (AvrogencppTests bigrecord_hh bigrecord_r_hh bigrecord2_hh
        tweet_hh
        union_array_union_hh union_map_union_hh union_conflict_hh
        recursive_hh reuse_hh circulardep_hh tree1_hh tree2_hh crossref_hh
        primitivetypes_hh empty_record_hh cpp_reserved_words_union_typedef_hh
        union_empty_record_hh big_union_hh union_redundant_types_hh)
endif ()

include (InstallRequiredSystemLibraries)
include (GNUInstallDirs)

set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install (TARGETS ${AVRO_INSTALL_LIBS}
    EXPORT avrocpp_targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if (AVRO_BUILD_EXECUTABLES)
    install (TARGETS avrogencpp EXPORT avrocpp_targets RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif ()

install (DIRECTORY include/avro DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN *.hh)

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
endif (NOT CMAKE_BUILD_TYPE)

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-config-version.cmake"
    VERSION ${AVRO_VERSION}
    COMPATIBILITY SameMajorVersion)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/avro-cpp-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/avro-cpp
)

install(EXPORT avrocpp_targets
    NAMESPACE avro-cpp::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/avro-cpp
    FILE "avro-cpp-targets.cmake"
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/avro-cpp
)

cmake_path(APPEND pc_install_includedir "\${prefix}" "${CMAKE_INSTALL_INCLUDEDIR}")
cmake_path(APPEND pc_install_libdir "\${exec_prefix}" "${CMAKE_INSTALL_LIBDIR}")
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/avro-cpp.pc.in"
    "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp.pc"
    @ONLY
)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
)
