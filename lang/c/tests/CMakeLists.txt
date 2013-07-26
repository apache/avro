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

macro(add_avro_executable name)
    set(source "${ARGV1}")
    if (NOT source)
        set(source "${name}.c")
    endif (NOT source)
    add_executable(${name} ${source})
    target_link_libraries(${name} avro-static)
endmacro(add_avro_executable)

macro(add_avro_test name)
    add_avro_executable(${name} ${ARGN})
    if (WIN32)
        set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/Debug/${name}.exe)
    else (WIN32)
        set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/${name})
    endif (WIN32)

    add_test(${name}
        ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests
        ${exec_name}
    )
endmacro(add_avro_test)

add_avro_executable(generate_interop_data)
add_avro_executable(performance)
add_avro_executable(test_interop_data)

add_avro_test(test_data_structures)
add_avro_test(test_avro_schema)
add_avro_test(test_avro_schema_names)
add_avro_test(test_avro_values)
add_avro_test(test_avro_968)
add_avro_test(test_avro_984)
add_avro_test(test_avro_1034)
add_avro_test(test_avro_1084)
add_avro_test(test_avro_1087)
add_avro_test(test_avro_1165)
add_avro_test(test_avro_1238)
add_avro_test(test_avro_1279)
add_avro_test(test_avro_data)
add_avro_test(test_refcount)
add_avro_test(test_cpp test_cpp.cpp)
