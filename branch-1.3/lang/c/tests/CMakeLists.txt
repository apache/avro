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

add_executable(generate_interop_data generate_interop_data.c)
target_link_libraries(generate_interop_data avro-static)
add_executable(test_interop_data test_interop_data.c)
target_link_libraries(test_interop_data avro-static)

add_executable(test_avro_schema test_avro_schema.c)
target_link_libraries(test_avro_schema avro-static)
add_test(test_avro_schema ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests ${CMAKE_CURRENT_BINARY_DIR}/test_avro_schema)

add_executable(test_avro_data test_avro_data.c)
target_link_libraries(test_avro_data avro-static)
add_test(test_avro_data ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests ${CMAKE_CURRENT_BINARY_DIR}/test_avro_data)

add_executable(test_cpp test_cpp.cpp)
target_link_libraries(test_cpp avro-static)
add_test(test_cpp ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests ${CMAKE_CURRENT_BINARY_DIR}/test_cpp)
