#!/usr/bin/env php
<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once('test_helper.php');

$data_file = join(DIRECTORY_SEPARATOR, array(AVRO_BUILD_DATA_DIR, 'php.avro'));
$datum = array('nullField' => null,
               'boolField' => true,
               'intField' => -42,
               'longField' => (int) 2147483650,
               'floatField' => 1234.0,
               'doubleField' => -5432.6,
               'stringField' => 'hello avro',
               'bytesField' => "\x16\xa6",
               'arrayField' => array(5.0, -6.0, -10.5),
               'mapField' => array('a' => array('label' => 'a'),
                                   'c' => array('label' => '3P0')),
               'unionField' => 14.5,
               'enumField' => 'C',
               'fixedField' => '1019181716151413',
               'recordField' => array('label' => 'blah',
                                      'children' => array(
                                        array('label' => 'inner',
                                              'children' => array()))));

$schema_json = file_get_contents(AVRO_INTEROP_SCHEMA);
$io_writer = AvroDataIO::open_file($data_file, 'w', $schema_json);
$io_writer->append($datum);
$io_writer->close();
