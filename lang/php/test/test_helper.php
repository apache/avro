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

define('AVRO_TEST_HELPER_DIR', dirname(__FILE__));

require_once(join(DIRECTORY_SEPARATOR, 
                  array(dirname(AVRO_TEST_HELPER_DIR), 'lib', 'avro.php')));

define('TEST_TEMP_DIR', join(DIRECTORY_SEPARATOR, 
                             array(AVRO_TEST_HELPER_DIR, 'tmp')));

define('AVRO_BASE_DIR', dirname(dirname(dirname(AVRO_TEST_HELPER_DIR))));
define('AVRO_SHARE_DIR', join(DIRECTORY_SEPARATOR,
                               array(AVRO_BASE_DIR, 'share')));
define('AVRO_BUILD_DIR', join(DIRECTORY_SEPARATOR,
                               array(AVRO_BASE_DIR, 'build')));
define('AVRO_BUILD_DATA_DIR', join(DIRECTORY_SEPARATOR,
                                    array(AVRO_BUILD_DIR, 'interop', 'data')));
define('AVRO_TEST_SCHEMAS_DIR', join(DIRECTORY_SEPARATOR,
                                     array(AVRO_SHARE_DIR, 'test', 'schemas')));
define('AVRO_INTEROP_SCHEMA', join(DIRECTORY_SEPARATOR,
                                   array(AVRO_TEST_SCHEMAS_DIR, 'interop.avsc')));

$tz = ini_get('date.timezone');
if (empty($x))
  date_default_timezone_set('America/New_York');
