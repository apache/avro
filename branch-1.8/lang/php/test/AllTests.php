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

require_once('DataFileTest.php');
require_once('SchemaTest.php');
require_once('NameTest.php');
require_once('StringIOTest.php');
require_once('IODatumReaderTest.php');
require_once('LongEncodingTest.php');
require_once('FloatIntEncodingTest.php');
require_once('DatumIOTest.php');
require_once('ProtocolFileTest.php');
// InterOpTest tests are run separately.

class AllTests
{
  public static function suite()
  {
    $suite = new PHPUnit_Framework_TestSuite('AvroAllTests');
    $suite->addTestSuite('DataFileTest');
    $suite->addTestSuite('SchemaTest');
    $suite->addTestSuite('NameTest');
    $suite->addTestSuite('StringIOTest');
    $suite->addTestSuite('IODatumReaderTest');
    $suite->addTestSuite('LongEncodingTest');
    $suite->addTestSuite('FloatIntEncodingTest');
    $suite->addTestSuite('DatumIOTest');
    $suite->addTestSuite('ProtocolFileTest');
    return $suite;
  }
}
