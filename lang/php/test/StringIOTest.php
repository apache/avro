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
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once('test_helper.php');

class StringIOTest extends PHPUnit_Framework_TestCase
{

  public function test_write()
  {
    $strio = new AvroStringIO();
    $this->assertEquals(0, $strio->tell());
    $str = 'foo';
    $strlen = strlen($str);
    $this->assertEquals($strlen, $strio->write($str));
    $this->assertEquals($strlen, $strio->tell());
  }

  public function test_seek()
  {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function test_tell()
  {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function test_read()
  {
    $this->markTestIncomplete('This test has not been implemented yet.');
  }

  public function test_string_rep()
  {
    $writers_schema_json = '"null"';
    $writers_schema = AvroSchema::parse($writers_schema_json);
    $datum_writer = new AvroIODatumWriter($writers_schema);
    $strio = new AvroStringIO();
    $this->assertEquals('', $strio->string());
    $dw = new AvroDataIOWriter($strio, $datum_writer, $writers_schema_json);
    $dw->close(); 

    $this->assertEquals(57, strlen($strio->string()), 
                        AvroDebug::ascii_string($strio->string()));

    $read_strio = new AvroStringIO($strio->string());

    $datum_reader = new AvroIODatumReader();
    $dr = new AvroDataIOReader($read_strio, $datum_reader);
    $read_data = $dr->data();
    $datum_count = count($read_data);
    $this->assertEquals(0, $datum_count);
  }

}
