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

class DataFileTest extends PHPUnit_Framework_TestCase
{
  private $data_files;
  const REMOVE_DATA_FILES = true;

  static function current_timestamp() { return strftime("%Y%m%dT%H%M%S"); }

  protected function add_data_file($data_file)
  {
    if (is_null($this->data_files))
      $this->data_files = array();
    $data_file = "$data_file.".self::current_timestamp();
    $full = join(DIRECTORY_SEPARATOR, array(TEST_TEMP_DIR, $data_file));
    $this->data_files []= $full;
    return $full;
  }

  protected static function remove_data_file($data_file)
  {
    if (file_exists($data_file))
      unlink($data_file);
  }

  protected function remove_data_files()
  {
    if (self::REMOVE_DATA_FILES
        && 0 < count($this->data_files))
      foreach ($this->data_files as $data_file)
        $this->remove_data_file($data_file);
  }

  protected function setUp()
  {
    if (!file_exists(TEST_TEMP_DIR))
      mkdir(TEST_TEMP_DIR);
    $this->remove_data_files();
  }
  protected function tearDown()
  {
    $this->remove_data_files();
  }

  public function test_write_read_nothing_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-nothing-null.avr');
    $writers_schema = '"null"';
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_data = array_shift($dr->data());
    $dr->close();
    $this->assertEquals(null, $read_data);
  }

  public function test_write_read_null_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-null.avr');
    $writers_schema = '"null"';
    $data = null;
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->append($data);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_data = array_shift($dr->data());
    $dr->close();
    $this->assertEquals($data, $read_data);
  }

  public function test_write_read_string_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-str.avr');
    $writers_schema = '"string"';
    $data = 'foo';
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->append($data);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_data = array_shift($dr->data());
    $dr->close();
    $this->assertEquals($data, $read_data);
  }


  public function test_write_read_round_trip()
  {

    $data_file = $this->add_data_file('data-wr-int.avr');
    $writers_schema = '"int"';
    $data = 1;

    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->append(1);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_data = array_shift($dr->data());
    $dr->close();
    $this->assertEquals($data, $read_data);

  }

  public function test_write_read_true_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-true.avr');
    $writers_schema = '"boolean"';
    $datum = true;
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->append($datum);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_datum = array_shift($dr->data());
    $dr->close();
    $this->assertEquals($datum, $read_datum);
  }

  public function test_write_read_false_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-false.avr');
    $writers_schema = '"boolean"';
    $datum = false;
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    $dw->append($datum);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_datum = array_shift($dr->data());
    $dr->close();
    $this->assertEquals($datum, $read_datum);
  }
  public function test_write_read_int_array_round_trip()
  {
    $data_file = $this->add_data_file('data-wr-int-ary.avr');
    $writers_schema = '"int"';
    $data = array(10, 20, 30, 40, 50, 60, 70);
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    foreach ($data as $datum)
      $dw->append($datum);
    $dw->close();

    $dr = AvroDataIO::open_file($data_file);
    $read_data = $dr->data();
    $dr->close();
    $this->assertEquals($data, $read_data,
                        sprintf("in: %s\nout: %s",
                                json_encode($data), json_encode($read_data)));
  }

  public function test_differing_schemas_with_primitives()
  {
    $data_file = $this->add_data_file('data-prim.avr');

    $writer_schema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "verified", "type": "boolean", "default": "false"}
      ]}
JSON;
    $data = array(array('username' => 'john', 'age' => 25, 'verified' => true),
                  array('username' => 'ryan', 'age' => 23, 'verified' => false));
    $dw = AvroDataIO::open_file($data_file, 'w', $writer_schema);
    foreach ($data as $datum)
    {
      $dw->append($datum);
    }
    $dw->close();
    $reader_schema = <<<JSON
      { "type": "record",
        "name": "User",
        "fields" : [
      {"name": "username", "type": "string"}
      ]}
JSON;
    $dr = AvroDataIO::open_file($data_file, 'r', $reader_schema);
    foreach ($dr->data() as $index => $record)
    {
      $this->assertEquals($data[$index]['username'], $record['username']);
    }
  }

  public function test_differing_schemas_with_complex_objects()
  {
    $data_file = $this->add_data_file('data-complex.avr');

    $writers_schema = <<<JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON;

    $data = array(array("username" => "john",
                        "something_fixed" => "foo",
                        "something_enum" => "hello",
                        "something_array" => array(1,2,3),
                        "something_map" => array("a" => 1, "b" => 2),
                        "something_record" => array("inner" => 2),
                        "something_error" => array("code" => 403)),
                  array("username" => "ryan",
                        "something_fixed" => "bar",
                        "something_enum" => "goodbye",
                        "something_array" => array(1,2,3),
                        "something_map" => array("a" => 2, "b" => 6),
                        "something_record" => array("inner" => 1),
                        "something_error" => array("code" => 401)));
    $dw = AvroDataIO::open_file($data_file, 'w', $writers_schema);
    foreach ($data as $datum)
      $dw->append($datum);
    $dw->close();

    foreach (array('fixed', 'enum', 'record', 'error',
                   'array' , 'map', 'union') as $s)
    {
      $readers_schema = json_decode($writers_schema, true);
      $dr = AvroDataIO::open_file($data_file, 'r', json_encode($readers_schema));
      foreach ($dr->data() as $idx => $obj)
      {
        foreach ($readers_schema['fields'] as $field)
        {
          $field_name = $field['name'];
          $this->assertEquals($data[$idx][$field_name], $obj[$field_name]);
        }
      }
      $dr->close();

    }

  }

}
