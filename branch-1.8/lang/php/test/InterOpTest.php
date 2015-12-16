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

class InterOpTest extends PHPUnit_Framework_TestCase
{
  var $projection_json;
  var $projection;

  public function setUp()
  {
    $interop_schema_file_name = AVRO_INTEROP_SCHEMA;
    $this->projection_json = file_get_contents($interop_schema_file_name);
    $this->projection = AvroSchema::parse($this->projection_json);
  }

  public function file_name_provider()
  {
    $data_dir = AVRO_BUILD_DATA_DIR;
    $data_files = array();
    if (!($dh = opendir($data_dir)))
      die("Could not open data dir '$data_dir'\n");

    /* TODO This currently only tries to read files of the form 'language.avro',
     * but not 'language_deflate.avro' as the PHP implementation is not yet
     * able to read deflate data files. When deflate support is added, change
     * this to match *.avro. */
    while ($file = readdir($dh))
      if (0 < preg_match('/^[a-z]+\.avro$/', $file))
        $data_files []= join(DIRECTORY_SEPARATOR, array($data_dir, $file));
    closedir($dh);

    $ary = array();
    foreach ($data_files as $df)
      $ary []= array($df);
    return $ary;
  }

  /**
   *  @dataProvider file_name_provider
   */
  public function test_read($file_name)
  {

    $dr = AvroDataIO::open_file(
      $file_name, AvroFile::READ_MODE, $this->projection_json);

    $data = $dr->data();

    $this->assertNotEquals(0, count($data),
                           sprintf("no data read from %s", $file_name));

    foreach ($data as $idx => $datum)
      $this->assertNotNull($datum, sprintf("null datum from %s", $file_name));

  }

}
