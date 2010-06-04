# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'

class TestDataFile < Test::Unit::TestCase
  HERE = File.expand_path File.dirname(__FILE__)
  def setup
    if File.exists?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def teardown
    if File.exists?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def test_differing_schemas_with_primitives
    writer_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "verified", "type": "boolean", "default": "false"}
  ]}
JSON

    data = [{"username" => "john", "age" => 25, "verified" => true},
            {"username" => "ryan", "age" => 23, "verified" => false}]

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      data.each{|h| dw << h }
    end

    # extract the username only from the avro serialized file
    reader_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"}
 ]}
JSON

    Avro::DataFile.open('data.avr', 'r', reader_schema) do |dr|
      dr.each_with_index do |record, i|
        assert_equal data[i]['username'], record['username']
      end
    end
  end

  def test_differing_schemas_with_complex_objects
    writer_schema = <<-JSON
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
JSON

    data = [{"username" => "john",
              "something_fixed" => "foo",
              "something_enum" => "hello",
              "something_array" => [1,2,3],
              "something_map" => {"a" => 1, "b" => 2},
              "something_record" => {"inner" => 2},
              "something_error" => {"code" => 403}
            },
            {"username" => "ryan",
              "something_fixed" => "bar",
              "something_enum" => "goodbye",
              "something_array" => [1,2,3],
              "something_map" => {"a" => 2, "b" => 6},
              "something_record" => {"inner" => 1},
              "something_error" => {"code" => 401}
            }]

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      data.each{|d| dw << d }
    end

    %w[fixed enum record error array map union].each do |s|
      reader = Yajl.load(writer_schema)
      reader['fields'] = reader['fields'].reject{|f| f['type']['type'] == s}
      Avro::DataFile.open('data.avr', 'r', Yajl.dump(reader)) do |dr|
        dr.each_with_index do |obj, i|
          reader['fields'].each do |field|
            assert_equal data[i][field['name']], obj[field['name']]
          end
        end
      end
    end
  end
end
