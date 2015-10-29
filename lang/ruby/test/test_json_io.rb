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
require 'avro/json_io'

class TestJsonIO < Test::Unit::TestCase
  Schema = Avro::Schema

  def test_null
    null_schema = '"null"'
    check(null_schema)
    check_default(null_schema, "null", nil)
    check_invalid(null_schema, "foo")
  end

  def test_boolean
    boolean_schema = '"boolean"'
    check(boolean_schema)
    check_default(boolean_schema, "true", true)
    check_default(boolean_schema, "false", false)
    check_invalid(boolean_schema, "foo")
  end

  def test_string
    string_schema = '"string"'
    check(string_schema)
    check_default(string_schema, '"foo"', "foo")
    check_invalid(string_schema, 123)
  end

  def test_bytes
    schema = '"bytes"'
    schm = Avro::Schema.parse schema

    datum = ''
    (0..255).each do |n|
      datum += [n].pack('C*')
    end

    w = Avro::IO::JsonDatumWriter.new(schm)
    writer = StringIO.new "", "wb"
    w.write(datum, Avro::IO::JsonEncoder.new(writer))

    r = datum_reader(schm)
    reader = StringIO.new(writer.string)
    ob = r.read(Avro::IO::JsonDecoder.new.io_reader(reader))

    assert_equal(datum.bytes, ob.bytes)
  end

  def test_int
    int_schema = '"int"'
    check(int_schema)
    check_default(int_schema, "5", 5)
    check_invalid(int_schema, "foo")
  end

  def test_out_of_range_int
    schema = '"int"'
    avro_schema = Avro::Schema.parse(schema)
    bad_datum = Avro::Schema::INT_MAX_VALUE + 1
    error_message = "The datum #{bad_datum} is not an example of schema \"int\""

    exception = assert_raises(Avro::IO::AvroTypeError) do
      write_datum(bad_datum, avro_schema)
    end
    assert_equal error_message, exception.message

    exception = assert_raises(Avro::IO::AvroTypeError) do
      read_datum(StringIO.new(JSON.dump(bad_datum)), avro_schema)
    end

    assert_equal error_message, exception.message
  end

  def test_long
    long_schema = '"long"'
    check(long_schema)
    check_default(long_schema, "9", 9)
    check_invalid(long_schema, "foo")
  end

  def test_out_of_range_long
    schema = '"long"'
    avro_schema = Avro::Schema.parse(schema)
    bad_datum = Avro::Schema::LONG_MAX_VALUE + 1
    error_message = "The datum #{bad_datum} is not an example of schema \"long\""

    exception = assert_raises(Avro::IO::AvroTypeError) do
      write_datum(bad_datum, avro_schema)
    end
    assert_equal error_message, exception.message

    exception = assert_raises(Avro::IO::AvroTypeError) do
      read_datum(StringIO.new(JSON.dump(bad_datum)), avro_schema)
    end

    assert_equal error_message, exception.message
  end

  def test_float
    float_schema = '"float"'
    check(float_schema)
    check_default(float_schema, "1.2", 1.2)
    check_invalid(float_schema, "foo")
  end

  def test_double
    double_schema = '"double"'
    check(double_schema)
    check_default(double_schema, "1.2", 1.2)
    check_invalid(double_schema, "foo")
  end

  def test_array
    array_schema = '{"type": "array", "items": "long"}'
    check(array_schema)
    check_default(array_schema, "[1]", [1])
    check_invalid(array_schema, ['foo'])
  end

  def test_map
    map_schema = '{"type": "map", "values": "long"}'
    check(map_schema)
    check_default(map_schema, '{"a": 1}', {"a" => 1})
    check_invalid(map_schema, {'foo' => 'bar'})
  end

  def test_record
    record_schema = <<EOS
      {"type": "record",
       "name": "Test",
       "fields": [{"name": "f",
                   "type": "long"}]}
EOS
    check(record_schema)
    check_default(record_schema, '{"f": 11}', {"f" => 11})
    check_invalid(record_schema, "foo")
  end

  def test_error
    error_schema = <<EOS
      {"type": "error",
       "name": "TestError",
       "fields": [{"name": "message",
                   "type": "string"}]}
EOS
    check(error_schema)
    check_default(error_schema, '{"message": "boom"}', {"message" => "boom"})
  end

  def test_enum
    enum_schema = '{"type": "enum", "name": "Test","symbols": ["A", "B"]}'
    check(enum_schema)
    check_default(enum_schema, '"B"', "B")
    check_invalid(enum_schema, "C")
  end

  def test_recursive
    recursive_schema = <<EOS
      {"type": "record",
       "name": "Node",
       "fields": [{"name": "label", "type": "string"},
                  {"name": "children",
                   "type": {"type": "array", "items": "Node"}}]}
EOS
    check(recursive_schema)
  end

  def test_union
    union_schema = <<EOS
      ["string",
       "null",
       "long",
       {"type": "record",
        "name": "Cons",
        "fields": [{"name": "car", "type": "string"},
                   {"name": "cdr", "type": "string"}]}]
EOS
    check(union_schema)
    check_default('["double", "long"]', "1.1", 1.1)
    check_invalid(union_schema, {'float' => 123.56})
  end

  def test_lisp
    lisp_schema = <<EOS
      {"type": "record",
       "name": "Lisp",
       "fields": [{"name": "value",
                   "type": ["null", "string",
                            {"type": "record",
                             "name": "Cons",
                             "fields": [{"name": "car", "type": "Lisp"},
                                        {"name": "cdr", "type": "Lisp"}]}]}]}
EOS
    check(lisp_schema)
  end

  def test_fixed
    fixed_schema = '{"type": "fixed", "name": "Test", "size": 1}'
    check(fixed_schema)
    check_default(fixed_schema, '"a"', "a")
    check_invalid(fixed_schema, 123.56)
  end

  def test_invalid_fixed_length
    schema = JSON.dump({"type" => "fixed", "name" => "Test", "size" => 1})
    fixed_schema = Avro::Schema.parse(schema)
    bad_datum = 'def'
    error_message = "The datum \"#{bad_datum}\" is not an example of schema #{schema}"

    exception = assert_raises(Avro::IO::AvroTypeError) do
      write_datum(bad_datum, fixed_schema)
    end
    assert_equal error_message, exception.message

    exception = assert_raises(Avro::IO::AvroTypeError) do
      read_datum(StringIO.new(JSON.dump(bad_datum)), fixed_schema)
    end

    assert_equal error_message, exception.message
  end

  def test_enum_with_duplicate
    str = '{"type": "enum", "name": "Test","symbols" : ["AA", "AA"]}'
    assert_raises(Avro::SchemaParseError) do
      schema = Avro::Schema.parse str
    end
  end

  def test_schema_promotion
    promotable_schemas = ['"int"', '"long"', '"float"', '"double"']
    incorrect = 0
    promotable_schemas.each_with_index do |ws, i|
      writers_schema = Avro::Schema.parse(ws)
      datum_to_write = 219
      for rs in promotable_schemas[(i + 1)..-1]
        readers_schema = Avro::Schema.parse(rs)
        writer, enc, dw = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        if datum_read != datum_to_write
          incorrect += 1
        end
      end
      assert_equal(incorrect, 0)
    end
  end

  def test_ordering
    datum = JSON.dump({b: 2, a: 1})
    schema = JSON.dump({type: "record", name: 'ab', fields: [{name: 'a', type: 'int'}, {name: 'b', type: 'int'}]})
    avro_schema = Avro::Schema.parse(schema)

    expected = {'a' => 1, 'b' => 2}
    actual = read_datum(StringIO.new(datum), avro_schema)
    assert_equal expected, actual
  end

  def test_excess_fields
    datum = JSON.dump({"b" => {"b3" => 1.4, "b2" => 3.14, "b1" => "h"}, "a" => {"a0" => 45, "a2" => true, "a1" => nil}})
    schema = JSON.dump({"type" => "record", "name" => "ab", "fields" => [{
      "name" => "a", "type" => {
        "type" => "record",
        "name" => "A",
        "fields" => [{"name" => "a1", "type" => "null"}, {"name" => "a2", "type" => "boolean"}]}
      },
      {
        "name" => "b", "type" => {
          "type" => "record",
          "name" => "B",
          "fields" => [
            {"name" => "b1", "type" => "string"},
            {"name" => "b2", "type" => "float"},
            {"name" => "b3", "type" => "double"}
          ]
        }
      }]})
    avro_schema = Avro::Schema.parse(schema)

    expected = {"a" => {"a1" => nil, "a2" => true}, "b" => {"b1" => "h", "b2" => 3.14, "b3" => 1.4}}
    actual = read_datum(StringIO.new(datum), avro_schema)
    assert_equal expected, actual
  end

  def test_record_ordering_with_projection
    datum = JSON.dump({"b" => {"b3" => 1.4, "b2" => 3.14, "b1" => "h"}, "a" => {"a2" => true, "a1" => nil}})
    reader_schema = JSON.dump({
      "type" => "record",
      "name" => "ab",
      "fields" => [{
        "name" => "a",
        "type" => {
          "type" => "record",
          "name" => "A",
          "fields" => [{ "name" => "a1", "type" => "null" }, { "name" => "a2", "type" => "boolean" }]
        }
      }]
    })
    writer_schema = JSON.dump({
      "type" => "record",
      "name" => "ab",
      "fields" => [
        {
          "name" => "a",
          "type" => {
            "type" => "record",
            "name" => "A",
            "fields" => [
              { "name" => "a1", "type" => "null" },
              { "name" => "a2", "type" => "boolean" }
            ]
          }
        },
        {
          "name" => "b",
          "type" => {
            "type" => "record",
            "name" => "B",
            "fields" => [
              { "name" => "b1", "type" => "string" },
              { "name" => "b2", "type" => "float" },
              { "name" => "b3", "type" => "double" }
            ]
          }
        }
      ]
    })

    writer_avro_schema = Avro::Schema.parse(writer_schema)
    reader_avro_schema = Avro::Schema.parse(reader_schema)
    expected = {"a" => {"a1" => nil, "a2" => true}}
    actual = read_datum(StringIO.new(datum), writer_avro_schema, reader_avro_schema)
    assert_equal expected, actual
  end

  def test_record_ordering_with_projection_2
    datum = JSON.dump({"b" => {"b1" => "h", "b2" => [3.14, 3.56], "b3" => 1.4}, "a" => {"a2" => true, "a1" => nil}})
    reader_schema = JSON.dump({
      "type" => "record",
      "name" => "ab",
      "fields" => [{
        "name" => "a",
        "type" => {
          "type" => "record",
          "name" => "A",
          "fields" => [{"name" => "a1", "type" => "null"}, {"name" => "a2", "type" => "boolean"}]
        }
      }]
    })
    writer_schema = JSON.dump({
      "type" => "record",
      "name" => "ab",
      "fields" => [
        {
          "name" => "a",
          "type" => {
            "type" => "record",
            "name" => "A",
            "fields" => [{"name" => "a1", "type" => "null"}, {"name" => "a2", "type" => "boolean"}]
          }
        },
        {
          "name" => "b",
          "type" => {
            "type" => "record",
            "name" => "B",
            "fields" => [
              {"name" => "b1", "type" => "string"},
              {
                "name" => "b2",
                "type" => {
                  "type" => "array",
                  "items" => "float"
                }
              },
              {"name" => "b3", "type" => "double"}
            ]
          }
        }
      ]
    })

    writer_avro_schema = Avro::Schema.parse(writer_schema)
    reader_avro_schema = Avro::Schema.parse(reader_schema)

    expected = {"a" => {"a1" => nil, "a2" => true}}
    actual = read_datum(StringIO.new(datum), writer_avro_schema, reader_avro_schema)
    assert_equal expected, actual
  end

  private

  def check_invalid(schema_json, bad_datum)
    avro_schema = Avro::Schema.parse(schema_json)

    assert_raises(Avro::IO::AvroTypeError) { write_datum(bad_datum, avro_schema) }

    assert_raises(Avro::IO::AvroTypeError) { read_datum(StringIO.new(JSON.dump(bad_datum)), avro_schema) }
  end

  def check_default(schema_json, default_json, default_value)
    actual_schema = '{"type": "record", "name": "Foo", "fields": []}'
    actual = Avro::Schema.parse(actual_schema)

    expected_schema = <<EOS
      {"type": "record",
       "name": "Foo",
       "fields": [{"name": "f", "type": #{schema_json}, "default": #{default_json}}]}
EOS
    expected = Avro::Schema.parse(expected_schema)

    reader = Avro::IO::JsonDatumReader.new(actual, expected)
    record = reader.read(Avro::IO::JsonDecoder.new.io_reader(StringIO.new))
    assert_equal default_value, record["f"]
  end

  def check(str)
    # parse schema, then convert back to string
    schema = Avro::Schema.parse str

    parsed_string = schema.to_s

     # test that the round-trip didn't mess up anything
    # NB: I don't think we should do this. Why enforce ordering?
    assert_equal(MultiJson.load(str),
                  MultiJson.load(parsed_string))

    # test __eq__
    assert_equal(schema, Avro::Schema.parse(str))

    # test hashcode doesn't generate infinite recursion
    schema.hash

    # test serialization of random data
    randomdata = RandomData.new(schema)
    9.times { checkser(schema, randomdata) }
  end

  def checkser(schm, randomdata)
    datum = randomdata.next
    assert validate(schm, datum)
    w = Avro::IO::JsonDatumWriter.new(schm)
    writer = StringIO.new "", "w"
    w.write(datum, Avro::IO::JsonEncoder.new(writer))
    r = datum_reader(schm)
    reader = StringIO.new(writer.string)
    ob = r.read(Avro::IO::JsonDecoder.new.io_reader(reader))

    assert_equal(datum, ob)
  end

  def validate(schm, datum)
    Avro::Schema.validate(schm, datum)
  end

  def datum_writer(schm)
    Avro::IO::JsonDatumWriter.new(schm)
  end

  def datum_reader(schm)
    Avro::IO::JsonDatumReader.new(schm)
  end

  def write_datum(datum, writers_schema)
    writer = StringIO.new
    encoder = Avro::IO::JsonEncoder.new(writer)
    datum_writer = Avro::IO::JsonDatumWriter.new(writers_schema)
    datum_writer.write(datum, encoder)
    [writer, encoder, datum_writer]
  end

  def read_datum(buffer, writers_schema, readers_schema=nil)
    reader = StringIO.new(buffer.string)
    decoder = Avro::IO::JsonDecoder.new.io_reader(reader)
    datum_reader = Avro::IO::JsonDatumReader.new(writers_schema, readers_schema)
    datum_reader.read(decoder)
  end
end
