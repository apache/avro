# -*- coding: utf-8 -*-
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

class TestLogicalTypes < Test::Unit::TestCase
  def test_int_date
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int", "logicalType": "date" }
    SCHEMA

    assert_encode_and_decode Date.today, schema
  end

  def test_int_date_conversion
    type = Avro::LogicalTypes::IntDate

    assert_equal 5, type.encode(Date.new(1970, 1, 6))
    assert_equal 0, type.encode(Date.new(1970, 1, 1))
    assert_equal -5, type.encode(Date.new(1969, 12, 27))

    assert_equal Date.new(1970, 1, 6), type.decode(5)
    assert_equal Date.new(1970, 1, 1), type.decode(0)
    assert_equal Date.new(1969, 12, 27), type.decode(-5)
  end

  def test_timestamp_millis_long
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "long", "logicalType": "timestamp-millis" }
    SCHEMA

    # The Time.at format is (seconds, microseconds) since Epoch.
    datum = Time.at(628232400, 12000)

    assert_encode_and_decode datum, schema
  end

  def test_timestamp_millis_long_conversion
    type = Avro::LogicalTypes::TimestampMillis

    assert_equal 1432849613221, type.encode(Time.utc(2015, 5, 28, 21, 46, 53, 221000))
    assert_equal Time.utc(2015, 5, 28, 21, 46, 53, 221000), type.decode(1432849613221)
  end

  def test_timestamp_micros_long
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "long", "logicalType": "timestamp-micros" }
    SCHEMA

    # The Time.at format is (seconds, microseconds) since Epoch.
    datum = Time.at(628232400, 12345)

    assert_encode_and_decode datum, schema
  end

  def encode(datum, schema)
    buffer = StringIO.new("")
    encoder = Avro::IO::BinaryEncoder.new(buffer)

    datum_writer = Avro::IO::DatumWriter.new(schema)
    datum_writer.write(datum, encoder)

    buffer.string
  end

  def decode(encoded, schema)
    buffer = StringIO.new(encoded)
    decoder = Avro::IO::BinaryDecoder.new(buffer)

    datum_reader = Avro::IO::DatumReader.new(schema, schema)
    datum_reader.read(decoder)
  end

  def assert_encode_and_decode(datum, schema)
    encoded = encode(datum, schema)
    assert_equal datum, decode(encoded, schema)
  end
end
