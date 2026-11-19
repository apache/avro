# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'
require 'avro/message'

class TestMessage < Test::Unit::TestCase
  def test_decoder_without_reader_schema
    writer_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "name", "type": "string"},
          {"name": "favorite_number",  "type": ["int", "null"]},
          {"name": "favorite_color", "type": ["string", "null"]}
        ]
      }
    JSON

    schema_store = Avro::Message::SchemaStore.new
    schema_store.add_schema(writer_schema)

    message_writer = Avro::Message::BinaryMessageEncoder.new(writer_schema)
    message_reader = Avro::Message::BinaryMessageDecoder.new(schema_store)

    datum_to_encode = { "name" => "Bob", "favorite_number" => 1, "favorite_color" => "Blue" }
    encoded_datum = message_writer.encode(datum_to_encode)
    decoded_datum = message_reader.decode(encoded_datum)

    assert_equal(["C3", "01", "B2", "D1", "D8", "D3", "DE", "28", "33", "CE", "06", "42", "6F", "62", "00", "02", "00", "08", "42", "6C", "75", "65"].map { |hex| hex.to_i(16) },
      encoded_datum.bytes)
    assert_equal(datum_to_encode, decoded_datum)
  end

  def test_decoder_with_reader_schema
    writer_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "name", "type": "string"},
          {"name": "favorite_number",  "type": ["int", "null"]},
          {"name": "favorite_color", "type": ["string", "null"]}
        ]
      }
    JSON

    reader_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "name", "type": "string"}
        ]
      }
    JSON

    schema_store = Avro::Message::SchemaStore.new
    schema_store.add_schema(writer_schema)

    message_writer = Avro::Message::BinaryMessageEncoder.new(writer_schema)
    message_reader = Avro::Message::BinaryMessageDecoder.new(schema_store, reader_schema)

    datum_to_encode = { "name" => "Bob", "favorite_number" => 1, "favorite_color" => "Blue" }
    encoded_datum = message_writer.encode(datum_to_encode)
    decoded_datum = message_reader.decode(encoded_datum)

    assert_equal({ "name" => "Bob" }, decoded_datum)
  end

  def test_decoder_with_incompatible_reader_schema
    writer_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "name", "type": "string"},
          {"name": "favorite_number",  "type": ["int", "null"]},
          {"name": "favorite_color", "type": ["string", "null"]}
        ]
      }
    JSON

    reader_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "field_not_in_written_datum", "type": "string"}
        ]
      }
    JSON

    schema_store = Avro::Message::SchemaStore.new
    schema_store.add_schema(writer_schema)

    message_writer = Avro::Message::BinaryMessageEncoder.new(writer_schema)
    message_reader = Avro::Message::BinaryMessageDecoder.new(schema_store, reader_schema)

    datum_to_encode = { "name" => "Bob", "favorite_number" => 1, "favorite_color" => "Blue" }
    encoded_datum = message_writer.encode(datum_to_encode)
    exception = assert_raise(Avro::AvroError) do
      message_reader.decode(encoded_datum)
    end

    assert_match(/Missing data for "string" with no default/, exception.to_s)
  end

  def test_missing_schema
    writer_schema = Avro::Schema.parse(<<-JSON)
      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "name", "type": "string"},
          {"name": "favorite_number",  "type": ["int", "null"]},
          {"name": "favorite_color", "type": ["string", "null"]}
        ]
      }
    JSON

    schema_store = Avro::Message::SchemaStore.new

    message_writer = Avro::Message::BinaryMessageEncoder.new(writer_schema)
    message_reader = Avro::Message::BinaryMessageDecoder.new(schema_store)

    datum_to_encode = { "name" => "Bob", "favorite_number" => 1, "favorite_color" => "Blue" }
    encoded_datum = message_writer.encode(datum_to_encode)
    exception = assert_raise(Avro::Message::MissingSchemaError) do
      message_reader.decode(encoded_datum)
    end

    assert_match(/Cannot resolve schema for fingerprint: 14858264533127451058/, exception.to_s)
  end

  def test_bad_header
    schema_store = Avro::Message::SchemaStore.new
    message_reader = Avro::Message::BinaryMessageDecoder.new(schema_store)
    exception = assert_raise(Avro::Message::BadHeaderError) do
      message_reader.decode("invalid-data")
    end

    assert_match(/Unrecognized header bytes \[\"69\", \"6E\"\]/, exception.to_s)
  end
end
