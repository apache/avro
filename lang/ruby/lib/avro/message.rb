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

module Avro
  module Message
    ENCODING_MODE = "BINARY".freeze
    FINGERPRINT_PACK_MODE = "Q<".freeze
    MAGIC_NUMBER_PACK_MODE = "C*".freeze
    READ_MODE = "r".freeze
    SINGLE_OBJECT_MAGIC_NUMBER = [0xC3, 0x01].freeze
    WRITE_MODE = "w".freeze

    class BadHeaderError < AvroError; end
    class MissingSchemaError < AvroError; end

    class BinaryMessageEncoder
      def initialize(schema)
        @schema = schema
        @writer = Avro::IO::DatumWriter.new(schema)
      end

      def encode(datum)
        buffer = StringIO.new("", WRITE_MODE)
        buffer.set_encoding(ENCODING_MODE) if buffer.respond_to?(:set_encoding)

        encoder = Avro::IO::BinaryEncoder.new(buffer)

        buffer.write(SINGLE_OBJECT_MAGIC_NUMBER.pack(MAGIC_NUMBER_PACK_MODE))
        buffer.write([@schema.crc_64_avro_fingerprint].pack(FINGERPRINT_PACK_MODE))
        @writer.write(datum, encoder)

        buffer.string
      end
    end

    class BinaryMessageDecoder
      def initialize(schema_store, reader_schema = nil)
        @schema_store = schema_store
        @reader_schema = reader_schema
      end

      def decode(encoded_datum)
        buffer = StringIO.new(encoded_datum, READ_MODE)
        magic_number = buffer.read(2).unpack(MAGIC_NUMBER_PACK_MODE)

        if magic_number != SINGLE_OBJECT_MAGIC_NUMBER
          raise BadHeaderError.new("Unrecognized header bytes #{magic_number.map { |i| i.to_s(16).rjust(2, "0").upcase }}")
        end

        writer_schema_fingerprint = buffer.read(8).unpack(FINGERPRINT_PACK_MODE).first
        writer_schema = @schema_store.find_by_fingerprint(writer_schema_fingerprint)

        if !writer_schema
          raise MissingSchemaError.new("Cannot resolve schema for fingerprint: #{writer_schema_fingerprint}")
        end

        decoder = Avro::IO::BinaryDecoder.new(buffer)

        reader = Avro::IO::DatumReader.new(writer_schema, @reader_schema)
        reader.read(decoder)
      end
    end

    class SchemaStore
      def self.new
        Cache.new
      end

      class Cache
        def initialize
          @schemas = {}
        end

        def add_schema(schema)
          @schemas[schema.crc_64_avro_fingerprint] = schema
        end

        def find_by_fingerprint(fingerprint)
          @schemas[fingerprint]
        end
      end
    end
  end
end
