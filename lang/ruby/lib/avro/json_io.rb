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

require 'avro/io'
require 'json'

module Avro
  module IO
    # JsonDecoder keeps track of the data and reads it using a given data type.
    class JsonDecoder
      # memo-ized map of Schema::PrimitiveSchema objects so we're not allocating an object for every validation
      PRIMITIVE_SCHEMAS = Hash[Schema::PRIMITIVE_TYPES_SYM.map { |k| [k, Schema::PrimitiveSchema.new(k)] }]

      attr_accessor :data

      def io_reader(reader)
        datum = reader.read
        datum = '""' if datum.empty?
        data = JSON.parse(datum, {:quirks_mode => true})
        @data = data

        self
      end

      def read_null
        validate(:null, data)
        data
      end

      def read_boolean
        validate(:boolean, data)
        data
      end

      def read_int
        validate(:int, data)
        Integer(data)
      end

      def read_long
        validate(:long, data)
        Integer(data)
      end

      def read_float
        validate(:float, data)
        Float(data)
      end

      def read_double
        read_float
      end

      def read_bytes
        validate(:bytes, data)
        data.encode(Encoding::ISO_8859_1, Encoding::UTF_8)
      end

      def read_fixed(size)
        read_bytes
      end

      def read_string
        validate(:string, data)
        data ? data.encode(Encoding::UTF_8) : data
      end

      def validate(data_type, data)
        schema = PRIMITIVE_SCHEMAS[data_type]
        unless Schema.validate(schema, data)
          raise AvroTypeError.new(schema, data)
        end
      end
    end # JsonDecoder

    # Unlike DatumReader, JsonDatumReader reads the file first so that it can decode schema with the JSON data.
    # It uses JsonDecoder to keep track of current data as it is going through the schema.
    class JsonDatumReader < DatumReader
      def validate(schema, data)
        unless Schema.validate(schema, data)
          raise AvroTypeError.new(schema, data)
        end
      end

      def read_record_data(field, readers_field, decoder)
        field_data = decoder.data ? decoder.data[field.name] : nil
        field_decoder = decoder.class.new
        field_decoder.data = field_data
        read_data(field.type, readers_field.type, field_decoder)
      end

      def skip_data(writers_schema, decoder)
        # no-op
      end

      def read_record(writers_schema, readers_schema, decoder)
        record = super
        validate(readers_schema, record)
        record
      end

      def read_fixed(writers_schema, readers_schema, decoder)
        validate(readers_schema, decoder.data)

        decoder.read_fixed(writers_schema.size)
      end

      def read_enum(writers_schema, readers_schema, decoder)
        validate(readers_schema, decoder.data)

        decoder.data
      end

      def read_array(writers_schema, readers_schema, decoder)
        read_items = []
        decoder.data.each do |item|
          item_decoder = decoder.class.new
          item_decoder.data = item
          read_items << read_data(writers_schema.items, readers_schema.items, item_decoder)
        end if decoder.data

        validate(readers_schema, read_items)

        read_items
      end

      def read_map(writers_schema, readers_schema, decoder)
        read_items = {}
        decoder.data.each_pair do |key, value|
          value_decoder = decoder.class.new
          value_decoder.data = value
          read_items[key] = read_data(writers_schema.values, readers_schema.values, value_decoder)
        end

        validate(readers_schema, read_items)

        read_items
      end

      def read_union(writers_schema, readers_schema, decoder)
        if decoder.data.nil?
          schema_type, value = :null, nil
        else
          schema_type, value = decoder.data.first
        end

        data_schema = readers_schema.schemas.find do |reader_schema|
          reader_schema.type_sym == schema_type.to_sym ||
                                    (reader_schema.name == schema_type if reader_schema.respond_to?(:name))
        end

        raise AvroTypeError.new(readers_schema, value) unless data_schema

        value_decoder = decoder.class.new
        value_decoder.data = value
        field_value = read_data(data_schema, readers_schema, value_decoder)

        validate(data_schema, field_value)

        field_value
      end
    end # JsonDatumReader

    # JsonEncoder encodes a given datum and, when asked, writes it to the writer.
    class JsonEncoder
      attr_reader :writer
      attr_accessor :data

      def initialize(writer)
        @writer = writer
      end

      def write
        writer.write(JSON.dump(@data))
      end

      def write_null(datum)
        @data = nil
      end

      def write_boolean(datum)
        @data = datum
      end

      def write_int(n)
        write_long(n)
      end

      def write_long(n)
        @data = Integer(n)
      end

      def write_float(n)
        @data = Float(n)
      end

      def write_double(n)
        write_float(n)
      end

      def write_bytes(datum)
        @data = datum.encode(Encoding::UTF_8, Encoding::ISO_8859_1)
      end

      def write_fixed(datum)
        @data = write_bytes(datum)
      end

      def write_string(datum)
        @data = datum.encode(Encoding::UTF_8)
      end
    end # JsonEncoder

    # Unlike DatumWriter, JsonDatumWriter writes the encoded data in memory. At the end, it writes the data to the writer.
    class JsonDatumWriter < DatumWriter
      def write(datum, encoder)
        super
        encoder.write
      end

      def write_record(writers_schema, datum, encoder)
        record = {}
        writers_schema.fields.each do |field|
          field_encoder = encoder.class.new(encoder.writer)
          write_data(field.type, datum[field.name], field_encoder)
          record[field.name] = field_encoder.data
        end
        encoder.data = record
      end

      def write_array(writers_schema, datum, encoder)
        items = []
        datum.each do |item|
          item_encoder = encoder.class.new(encoder.writer)
          write_data(writers_schema.items, item, item_encoder)
          items << item_encoder.data
        end
        encoder.data = items
      end

      def write_map(writers_schema, datum, encoder)
        object = {}
        datum.each do |k,v|
          value_encoder = encoder.class.new(encoder.writer)
          write_data(writers_schema.values, v, value_encoder)
          object[k] = value_encoder.data
        end
        encoder.data = object
      end

      def write_union(writers_schema, datum, encoder)
        datum_schema = writers_schema.schemas.find {|schema| Schema.validate(schema, datum)}
        raise AvroTypeError.new(writers_schema, datum) unless datum_schema

        value_encoder = encoder.class.new(encoder.writer)
        write_data(datum_schema, datum, value_encoder)

        if datum_schema.type_sym == :null
          value_encoder.data
        else
          data = {}
          if datum_schema.respond_to?(:name)
            data[datum_schema.name] = value_encoder.data
          else
            data[datum_schema.type_sym.to_s] = value_encoder.data
          end
          encoder.data = data
        end
      end

      def write_fixed(writers_schema, datum, encoder)
        encoder.write_fixed(datum)
      end

      def write_enum(writers_schema, datum, encoder)
        encoder.write_string(datum.to_s)
      end
    end # JsonDatumWriter
  end
end
