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

module Avro
  module Message
    VERSION = 0

    class Writer
      def initialize(schema)
        @schema = schema
        @writer = Avro::IO::DatumWriter.new(schema)
      end

      def write(datum)
        buffer = StringIO.new('', 'w')
        buffer.set_encoding('BINARY') if buffer.respond_to?(:set_encoding)

        encoder = IO::BinaryEncoder.new(buffer)

        buffer.write([VERSION].pack("c"))
        @writer.write(datum, encoder)

        buffer.string
      end
    end

    class Reader
      def initialize(schema)
        @schema = schema
        @reader = Avro::IO::DatumReader.new(schema, schema)
      end

      def read(encoded_datum)
        buffer = StringIO.new(encoded_datum, "r")
        version = buffer.read(1).unpack("c").first

        if version != VERSION
          raise "unsupported version #{version}"
        end

        decoder = IO::BinaryDecoder.new(buffer)

        @reader.read(decoder)
      end
    end
  end
end
