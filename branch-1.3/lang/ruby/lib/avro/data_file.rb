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

require 'openssl'

module Avro
  module DataFile
    VERSION = 1
    MAGIC = "Obj" + [VERSION].pack('c')
    MAGIC_SIZE = MAGIC.size
    SYNC_SIZE = 16
    SYNC_INTERVAL = 1000 * SYNC_SIZE
    META_SCHEMA = Schema.parse('{"type": "map", "values": "bytes"}')
    VALID_CODECS = ['null']
    VALID_ENCODINGS = ['binary'] # not used yet

    class DataFileError < AvroError; end

    def self.open(file_path, mode='r', schema=nil)
      schema = Avro::Schema.parse(schema) if schema
      case mode
      when 'w'
        unless schema
          raise DataFileError, "Writing an Avro file requires a schema."
        end
        io = open_writer(File.open(file_path, 'wb'), schema)
      when 'r'
        io = open_reader(File.open(file_path, 'rb'), schema)
      else
        raise DataFileError, "Only modes 'r' and 'w' allowed. You gave #{mode.inspect}."
      end

      yield io if block_given?
      io
    ensure
      io.close if block_given? && io
    end

    class << self
      private
      def open_writer(file, schema)
        writer = Avro::IO::DatumWriter.new(schema)
        Avro::DataFile::Writer.new(file, writer, schema)
      end

      def open_reader(file, schema)
        reader = Avro::IO::DatumReader.new(nil, schema)
        Avro::DataFile::Reader.new(file, reader)
      end
    end

    class Writer
      def self.generate_sync_marker
        OpenSSL::Random.random_bytes(16)
      end

      attr_reader :writer, :encoder, :datum_writer, :buffer_writer, :buffer_encoder, :sync_marker, :meta
      attr_accessor :block_count

      def initialize(writer, datum_writer, writers_schema=nil)
        # If writers_schema is not present, presume we're appending
        @writer = writer
        @encoder = IO::BinaryEncoder.new(@writer)
        @datum_writer = datum_writer
        @buffer_writer = StringIO.new('', 'w')
        @buffer_encoder = IO::BinaryEncoder.new(@buffer_writer)
        @block_count = 0

        @meta = {}

        if writers_schema
          @sync_marker = Writer.generate_sync_marker
          meta['avro.codec'] = 'null'
          meta['avro.schema'] = writers_schema.to_s
          datum_writer.writers_schema = writers_schema
          write_header
        else
          # open writer for reading to collect metadata
          dfr = Reader.new(writer, Avro::IO::DatumReader.new)

          # FIXME(jmhodges): collect arbitrary metadata
          # collect metadata
          @sync_marker = dfr.sync_marker
          meta['avro.codec'] = dfr.meta['avro.codec']

          # get schema used to write existing file
          schema_from_file = dfr.meta['avro.schema']
          meta['avro.schema'] = schema_from_file
          datum_writer.writers_schema = Schema.parse(schema_from_file)

          # seek to the end of the file and prepare for writing
          writer.seek(0,2)
        end
      end

      # Append a datum to the file
      def <<(datum)
        datum_writer.write(datum, buffer_encoder)
        self.block_count += 1

        # if the data to write is larger than the sync interval, write
        # the block
        if buffer_writer.tell >= SYNC_INTERVAL
          write_block
        end
      end

      # Return the current position as a value that may be passed to
      # DataFileReader.seek(long). Forces the end of the current block,
      # emitting a synchronization marker.
      def sync
        write_block
        writer.tell
      end

      # Flush the current state of the file, including metadata
      def flush
        write_block
        writer.flush
      end

      def close
        flush
        writer.close
      end

      private

      def write_header
        # write magic
        writer.write(MAGIC)

        # write metadata
        datum_writer.write_data(META_SCHEMA, meta, encoder)

        # write sync marker
        writer.write(sync_marker)
      end

      # TODO(jmhodges): make a schema for blocks and use datum_writer
      # TODO(jmhodges): do we really need the number of items in the block?
      # TODO(jmhodges): use codec when writing the block contents
      def write_block
        if block_count > 0
          # write number of items in block and block size in bytes
          encoder.write_long(block_count)
          to_write = buffer_writer.string
          encoder.write_long(to_write.size)

          # write block contents
          if meta['avro.codec'] == 'null'
            writer.write(to_write)
          else
            msg = "#{meta['avro.codec'].inspect} coded is not supported"
            raise DataFileError, msg
          end

          # write sync marker
          writer.write(sync_marker)

          # reset buffer
          buffer_writer.truncate(0)
          self.block_count = 0
        end
      end
    end

    # Read files written by DataFileWriter
    class Reader
      include ::Enumerable

      attr_reader :reader, :decoder, :datum_reader, :sync_marker, :meta, :file_length
      attr_accessor :block_count

      def initialize(reader, datum_reader)
        @reader = reader
        @decoder = IO::BinaryDecoder.new(reader)
        @datum_reader = datum_reader

        # read the header: magic, meta, sync
        read_header

        # ensure the codec is valid
        codec_from_file = meta['avro.codec']
        if codec_from_file && ! VALID_CODECS.include?(codec_from_file)
          raise DataFileError, "Unknown codec: #{codec_from_file}"
        end

        # get ready to read
        @block_count = 0
        datum_reader.writers_schema = Schema.parse meta['avro.schema']
      end

      # Iterates through each datum in this file
      # TODO(jmhodges): handle block of length zero
      def each
        loop do
          if block_count == 0
            case
            when eof?; break
            when skip_sync
              break if eof?
              read_block_header
            else
              read_block_header
            end
          end

          datum = datum_reader.read(decoder)
          self.block_count -= 1
          yield(datum)
        end
      end

      def eof?; reader.eof?; end

      def close
        reader.close
      end

      private
      def read_header
        # seek to the beginning of the file to get magic block
        reader.seek(0, 0)

        # check magic number
        magic_in_file = reader.read(MAGIC_SIZE)
        if magic_in_file.size < MAGIC_SIZE
          msg = 'Not an Avro data file: shorter than the Avro magic block'
          raise DataFileError, msg
        elsif magic_in_file != MAGIC
          msg = "Not an Avro data file: #{magic_in_file.inspect} doesn't match #{MAGIC.inspect}"
          raise DataFileError, msg
        end

        # read metadata
        @meta = datum_reader.read_data(META_SCHEMA,
                                       META_SCHEMA,
                                       decoder)
        # read sync marker
        @sync_marker = reader.read(SYNC_SIZE)
      end

      def read_block_header
        self.block_count = decoder.read_long
        decoder.read_long # not doing anything with length in bytes
      end

      # read the length of the sync marker; if it matches the sync
      # marker, return true. Otherwise, seek back to where we started
      # and return false
      def skip_sync
        proposed_sync_marker = reader.read(SYNC_SIZE)
        if proposed_sync_marker != sync_marker
          reader.seek(-SYNC_SIZE, 1)
          false
        else
          true
        end
      end
    end
  end
end
