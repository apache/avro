#!/usr/bin/env python

##
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

"""Read/Write Avro File Object Containers."""

from __future__ import absolute_import, division, print_function

import io
import os
import random
import zlib

import avro.io
import avro.schema

try:
  import snappy
  has_snappy = True
except ImportError:
  has_snappy = False
try:
  import zstandard as zstd
  has_zstandard = True
except ImportError:
  has_zstandard = False
#
# Constants
#

VERSION = 1
MAGIC = 'Obj' + chr(VERSION)
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
SYNC_INTERVAL = 4000 * SYNC_SIZE # TODO(hammer): make configurable
META_SCHEMA = avro.schema.parse("""\
{"type": "record", "name": "org.apache.avro.file.Header",
 "fields" : [
   {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": %d}},
   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
   {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": %d}}]}
""" % (MAGIC_SIZE, SYNC_SIZE))
VALID_CODECS = ['null', 'deflate']
if has_snappy:
    VALID_CODECS.append('snappy')
if has_zstandard:
    VALID_CODECS.append('zstandard')
VALID_ENCODINGS = ['binary'] # not used yet

CODEC_KEY = "avro.codec"
SCHEMA_KEY = "avro.schema"

#
# Exceptions
#

class DataFileException(avro.schema.AvroException):
  """
  Raised when there's a problem reading or writing file object containers.
  """
  def __init__(self, fail_msg):
    avro.schema.AvroException.__init__(self, fail_msg)

#
# Write Path
#

class _DataFile(object):
  """Mixin for methods common to both reading and writing."""

  block_count = 0
  _meta = None
  _sync_marker = None

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  def get_meta(self, key):
    return self.meta.get(key)

  def set_meta(self, key, val):
    self.meta[key] = val

  @property
  def sync_marker(self):
    return self._sync_marker

  @property
  def meta(self):
    """Read-only dictionary of metadata for this datafile."""
    if self._meta is None:
      self._meta = {}
    return self._meta

  @property
  def codec(self):
    """Meta are stored as bytes, but codec is returned as a string."""
    return self.get_meta(CODEC_KEY).decode()

  @codec.setter
  def codec(self, value):
    """Meta are stored as bytes, but codec is set as a string."""
    if value not in VALID_CODECS:
      raise DataFileException("Unknown codec: {!r}".format(value))
    self.set_meta(CODEC_KEY, value.encode())

  @property
  def schema(self):
    """Meta are stored as bytes, but schema is returned as a string."""
    return self.get_meta(SCHEMA_KEY).decode()

  @schema.setter
  def schema(self, value):
    """Meta are stored as bytes, but schema is set as a string."""
    self.set_meta(SCHEMA_KEY, value.encode())


class DataFileWriter(_DataFile):

  # TODO(hammer): make 'encoder' a metadata property
  def __init__(self, writer, datum_writer, writers_schema=None, codec='null'):
    """
    If the schema is not present, presume we're appending.

    @param writer: File-like object to write into.
    """
    self._writer = writer
    self._encoder = avro.io.BinaryEncoder(writer)
    self._datum_writer = datum_writer
    self._buffer_writer = io.BytesIO()
    self._buffer_encoder = avro.io.BinaryEncoder(self._buffer_writer)
    self.block_count = 0
    self._header_written = False

    if writers_schema is not None:
      self._sync_marker = generate_sixteen_random_bytes()
      self.codec = codec
      self.schema = str(writers_schema)
      self.datum_writer.writers_schema = writers_schema
    else:
      # open writer for reading to collect metadata
      dfr = DataFileReader(writer, avro.io.DatumReader())

      # TODO(hammer): collect arbitrary metadata
      # collect metadata
      self._sync_marker = dfr.sync_marker
      self.codec = dfr.codec

      # get schema used to write existing file
      self.schema = schema_from_file = dfr.schema
      self.datum_writer.writers_schema = avro.schema.parse(schema_from_file)

      # seek to the end of the file and prepare for writing
      writer.seek(0, 2)
      self._header_written = True

  # read-only properties
  writer = property(lambda self: self._writer)
  encoder = property(lambda self: self._encoder)
  datum_writer = property(lambda self: self._datum_writer)
  buffer_writer = property(lambda self: self._buffer_writer)
  buffer_encoder = property(lambda self: self._buffer_encoder)

  def _write_header(self):
    header = {'magic': MAGIC,
              'meta': self.meta,
              'sync': self.sync_marker}
    self.datum_writer.write_data(META_SCHEMA, header, self.encoder)
    self._header_written = True

  # TODO(hammer): make a schema for blocks and use datum_writer
  def _write_block(self):
    if not self._header_written:
      self._write_header()

    if self.block_count > 0:
      # write number of items in block
      self.encoder.write_long(self.block_count)

      # write block contents
      uncompressed_data = self.buffer_writer.getvalue()
      if self.get_meta(CODEC_KEY) == 'null':
        compressed_data = uncompressed_data
        compressed_data_length = len(compressed_data)
      elif self.get_meta(CODEC_KEY) == 'deflate':
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(uncompressed_data)[2:-1]
        compressed_data_length = len(compressed_data)
      elif self.get_meta(CODEC_KEY) == 'snappy':
        compressed_data = snappy.compress(uncompressed_data)
        compressed_data_length = len(compressed_data) + 4 # crc32
      elif self.get_meta(CODEC_KEY) == 'zstandard':
        compressed_data = zstd.ZstdCompressor().compress(uncompressed_data)
        compressed_data_length = len(compressed_data)
      else:
        fail_msg = '"%s" codec is not supported.' % self.codec
        raise DataFileException(fail_msg)

      # Write length of block
      self.encoder.write_long(compressed_data_length)

      # Write block
      self.writer.write(compressed_data)

      # Write CRC32 checksum for Snappy
      if self.get_meta(CODEC_KEY) == 'snappy':
        self.encoder.write_crc32(uncompressed_data)

      # write sync marker
      self.writer.write(self.sync_marker)

      # reset buffer
      self.buffer_writer.truncate(0)
      self.block_count = 0

  def append(self, datum):
    """Append a datum to the file."""
    self.datum_writer.write(datum, self.buffer_encoder)
    self.block_count += 1

    # if the data to write is larger than the sync interval, write the block
    if self.buffer_writer.tell() >= SYNC_INTERVAL:
      self._write_block()

  def sync(self):
    """
    Return the current position as a value that may be passed to
    DataFileReader.seek(long). Forces the end of the current block,
    emitting a synchronization marker.
    """
    self._write_block()
    return self.writer.tell()

  def flush(self):
    """Flush the current state of the file, including metadata."""
    self._write_block()
    self.writer.flush()

  def close(self):
    """Close the file."""
    self.flush()
    self.writer.close()

class DataFileReader(_DataFile):
  """Read files written by DataFileWriter."""
  # TODO(hammer): allow user to specify expected schema?
  # TODO(hammer): allow user to specify the encoder
  def __init__(self, reader, datum_reader):
    self._reader = reader
    self._raw_decoder = avro.io.BinaryDecoder(reader)
    self._datum_decoder = None # Maybe reset at every block.
    self._datum_reader = datum_reader

    # read the header: magic, meta, sync
    self._read_header()

    # ensure codec is valid
    self.codec = self.get_meta('avro.codec')
    if self.codec is None:
      self.codec = "null"
    if self.codec not in VALID_CODECS:
      raise DataFileException('Unknown codec: %s.' % self.codec)

    # get file length
    self._file_length = self.determine_file_length()

    # get ready to read
    self.block_count = 0
    self.datum_reader.writers_schema = avro.schema.parse(self.schema)

  def __iter__(self):
    return self

  # read-only properties
  reader = property(lambda self: self._reader)
  raw_decoder = property(lambda self: self._raw_decoder)
  datum_decoder = property(lambda self: self._datum_decoder)
  datum_reader = property(lambda self: self._datum_reader)
  file_length = property(lambda self: self._file_length)

  def determine_file_length(self):
    """
    Get file length and leave file cursor where we found it.
    """
    remember_pos = self.reader.tell()
    self.reader.seek(0, 2)
    file_length = self.reader.tell()
    self.reader.seek(remember_pos)
    return file_length

  def is_EOF(self):
    return self.reader.tell() == self.file_length

  def _read_header(self):
    # seek to the beginning of the file to get magic block
    self.reader.seek(0, 0)

    # read header into a dict
    header = self.datum_reader.read_data(
      META_SCHEMA, META_SCHEMA, self.raw_decoder)

    # check magic number
    if header.get('magic') != MAGIC:
      fail_msg = "Not an Avro data file: %s doesn't match %s."\
                 % (header.get('magic'), MAGIC)
      raise avro.schema.AvroException(fail_msg)

    # set metadata
    self._meta = header['meta']

    # set sync marker
    self._sync_marker = header['sync']

  def _read_block_header(self):
    self.block_count = self.raw_decoder.read_long()
    if self.codec == "null":
      # Skip a long; we don't need to use the length.
      self.raw_decoder.skip_long()
      self._datum_decoder = self._raw_decoder
    elif self.codec == 'deflate':
      # Compressed data is stored as (length, data), which
      # corresponds to how the "bytes" type is encoded.
      data = self.raw_decoder.read_bytes()
      # -15 is the log of the window size; negative indicates
      # "raw" (no zlib headers) decompression.  See zlib.h.
      uncompressed = zlib.decompress(data, -15)
      self._datum_decoder = avro.io.BinaryDecoder(io.BytesIO(uncompressed))
    elif self.codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = self.raw_decoder.read_long()
      data = self.raw_decoder.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._datum_decoder = avro.io.BinaryDecoder(io.BytesIO(uncompressed))
      self.raw_decoder.check_crc32(uncompressed);
    elif self.codec == 'zstandard':
      length = self.raw_decoder.read_long()
      data = self.raw_decoder.read(length)
      uncompressed = bytearray()
      dctx = zstd.ZstdDecompressor()
      with dctx.stream_reader(io.BytesIO(data)) as reader:
        while True:
          chunk = reader.read(16384)
          if not chunk:
            break
          uncompressed.extend(chunk)
      self._datum_decoder = avro.io.BinaryDecoder(io.BytesIO(uncompressed))
    else:
      raise DataFileException("Unknown codec: %r" % self.codec)

  def _skip_sync(self):
    """
    Read the length of the sync marker; if it matches the sync marker,
    return True. Otherwise, seek back to where we started and return False.
    """
    proposed_sync_marker = self.reader.read(SYNC_SIZE)
    if proposed_sync_marker != self.sync_marker:
      self.reader.seek(-SYNC_SIZE, 1)
      return False
    return True

  def __next__(self):
    """Return the next datum in the file."""
    while self.block_count == 0:
      if self.is_EOF() or (self._skip_sync() and self.is_EOF()):
        raise StopIteration
      self._read_block_header()

    datum = self.datum_reader.read(self.datum_decoder)
    self.block_count -= 1
    return datum
  next = __next__

  def close(self):
    """Close this reader."""
    self.reader.close()


def generate_sixteen_random_bytes():
  try:
    return os.urandom(16)
  except NotImplementedError:
    return [chr(random.randrange(256)) for i in range(16)]
