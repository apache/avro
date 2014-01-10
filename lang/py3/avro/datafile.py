#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read/Write Avro File Object Containers."""

import io
import logging
import os
import zlib

from avro import schema
from avro import io as avro_io

try:
  import snappy
  has_snappy = True
except ImportError:
  has_snappy = False


# ------------------------------------------------------------------------------
# Constants

# Version of the container file:
VERSION = 1

# Magic code that starts a data container file:
MAGIC = b'Obj' + bytes([VERSION])

# Size of the magic code, in number of bytes:
MAGIC_SIZE = len(MAGIC)

# Size of the synchronization marker, in number of bytes:
SYNC_SIZE = 16

# Interval between synchronization markers, in number of bytes:
# TODO: make configurable
SYNC_INTERVAL = 1000 * SYNC_SIZE

# Schema of the container header:
META_SCHEMA = schema.Parse("""
{
  "type": "record", "name": "org.apache.avro.file.Header",
  "fields": [{
    "name": "magic",
    "type": {"type": "fixed", "name": "magic", "size": %(magic_size)d}
  }, {
    "name": "meta",
    "type": {"type": "map", "values": "bytes"}
  }, {
    "name": "sync",
    "type": {"type": "fixed", "name": "sync", "size": %(sync_size)d}
  }]
}
""" % {
    'magic_size': MAGIC_SIZE,
    'sync_size': SYNC_SIZE,
})

# Codecs supported by container files:
VALID_CODECS = frozenset(['null', 'deflate'])
if has_snappy:
  VALID_CODECS = frozenset.union(VALID_CODECS, ['snappy'])

# Not used yet
VALID_ENCODINGS = frozenset(['binary'])

# Metadata key associated to the codec:
CODEC_KEY = "avro.codec"

# Metadata key associated to the schema:
SCHEMA_KEY = "avro.schema"


# ------------------------------------------------------------------------------
# Exceptions


class DataFileException(schema.AvroException):
  """Problem reading or writing file object containers."""

  def __init__(self, msg):
    super(DataFileException, self).__init__(msg)


# ------------------------------------------------------------------------------


class DataFileWriter(object):
  """Writes Avro data files."""

  @staticmethod
  def GenerateSyncMarker():
    """Generates a random synchronization marker."""
    return os.urandom(SYNC_SIZE)

  # TODO: make 'encoder' a metadata property
  def __init__(
      self,
      writer,
      datum_writer,
      writer_schema=None,
      codec='null',
  ):
    """Constructs a new DataFileWriter instance.

    If the schema is not present, presume we're appending.

    Args:
      writer: File-like object to write into.
      datum_writer:
      writer_schema: Schema
      codec:
    """
    self._writer = writer
    self._encoder = avro_io.BinaryEncoder(writer)
    self._datum_writer = datum_writer
    self._buffer_writer = io.BytesIO()
    self._buffer_encoder = avro_io.BinaryEncoder(self._buffer_writer)
    self._block_count = 0
    self._meta = {}

    # Ensure we have a writer that accepts bytes:
    self._writer.write(b'')

    # Whether the header has already been written:
    self._header_written = False

    if writer_schema is not None:
      if codec not in VALID_CODECS:
        raise DataFileException('Unknown codec: %r' % codec)
      self._sync_marker = DataFileWriter.GenerateSyncMarker()
      self.SetMeta('avro.codec', codec)
      self.SetMeta('avro.schema', str(writer_schema).encode('utf-8'))
      self.datum_writer.writer_schema = writer_schema
    else:
      # open writer for reading to collect metadata
      dfr = DataFileReader(writer, avro_io.DatumReader())

      # TODO: collect arbitrary metadata
      # collect metadata
      self._sync_marker = dfr.sync_marker
      self.SetMeta('avro.codec', dfr.GetMeta('avro.codec'))

      # get schema used to write existing file
      schema_from_file = dfr.GetMeta('avro.schema').decode('utf-8')
      self.SetMeta('avro.schema', schema_from_file)
      self.datum_writer.writer_schema = schema.Parse(schema_from_file)

      # seek to the end of the file and prepare for writing
      writer.seek(0, 2)
      self._header_written = True

  # read-only properties

  @property
  def writer(self):
    return self._writer

  @property
  def encoder(self):
    return self._encoder

  @property
  def datum_writer(self):
    return self._datum_writer

  @property
  def buffer_encoder(self):
    return self._buffer_encoder

  @property
  def sync_marker(self):
    return self._sync_marker

  @property
  def meta(self):
    return self._meta

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  @property
  def block_count(self):
    return self._block_count

  def GetMeta(self, key):
    """Reports the metadata associated to the given key.

    Args:
      key: Key of the metadata to report the value of.
    Returns:
      The metadata value, as bytes, or None if the key does not exist.
    """
    return self._meta.get(key)

  def SetMeta(self, key, value):
    """Sets the metadata value for the given key.

    Note: metadata is persisted and retrieved as bytes.

    Args:
      key: Key of the metadata to set.
      value: Value of the metadata, as bytes or str.
          Strings are automatically converted to bytes.
    """
    if isinstance(value, str):
      value = value.encode('utf-8')
    assert isinstance(value, bytes), (
        'Invalid metadata value for key %r: %r' % (key, value))
    self._meta[key] = value

  def _WriteHeader(self):
    header = {
        'magic': MAGIC,
        'meta': self.meta,
        'sync': self.sync_marker,
    }
    logging.debug(
        'Writing Avro data file header:\n%s\nAvro header schema:\n%s',
        header, META_SCHEMA)
    self.datum_writer.write_data(META_SCHEMA, header, self.encoder)
    self._header_written = True

  # TODO: make a schema for blocks and use datum_writer
  def _WriteBlock(self):
    if not self._header_written:
      self._WriteHeader()

    if self.block_count <= 0:
      logging.info('Current block is empty, nothing to write.')
      return

    # write number of items in block
    self.encoder.write_long(self.block_count)

    # write block contents
    uncompressed_data = self._buffer_writer.getvalue()
    codec = self.GetMeta(CODEC_KEY).decode('utf-8')
    if codec == 'null':
      compressed_data = uncompressed_data
      compressed_data_length = len(compressed_data)
    elif codec == 'deflate':
      # The first two characters and last character are zlib
      # wrappers around deflate data.
      compressed_data = zlib.compress(uncompressed_data)[2:-1]
      compressed_data_length = len(compressed_data)
    elif codec == 'snappy':
      compressed_data = snappy.compress(uncompressed_data)
      compressed_data_length = len(compressed_data) + 4 # crc32
    else:
      fail_msg = '"%s" codec is not supported.' % codec
      raise DataFileException(fail_msg)

    # Write length of block
    self.encoder.write_long(compressed_data_length)

    # Write block
    self.writer.write(compressed_data)

    # Write CRC32 checksum for Snappy
    if self.GetMeta(CODEC_KEY) == 'snappy':
      self.encoder.write_crc32(uncompressed_data)

    # write sync marker
    self.writer.write(self.sync_marker)

    logging.debug(
        'Writing block with count=%d nbytes=%d sync=%r',
        self.block_count, compressed_data_length, self.sync_marker)

    # reset buffer
    self._buffer_writer.seek(0)
    self._buffer_writer.truncate()
    self._block_count = 0

  def append(self, datum):
    """Append a datum to the file."""
    self.datum_writer.write(datum, self.buffer_encoder)
    self._block_count += 1

    # if the data to write is larger than the sync interval, write the block
    if self._buffer_writer.tell() >= SYNC_INTERVAL:
      self._WriteBlock()

  def sync(self):
    """
    Return the current position as a value that may be passed to
    DataFileReader.seek(long). Forces the end of the current block,
    emitting a synchronization marker.
    """
    self._WriteBlock()
    return self.writer.tell()

  def flush(self):
    """Flush the current state of the file, including metadata."""
    self._WriteBlock()
    self.writer.flush()

  def close(self):
    """Close the file."""
    self.flush()
    self.writer.close()


# ------------------------------------------------------------------------------


class DataFileReader(object):
  """Read files written by DataFileWriter."""

  # TODO: allow user to specify expected schema?
  # TODO: allow user to specify the encoder
  def __init__(self, reader, datum_reader):
    """Initializes a new data file reader.

    Args:
      reader: Open file to read from.
      datum_reader: Avro datum reader.
    """
    self._reader = reader
    self._raw_decoder = avro_io.BinaryDecoder(reader)
    self._datum_decoder = None # Maybe reset at every block.
    self._datum_reader = datum_reader

    # read the header: magic, meta, sync
    self._read_header()

    # ensure codec is valid
    self.codec = self.GetMeta('avro.codec').decode('utf-8')
    if self.codec is None:
      self.codec = "null"
    if self.codec not in VALID_CODECS:
      raise DataFileException('Unknown codec: %s.' % self.codec)

    self._file_length = self._GetInputFileLength()

    # get ready to read
    self._block_count = 0
    self.datum_reader.writer_schema = (
        schema.Parse(self.GetMeta(SCHEMA_KEY).decode('utf-8')))

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  def __iter__(self):
    return self

  def __next__(self):
    """Implements the iterator interface."""
    return next(self)

  # read-only properties
  @property
  def reader(self):
    return self._reader

  @property
  def raw_decoder(self):
    return self._raw_decoder

  @property
  def datum_decoder(self):
    return self._datum_decoder

  @property
  def datum_reader(self):
    return self._datum_reader

  @property
  def sync_marker(self):
    return self._sync_marker

  @property
  def meta(self):
    return self._meta

  @property
  def file_length(self):
    """Length of the input file, in bytes."""
    return self._file_length

  # read/write properties
  @property
  def block_count(self):
    return self._block_count

  def GetMeta(self, key):
    """Reports the value of a given metadata key.

    Args:
      key: Metadata key (string) to report the value of.
    Returns:
      Value associated to the metadata key, as bytes.
    """
    return self._meta.get(key)

  def SetMeta(self, key, value):
    """Sets a metadata.

    Args:
      key: Metadata key (string) to set.
      value: Metadata value to set, as bytes.
    """
    if isinstance(value, str):
      value = value.encode('utf-8')
    self._meta[key] = value

  def _GetInputFileLength(self):
    """Reports the length of the input file, in bytes.

    Leaves the current position unmodified.

    Returns:
      The length of the input file, in bytes.
    """
    current_pos = self.reader.tell()
    self.reader.seek(0, 2)
    file_length = self.reader.tell()
    self.reader.seek(current_pos)
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
      raise schema.AvroException(fail_msg)

    # set metadata
    self._meta = header['meta']

    # set sync marker
    self._sync_marker = header['sync']

  def _read_block_header(self):
    self._block_count = self.raw_decoder.read_long()
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
      self._datum_decoder = avro_io.BinaryDecoder(io.BytesIO(uncompressed))
    elif self.codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = self.raw_decoder.read_long()
      data = self.raw_decoder.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._datum_decoder = avro_io.BinaryDecoder(io.BytesIO(uncompressed))
      self.raw_decoder.check_crc32(uncompressed);
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
    else:
      return True

  # TODO: handle block of length zero
  # TODO: clean this up with recursion
  def __next__(self):
    """Return the next datum in the file."""
    if self.block_count == 0:
      if self.is_EOF():
        raise StopIteration
      elif self._skip_sync():
        if self.is_EOF(): raise StopIteration
        self._read_block_header()
      else:
        self._read_block_header()

    datum = self.datum_reader.read(self.datum_decoder)
    self._block_count -= 1
    return datum

  def close(self):
    """Close this reader."""
    self.reader.close()


if __name__ == '__main__':
  raise Exception('Not a standalone module')
