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
"""
Read/Write Avro File Object Containers.
"""
import zlib
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
from avro import schema
from avro import io
try:
  import snappy
except:
  pass # fail later if snappy is used
#
# Constants
#

VERSION = 1
MAGIC = 'Obj' + chr(VERSION)
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
SYNC_INTERVAL = 1000 * SYNC_SIZE # TODO(hammer): make configurable
META_SCHEMA = schema.parse("""\
{"type": "record", "name": "org.apache.avro.file.Header",
 "fields" : [
   {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": %d}},
   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
   {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": %d}}]}
""" % (MAGIC_SIZE, SYNC_SIZE))
VALID_CODECS = ['null', 'deflate', 'snappy']
VALID_ENCODINGS = ['binary'] # not used yet

CODEC_KEY = "avro.codec"
SCHEMA_KEY = "avro.schema"

#
# Exceptions
#

class DataFileException(schema.AvroException):
  """
  Raised when there's a problem reading or writing file object containers.
  """
  def __init__(self, fail_msg):
    schema.AvroException.__init__(self, fail_msg)

#
# Write Path
#

class DataFileWriter(object):
  @staticmethod
  def generate_sync_marker():
    return generate_sixteen_random_bytes()

  # TODO(hammer): make 'encoder' a metadata property
  def __init__(self, writer, datum_writer, writers_schema=None, codec='null'):
    """
    If the schema is not present, presume we're appending.

    @param writer: File-like object to write into.
    """
    self._writer = writer
    self._encoder = io.BinaryEncoder(writer)
    self._datum_writer = datum_writer
    self._buffer_writer = StringIO()
    self._buffer_encoder = io.BinaryEncoder(self._buffer_writer)
    self._block_count = 0
    self._meta = {}

    if writers_schema is not None:
      if codec not in VALID_CODECS:
        raise DataFileException("Unknown codec: %r" % codec)
      self._sync_marker = DataFileWriter.generate_sync_marker()
      self.set_meta('avro.codec', codec)
      self.set_meta('avro.schema', str(writers_schema))
      self.datum_writer.writers_schema = writers_schema
      self._write_header()
    else:
      # open writer for reading to collect metadata
      dfr = DataFileReader(writer, io.DatumReader())
      
      # TODO(hammer): collect arbitrary metadata
      # collect metadata
      self._sync_marker = dfr.sync_marker
      self.set_meta('avro.codec', dfr.get_meta('avro.codec'))

      # get schema used to write existing file
      schema_from_file = dfr.get_meta('avro.schema')
      self.set_meta('avro.schema', schema_from_file)
      self.datum_writer.writers_schema = schema.parse(schema_from_file)

      # seek to the end of the file and prepare for writing
      writer.seek(0, 2)

  # read-only properties
  writer = property(lambda self: self._writer)
  encoder = property(lambda self: self._encoder)
  datum_writer = property(lambda self: self._datum_writer)
  buffer_writer = property(lambda self: self._buffer_writer)
  buffer_encoder = property(lambda self: self._buffer_encoder)
  sync_marker = property(lambda self: self._sync_marker)
  meta = property(lambda self: self._meta)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  # read/write properties
  def set_block_count(self, new_val):
    self._block_count = new_val
  block_count = property(lambda self: self._block_count, set_block_count)

  # utility functions to read/write metadata entries
  def get_meta(self, key):
    return self._meta.get(key)
  def set_meta(self, key, val):
    self._meta[key] = val

  def _write_header(self):
    header = {'magic': MAGIC,
              'meta': self.meta,
              'sync': self.sync_marker}
    self.datum_writer.write_data(META_SCHEMA, header, self.encoder)

  # TODO(hammer): make a schema for blocks and use datum_writer
  def _write_block(self):
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
      else:
        fail_msg = '"%s" codec is not supported.' % self.get_meta(CODEC_KEY)
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

class DataFileReader(object):
  """Read files written by DataFileWriter."""
  # TODO(hammer): allow user to specify expected schema?
  # TODO(hammer): allow user to specify the encoder
  def __init__(self, reader, datum_reader):
    self._reader = reader
    self._raw_decoder = io.BinaryDecoder(reader)
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
    self._block_count = 0
    self.datum_reader.writers_schema = schema.parse(self.get_meta(SCHEMA_KEY))

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  def __iter__(self):
    return self

  # read-only properties
  reader = property(lambda self: self._reader)
  raw_decoder = property(lambda self: self._raw_decoder)
  datum_decoder = property(lambda self: self._datum_decoder)
  datum_reader = property(lambda self: self._datum_reader)
  sync_marker = property(lambda self: self._sync_marker)
  meta = property(lambda self: self._meta)
  file_length = property(lambda self: self._file_length)

  # read/write properties
  def set_block_count(self, new_val):
    self._block_count = new_val
  block_count = property(lambda self: self._block_count, set_block_count)

  # utility functions to read/write metadata entries
  def get_meta(self, key):
    return self._meta.get(key)
  def set_meta(self, key, val):
    self._meta[key] = val

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
      raise schema.AvroException(fail_msg)

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
      self._datum_decoder = io.BinaryDecoder(StringIO(uncompressed))
    elif self.codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = self.raw_decoder.read_long()
      data = self.raw_decoder.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._datum_decoder = io.BinaryDecoder(StringIO(uncompressed))
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

  # TODO(hammer): handle block of length zero
  # TODO(hammer): clean this up with recursion
  def next(self):
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
    self.block_count -= 1
    return datum

  def close(self):
    """Close this reader."""
    self.reader.close()

def generate_sixteen_random_bytes():
  try:
    import os
    return os.urandom(16)
  except:
    import random
    return [ chr(random.randrange(256)) for i in range(16) ]
