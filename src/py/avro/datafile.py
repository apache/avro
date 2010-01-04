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
import uuid
import cStringIO
from avro import schema
from avro import io

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
VALID_CODECS = ['null']
VALID_ENCODINGS = ['binary'] # not used yet

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
    return uuid.uuid4().bytes

  # TODO(hammer): make 'encoder' a metadata property
  def __init__(self, writer, datum_writer, writers_schema=None):
    """
    If the schema is not present, presume we're appending.
    """
    self._writer = writer
    self._encoder = io.BinaryEncoder(writer)
    self._datum_writer = datum_writer
    self._buffer_writer = cStringIO.StringIO()
    self._buffer_encoder = io.BinaryEncoder(self._buffer_writer)
    self._block_count = 0
    self._meta = {}

    if writers_schema is not None:
      self._sync_marker = DataFileWriter.generate_sync_marker()
      self.set_meta('codec', 'null')
      self.set_meta('schema', str(writers_schema))
      self.datum_writer.writers_schema = writers_schema
      self._write_header()
    else:
      # open writer for reading to collect metadata
      dfr = DataFileReader(writer, io.DatumReader())
      
      # TODO(hammer): collect arbitrary metadata
      # collect metadata
      self._sync_marker = dfr.sync_marker
      self.set_meta('codec', dfr.get_meta('codec'))

      # get schema used to write existing file
      schema_from_file = dfr.get_meta('schema')
      self.set_meta('schema', schema_from_file)
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
  # TODO(hammer): use codec when writing the block contents
  def _write_block(self):
    if self.block_count > 0:
      # write number of items in block
      self.encoder.write_long(self.block_count)

      # write block contents
      if self.get_meta('codec') == 'null':
        self.writer.write(self.buffer_writer.getvalue())
      else:
        fail_msg = '"%s" codec is not supported.' % self.get_meta('codec')
        raise DataFileException(fail_msg)

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
    self._decoder = io.BinaryDecoder(reader)
    self._datum_reader = datum_reader
    
    # read the header: magic, meta, sync
    self._read_header()

    # ensure codec is valid
    codec_from_file = self.get_meta('codec')
    if codec_from_file is not None and codec_from_file not in VALID_CODECS:
      raise DataFileException('Unknown codec: %s.' % codec_from_file)

    # get file length
    self._file_length = self.determine_file_length()

    # get ready to read
    self._block_count = 0
    self.datum_reader.writers_schema = schema.parse(self.get_meta('schema'))
  
  def __iter__(self):
    return self

  # read-only properties
  reader = property(lambda self: self._reader)
  decoder = property(lambda self: self._decoder)
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
    header = self.datum_reader.read_data(META_SCHEMA, META_SCHEMA, self.decoder)

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
    self.block_count = self.decoder.read_long()

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

    datum = self.datum_reader.read(self.decoder) 
    self.block_count -= 1
    return datum

  def close(self):
    """Close this reader."""
    self.reader.close()
