#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

"""Input/Output utilities."""

import struct, uuid, cStringIO
import avro.schema as schema

#Constants
_INT_MIN_VALUE = -(1 << 31)
_INT_MAX_VALUE = (1 << 31) - 1
_LONG_MIN_VALUE = -(1 << 63)
_LONG_MAX_VALUE = (1 << 63) - 1
_STRUCT_INT = struct.Struct('!I')
_STRUCT_LONG = struct.Struct('!Q')
_STRUCT_FLOAT = struct.Struct('!f')
_STRUCT_DOUBLE = struct.Struct('!d')

class AvroTypeException(schema.AvroException):
  """Raised when illegal type is used."""
  def __init__(self, schm=None, datum=None, msg=None):
    if msg is None:
      msg = "Not a "+schema.stringval(schm)+": "+datum.__str__()
    schema.AvroException.__init__(self, msg)

class DatumReaderBase(object):
  """Base class for reading data of a schema."""

  def setschema(self, schema):
    pass

  def read(self, decoder):
    """Read a datum. Traverse the schema, depth-first, reading all leaf values
    in the schema into a datum that is returned"""
    pass

class DatumWriterBase(object):
  """Base class for writing data of a schema."""
  def setschema(self, schema):
    pass

  def write(self, data, encoder):
    """Write a datum. Traverse the schema, depth first, writing each leaf value
    in the schema from the datum to the output."""
    pass


class Decoder(object):
  """Read leaf values."""

  def __init__(self, reader):
    self.__reader = reader

  def readboolean(self):
    return ord(self.__reader.read(1)) == 1

  def readint(self):
    return self.readlong()

  def readlong(self):
    b = ord(self.__reader.read(1))
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
      b = ord(self.__reader.read(1))
      n |= (b & 0x7F) << shift
      shift += 7
    datum = (n >> 1) ^ -(n & 1)
    return datum

  def readfloat(self):
    bits = (((ord(self.__reader.read(1)) & 0xffL)    ) |
        ((ord(self.__reader.read(1)) & 0xffL) <<  8) |
        ((ord(self.__reader.read(1)) & 0xffL) << 16) |
        ((ord(self.__reader.read(1)) & 0xffL) << 24))
    return _STRUCT_FLOAT.unpack(_STRUCT_INT.pack(bits))[0]

  def readdouble(self):
    bits = (((ord(self.__reader.read(1)) & 0xffL)    ) |
        ((ord(self.__reader.read(1)) & 0xffL) <<  8) |
        ((ord(self.__reader.read(1)) & 0xffL) << 16) |
        ((ord(self.__reader.read(1)) & 0xffL) << 24) |
        ((ord(self.__reader.read(1)) & 0xffL) << 32) |
        ((ord(self.__reader.read(1)) & 0xffL) << 40) |
        ((ord(self.__reader.read(1)) & 0xffL) << 48) |
        ((ord(self.__reader.read(1)) & 0xffL) << 56))
    return _STRUCT_DOUBLE.unpack(_STRUCT_LONG.pack(bits))[0]

  def readbytes(self):
    return self.read(self.readlong())

  def readutf8(self):
    return unicode(self.readbytes(), "utf-8")

  def read(self, len):
    return struct.unpack(len.__str__()+'s', self.__reader.read(len))[0]

  def skipboolean(self):
    self.skip(1)

  def skipint(self):
    self.skip(4)

  def skiplong(self):
    self.skip(8)

  def skipfloat(self):
    self.skip(4)

  def skipdouble(self):
    self.skip(8)

  def skipbytes(self):
    self.skip(self.readlong())

  def skiputf8(self):
    self.skipbytes()

  def skip(self, len):
    self.__reader.seek(self.__reader.tell()+len)

class Encoder(object):
  """Write leaf values."""

  def __init__(self, writer):
    self.__writer = writer
  
  def writeboolean(self, datum):
    if not isinstance(datum, bool):
      raise AvroTypeException(schema.BOOLEAN, datum)
    if datum:
      self.__writer.write(chr(1))
    else:
      self.__writer.write(chr(0))

  def writeint(self, n):
    if n < _INT_MIN_VALUE or n > _INT_MAX_VALUE:
      raise AvroTypeException(schema.INT, n, 
                              "datam too big to fit into avro int")
    self.writelong(n);

  def writelong(self, n):
    if n < _LONG_MIN_VALUE or n > _LONG_MAX_VALUE:
      raise AvroTypeException(schema.LONG, n, 
                              "datam too big to fit into avro long")
    n = (n << 1) ^ (n >> 63)
    while (n & ~0x7F) != 0:
      self.__writer.write(chr((n & 0x7f) | 0x80))
      n >>=7
    self.__writer.write(chr(n))

  def writefloat(self, datum):
    bits = _STRUCT_INT.unpack(_STRUCT_FLOAT.pack(datum))[0]
    self.__writer.write(chr((bits    ) & 0xFF))
    self.__writer.write(chr((bits >>  8) & 0xFF))
    self.__writer.write(chr((bits >>  16) & 0xFF))
    self.__writer.write(chr((bits >>  24) & 0xFF))

  def writedouble(self, datum):
    bits = _STRUCT_LONG.unpack(_STRUCT_DOUBLE.pack(datum))[0]
    self.__writer.write(chr((bits    ) & 0xFF))
    self.__writer.write(chr((bits >>  8) & 0xFF))
    self.__writer.write(chr((bits >>  16) & 0xFF))
    self.__writer.write(chr((bits >>  24) & 0xFF))
    self.__writer.write(chr((bits >>  32) & 0xFF))
    self.__writer.write(chr((bits >>  40) & 0xFF))
    self.__writer.write(chr((bits >>  48) & 0xFF))
    self.__writer.write(chr((bits >>  56) & 0xFF))

  def writebytes(self, datum):
    if not isinstance(datum, str):
      raise AvroTypeException(schema.BYTES, datum, 
                              "avro BYTES should be python str")
    self.writelong(len(datum))
    self.__writer.write(struct.pack(len(datum).__str__()+'s',datum))

  def writeutf8(self, datum):
    if not isinstance(datum, unicode):
      raise AvroTypeException(schema.STRING, datum, 
                              "avro STRING should be python unicode")
    datum = datum.encode("utf-8")
    self.writebytes(datum)

  def write(self, datum):
    self.__writer.write(datum)

#Data file constants.
_VERSION = 0
_MAGIC = "Obj"+chr(_VERSION)
_SYNC_SIZE = 16
_SYNC_INTERVAL = 1000*_SYNC_SIZE
_FOOTER_BLOCK = -1
class DataFileWriter(object):
  """Stores in a file a sequence of data conforming to a schema. The schema is 
  stored in the file with the data. Each datum in a file is of the same
  schema. Data is grouped into blocks.
  A synchronization marker is written between blocks, so that
  files may be split. Blocks may be compressed. Extensible metadata is
  stored at the end of the file. Files may be appended to."""

  def __init__(self, schm, writer, dwriter):
    self.__writer = writer
    self.__encoder = Encoder(writer)
    self.__dwriter = dwriter
    self.__dwriter.setschema(schm)
    self.__count = 0  #entries in file
    self.__blockcount = 0  #entries in current block
    self.__buffer = cStringIO.StringIO()
    self.__bufwriter = Encoder(self.__buffer)
    self.__meta = dict()
    self.__sync = uuid.uuid4().bytes
    self.__meta["sync"] = self.__sync
    self.__meta["schema"] = schema.stringval(schm)
    self.__writer.write(struct.pack(len(_MAGIC).__str__()+'s',
                                    _MAGIC))

  def setmeta(self, key, val):
    """Set a meta data property."""
    self.__meta[key] = val

  def append(self, datum):
    """Append a datum to the file."""
    self.__dwriter.write(datum, self.__bufwriter)
    self.__count+=1
    self.__blockcount+=1
    if self.__buffer.tell() >= _SYNC_INTERVAL:
      self.__writeblock()

  def __writeblock(self):
    if self.__blockcount > 0:
      self.__writer.write(self.__sync)
      self.__encoder.writelong(self.__blockcount)
      self.__writer.write(self.__buffer.getvalue())
      self.__buffer.truncate(0) #reset
      self.__blockcount = 0

  def sync(self):
    """Return the current position as a value that may be passed to
    DataFileReader.seek(long). Forces the end of the current block,
    emitting a synchronization marker."""
    self.__writeblock()
    return self.__writer.tell()

  def flush(self):
    """Flush the current state of the file, including metadata."""
    self.__writefooter()
    self.__writer.flush()

  def close(self):
    """Close the file."""
    self.flush()
    self.__writer.close()

  def __writefooter(self):
    self.__writeblock()
    self.__meta["count"] = self.__count.__str__()
    
    self.__bufwriter.writelong(len(self.__meta))
    for k,v in self.__meta.items():
      self.__bufwriter.writeutf8(unicode(k,'utf-8'))
      self.__bufwriter.writebytes(str(v))
    size = self.__buffer.tell() + 4
    self.__writer.write(self.__sync)
    self.__encoder.writelong(_FOOTER_BLOCK)
    self.__encoder.writelong(size)
    self.__buffer.flush()
    self.__writer.write(self.__buffer.getvalue())
    self.__buffer.truncate(0) #reset
    self.__writer.write(chr((size >> 24) & 0xFF))
    self.__writer.write(chr((size >> 16) & 0xFF))
    self.__writer.write(chr((size >> 8) & 0xFF))
    self.__writer.write(chr((size >> 0) & 0xFF))

class DataFileReader(object):
  """Read files written by DataFileWriter."""

  def __init__(self, reader, dreader):
    self.__reader = reader
    self.__decoder = Decoder(reader)
    mag = struct.unpack(len(_MAGIC).__str__()+'s', 
                 self.__reader.read(len(_MAGIC)))[0]
    if mag != _MAGIC:
      raise schema.AvroException("Not an avro data file")
    #find the length
    self.__reader.seek(0,2)
    self.__length = self.__reader.tell()
    self.__reader.seek(-4, 2)
    footersize = (int(ord(self.__reader.read(1)) << 24) +
            int(ord(self.__reader.read(1)) << 16) +
            int(ord(self.__reader.read(1)) << 8) +
            int(ord(self.__reader.read(1))))
    seekpos = self.__reader.seek(self.__length-footersize)
    metalength = self.__decoder.readlong()
    self.__meta = dict()
    for i in range(0, metalength):
      key = self.__decoder.readutf8()
      self.__meta[key] = self.__decoder.readbytes()
    self.__sync = self.__meta.get("sync")
    self.__count = int(self.__meta.get("count"))
    self.__schema = schema.parse(self.__meta.get("schema").encode("utf-8"))
    self.__blockcount = 0
    self.__dreader = dreader
    self.__dreader.setschema(self.__schema)
    self.__reader.seek(len(_MAGIC))

  def getmeta(self, key):
    """Return the value of a metadata property."""
    return self.__meta.get(key)

  def next(self):
    """Return the next datum in the file."""
    while self.__blockcount == 0:
      if self.__reader.tell() == self.__length:
        return None
      self.__skipsync()
      self.__blockcount = self.__decoder.readlong()
      if self.__blockcount == _FOOTER_BLOCK:
        self.__reader.seek(self.__decoder.readlong()+self.__reader.tell())
        self.__blockcount = 0
    self.__blockcount-=1
    datum = self.__dreader.read(self.__decoder)
    return datum

  def __skipsync(self):
    if self.__reader.read(_SYNC_SIZE)!=self.__sync:
      raise schema.AvroException("Invalid sync!")

  def seek(self, pos):
    """Move to the specified synchronization point, as returned by 
    DataFileWriter.sync()."""
    self.__reader.seek(pos)
    self.__blockcount = 0

  def sync(self, position):
    """Move to the next synchronization point after a position."""
    if self.__reader.tell()+_SYNC_SIZE >= self.__length:
      self.__reader.seek(self.__length)
      return
    self.__reader.seek(position)
    self.__reader.read(_SYNC_SIZE)

  def close(self):
    """Close this reader."""
    self.__reader.close()
