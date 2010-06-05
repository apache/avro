/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.avro.file.DataFileWriter.NonCopyingByteArrayOutputStream;

/** 
 * Implements DEFLATE (RFC1951) compression and decompression. 
 *
 * Note that there is a distinction between RFC1951 (deflate)
 * and RFC1950 (zlib).  zlib adds an extra 2-byte header
 * at the front, and a 4-byte checksum at the end.  The
 * code here, by passing "true" as the "nowrap" option to
 * {@link Inflater} and {@link Deflater}, is using
 * RFC1951.
 */
class DeflateCodec extends Codec {
  
  static class Option extends CodecFactory {
    private int compressionLevel;

    public Option(int compressionLevel) {
      this.compressionLevel = compressionLevel;
    }

    @Override
    protected Codec createInstance() {
      return new DeflateCodec(compressionLevel);
    }
  }

  NonCopyingByteArrayOutputStream compressionBuffer;
  private Deflater deflater;
  private Inflater inflater;
  //currently only do 'nowrap' -- RFC 1951, not zlib
  private boolean nowrap = true; 
  private int compressionLevel;

  public DeflateCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  String getName() {
    return DataFileConstants.DEFLATE_CODEC;
  }

  @Override
  ByteBuffer compress(ByteBuffer data) throws IOException {
    if (compressionBuffer == null) {
      compressionBuffer = new NonCopyingByteArrayOutputStream(
          data.remaining());
    }
    if (deflater == null) {
      deflater = new Deflater(compressionLevel, nowrap);
    }
    // Pass output through deflate
    DeflaterOutputStream deflaterStream = 
      new DeflaterOutputStream(compressionBuffer, deflater);
    deflaterStream.write(data.array(),
        data.position() + data.arrayOffset(),
        data.limit() + data.arrayOffset());
    deflaterStream.finish();
    ByteBuffer result = compressionBuffer.getByteArrayAsByteBuffer();
    deflater.reset();
    compressionBuffer.reset();
    return result;
  }

  @Override
  ByteBuffer decompress(ByteBuffer data) throws IOException {
    if (compressionBuffer == null) {
      compressionBuffer = new NonCopyingByteArrayOutputStream(
          data.remaining());
    }
    if (inflater == null) {
      inflater = new Inflater(nowrap);
    }
    InputStream uncompressed = new InflaterInputStream(
        new ByteArrayInputStream(data.array(),
            data.position() + data.arrayOffset(),
            data.remaining()), inflater);
    int read;
    byte[] buff = new byte[2048];
    try {
      while (true) {
        read = uncompressed.read(buff);
        if (read < 0) break;
        compressionBuffer.write(buff, 0, read);
      } 
    } catch (EOFException e) {
      // sometimes InflaterInputStream.read
      // throws this instead of returning -1
    }
    ByteBuffer result = compressionBuffer.getByteArrayAsByteBuffer();
    inflater.reset();
    compressionBuffer.reset();
    return result;
  }

  @Override
  public int hashCode() {
    return nowrap ? 0 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    DeflateCodec other = (DeflateCodec)obj;
    return (this.nowrap == other.nowrap);
  }

  @Override
  public String toString() {
    return getName() + "-" + compressionLevel;
  }
}
