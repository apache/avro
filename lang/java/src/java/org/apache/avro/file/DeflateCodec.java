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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;

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

  ByteArrayOutputStream compressionBuffer;
  private Deflater deflater;
  private int compressionLevel;
  private Inflater inflater;

  public DeflateCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  String getName() {
    return "deflate";
  }

  @Override
  void compress(ByteArrayOutputStream buffer, OutputStream out) throws IOException {
    if (compressionBuffer == null) {
      compressionBuffer = new ByteArrayOutputStream(buffer.size());
    }
    if (deflater == null) {
      deflater = new Deflater(compressionLevel, false);
    }
    // Pass output through deflate, and prepend with length of compressed output.
    DeflaterOutputStream deflaterStream = 
      new DeflaterOutputStream(compressionBuffer, deflater);
    buffer.writeTo(deflaterStream);
    deflaterStream.finish();
    new BinaryEncoder(out).writeLong(compressionBuffer.size());
    compressionBuffer.writeTo(out);
    compressionBuffer.reset();
    deflater.reset();
  }

  @Override
  Decoder decompress(InputStream in, Decoder vin) throws IOException {
    if (inflater == null) {
      inflater = new Inflater(false);
    }
    long compressedLength = vin.readLong();
    InputStream uncompressed = new InflaterInputStream(
        new LengthLimitedInputStream(in, compressedLength),
        inflater);
    inflater.reset();
    return new BinaryDecoder(uncompressed);
  }

}
