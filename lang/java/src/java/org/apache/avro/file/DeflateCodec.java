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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;

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

  public DeflateCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  String getName() {
    return DataFileConstants.DEFLATE_CODEC;
  }

  @Override
  ByteArrayOutputStream compress(ByteArrayOutputStream buffer) 
    throws IOException {
    if (compressionBuffer == null) {
      compressionBuffer = new ByteArrayOutputStream(buffer.size());
    } else {
      compressionBuffer.reset();
    }
    if (deflater == null) {
      deflater = new Deflater(compressionLevel, true);
    }
    // Pass output through deflate
    DeflaterOutputStream deflaterStream = 
      new DeflaterOutputStream(compressionBuffer, deflater);
    buffer.writeTo(deflaterStream);
    deflaterStream.finish();
    deflater.reset();
    return compressionBuffer;
  }

  @Override
  BinaryDecoder decompress(byte[] data, int offset, int length)
      throws IOException {
    Inflater inflater = new Inflater(true);
    InputStream uncompressed = new InflaterInputStream(
        new ByteArrayInputStream(data, offset, length), inflater);
    return DecoderFactory.defaultFactory().createBinaryDecoder(uncompressed, null);
  }

}
