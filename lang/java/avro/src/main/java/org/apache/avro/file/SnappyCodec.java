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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import org.xerial.snappy.Snappy;

/** * Implements Snappy compression and decompression. */
class SnappyCodec extends Codec {
  private final CRC32 crc32 = new CRC32();

  static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return new SnappyCodec();
    }
  }

  SnappyCodec() {}

  @Override public String getName() { return DataFileConstants.SNAPPY_CODEC; }

  @Override
  public ByteBuffer compress(ByteBuffer in) throws IOException {
    int maxSize = Snappy.maxCompressedLength(in.remaining()) + 4;
    byte[] outbytes = new byte[maxSize];
    int size = Snappy.compress(
        in.array(),
        in.arrayOffset() + in.position(),
        in.remaining(),
        outbytes,
        0);

    crc32.reset();
    crc32.update(
        in.array(),
        in.arrayOffset() + in.position(),
        in.remaining());

    ByteBuffer out = ByteBuffer.wrap(outbytes);
    out.order(ByteOrder.LITTLE_ENDIAN);
    out.putInt(size, (int)crc32.getValue());
    out.limit(size+4);

    return out;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer in) throws IOException {
    byte[] compressed = in.array();
    int offset = in.arrayOffset() + in.position();
    int length = in.remaining() - 4;
    in.order(ByteOrder.LITTLE_ENDIAN);
    int checksum = in.getInt(in.limit() - 4);

    int expectedSize = Snappy.uncompressedLength(compressed, offset, length);

    byte[] outbytes = new byte[expectedSize];

    int size = Snappy.uncompress(compressed, offset, length, outbytes, 0);

    crc32.reset();
    crc32.update(outbytes, 0, size);
    if (checksum != (int)crc32.getValue()) {
      throw new IOException("Checksum failure");
    }

    return ByteBuffer.wrap(outbytes, 0, size);
  }

  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object other) {
    return (this == other) || (this.getClass() == other.getClass());
  }

}
