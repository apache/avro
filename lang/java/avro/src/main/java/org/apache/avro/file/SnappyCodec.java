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

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyException;

/** * Implements Snappy compression and decompression. */
class SnappyCodec extends Codec {

  private static final SnappyCodec INSTANCE = new SnappyCodec();

  static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return INSTANCE;
    }
  }

  private SnappyCodec() {}

  @Override String getName() { return DataFileConstants.SNAPPY_CODEC; }

  @Override
  ByteBuffer compress(ByteBuffer in) throws IOException {
    try { 
      ByteBuffer out =
        ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()));
      int size = Snappy.compress(in.array(), in.position(), in.remaining(),
                                 out.array(), 0);
      out.limit(size);
      return out;
    } catch (SnappyException e) {
      throw new IOException(e);
    }
  }

  @Override
  ByteBuffer decompress(ByteBuffer in) throws IOException {
    try { 
      ByteBuffer out = ByteBuffer.allocate
        (Snappy.uncompressedLength(in.array(), in.position(), in.remaining()));
      int size = Snappy.uncompress(in.array(), in.position(), in.remaining(),
                                   out.array(), 0);
      out.limit(size);
      return out;
    } catch (SnappyException e) {
      throw new IOException(e);
    }
  }
  
  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    return true;
  }

}
